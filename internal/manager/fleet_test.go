package manager

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zlovtnik/ssl-proxy/services/dq/internal/config"
	"github.com/zlovtnik/ssl-proxy/services/dq/internal/control"
	workerhost "github.com/zlovtnik/ssl-proxy/services/dq/internal/worker"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/rpc"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// authoritativeFixture is a TEST ONLY ledger exercising the manager's control
// boundary. Production durability and fencing are separately tested in Octopus.
type authoritativeFixture struct {
	mu           sync.Mutex
	manifest     contract.Manifest
	runs         map[string]contract.Run
	workers      map[string]contract.Heartbeat
	accepted     map[string]bool
	received     map[string]bool
	reservations int
	loseAccepted bool
}

func fixtureLedger(t *testing.T) *authoritativeFixture {
	m := fixtureManifest(t)
	params, _ := CanonicalJSON([]byte(`{"authorized_scope":"floor-a","count":2}`))
	s := &authoritativeFixture{manifest: m, runs: map[string]contract.Run{}, workers: map[string]contract.Heartbeat{}, accepted: map[string]bool{}, received: map[string]bool{}}
	for _, id := range []string{"run-1", "run-2"} {
		s.runs[id] = contract.Run{InstallationID: m.InstallationID, Namespace: m.Namespace, RunID: id, ProcessorID: m.ProcessorID, ProcessorVersion: m.Version, ImplementationDigest: m.ImplementationDigest, Operation: "calculate", Parameters: params, ParametersSHA256: contract.SHA256(params), PolicyVersion: m.PolicyVersion, InputRefs: []contract.ArtifactRef{}, OutputRefs: []contract.ArtifactRef{}, SourceRevisionDigest: strings.Repeat("b", 64), Status: "queued", DeadlineAt: time.Now().Add(time.Minute)}
	}
	return s
}
func jsonCopy(src, dst any) error {
	b, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}
func (s *authoritativeFixture) Do(_ context.Context, method, path string, _ url.Values, body, out any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if method == http.MethodPost && strings.HasSuffix(path, "/heartbeat") {
		h := body.(contract.Heartbeat)
		s.workers[h.WorkerInstanceID] = h
		return nil
	}
	if method == http.MethodGet && path == "/dispatches" {
		items := []contract.Dispatch{}
		ids := []string{}
		for id := range s.runs {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			r := s.runs[id]
			if !s.received[id] || r.CurrentAttempt != nil && s.accepted[r.CurrentAttempt.AttemptID] {
				continue
			}
			items = append(items, contract.Dispatch{Run: r, Manifest: s.manifest, Attempt: r.CurrentAttempt})
		}
		return jsonCopy(contract.Dispatches{Items: items}, out)
	}
	if method == http.MethodPost && strings.HasSuffix(path, "/attempts/reserve") {
		id := strings.Split(path, "/")[2]
		r := s.runs[id]
		if r.CurrentAttempt != nil {
			return jsonCopy(contract.Reservation{Run: r, Attempt: *r.CurrentAttempt, Manifest: s.manifest, WorkerAddress: s.workers[r.CurrentAttempt.WorkerInstanceID].Address}, out)
		}
		b := body.(contract.ReservationRequest)
		h, ok := s.workers[b.WorkerInstanceID]
		if !ok || !h.Ready || b.WorkerSlot < 0 || b.WorkerSlot >= h.Concurrency {
			return &control.StatusError{Status: 409}
		}
		for _, run := range s.runs {
			if run.CurrentAttempt != nil && run.CurrentAttempt.WorkerInstanceID == b.WorkerInstanceID && run.CurrentAttempt.WorkerSlot == b.WorkerSlot {
				return &control.StatusError{Status: 409}
			}
		}
		a := contract.Attempt{AttemptID: id + "-attempt-1", RunID: id, AttemptNumber: 1, WorkerInstanceID: b.WorkerInstanceID, WorkerSlot: b.WorkerSlot, LeaseToken: "exact-secret-lease-token", Fence: 1, LeaseExpiresAt: time.Now().Add(time.Minute), Status: "reserved"}
		r.CurrentAttempt = &a
		r.Status = "running"
		s.runs[id] = r
		s.reservations++
		return jsonCopy(contract.Reservation{Run: r, Attempt: a, Manifest: s.manifest, WorkerAddress: h.Address}, out)
	}
	if method == http.MethodPost && strings.HasSuffix(path, "/accepted") {
		b := body.(contract.LeaseMutation)
		r := s.runs[b.RunID]
		if r.CurrentAttempt == nil || b.LeaseToken != r.CurrentAttempt.LeaseToken || b.Fence != r.CurrentAttempt.Fence || b.WorkerInstanceID != r.CurrentAttempt.WorkerInstanceID {
			return &control.StatusError{Status: 409}
		}
		if s.loseAccepted {
			s.loseAccepted = false
			return control.ErrUnavailable
		}
		s.accepted[r.CurrentAttempt.AttemptID] = true
		return nil
	}
	if method == http.MethodGet && strings.HasPrefix(path, "/runs/") {
		return jsonCopy(s.runs[strings.TrimPrefix(path, "/runs/")], out)
	}
	return errors.New("unexpected test coordinator route")
}

type heldProcessor struct{ executions atomic.Int64 }

func (p *heldProcessor) Run(ctx context.Context, _ contract.RunRequest, _ sdk.RunContext) (contract.OutputManifest, error) {
	p.executions.Add(1)
	<-ctx.Done()
	return contract.OutputManifest{}, ctx.Err()
}

type suppliedResources struct{}

func (suppliedResources) ForAttempt(contract.RunRequest) (sdk.Artifacts, error) { return nil, nil }

type testReporter struct{}

func (testReporter) Report(context.Context, contract.Envelope) error { return nil }

type lostAck struct {
	Lifecycle
	lose atomic.Bool
}

func (c *lostAck) Run(ctx context.Context, r contract.RunRequest) (contract.RunResponse, error) {
	response, err := c.Lifecycle.Run(ctx, r)
	if err == nil && c.lose.Swap(false) {
		return contract.RunResponse{}, errors.New("lost network acknowledgement")
	}
	return response, err
}

func grpcFixtureWorker(t *testing.T, m contract.Manifest, name string) (*Worker, *heldProcessor, *lostAck) {
	t.Helper()
	processor := &heldProcessor{}
	host, err := workerhost.New(workerhost.Config{Manifest: m, InstancePrefix: name}, processor, suppliedResources{}, testReporter{})
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	rpc.Register(server, host)
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(); server.Stop(); host.Close(); _ = listener.Close() })
	client := &lostAck{Lifecycle: rpc.NewClient(conn)}
	return &Worker{Approved: config.Worker{Address: name + ":9091", ProcessorID: m.ProcessorID, Version: m.Version, Digest: m.ImplementationDigest, Service: m.WorkerService}, Client: client}, processor, client
}

func TestTwoManagersTwoGRPCWorkersReservationAndLostAcknowledgement(t *testing.T) {
	ledger := fixtureLedger(t)
	w1, p1, drop1 := grpcFixtureWorker(t, ledger.manifest, "one")
	w2, p2, _ := grpcFixtureWorker(t, ledger.manifest, "two")
	drop1.lose.Store(true)
	ledger.loseAccepted = true
	scope := contract.Scope{InstallationID: ledger.manifest.InstallationID, Namespace: ledger.manifest.Namespace}
	// Each replica has its own discovery state and persistent connections. They
	// share only the authoritative control API, as production managers do.
	clone := func(w *Worker) *Worker { return &Worker{Approved: w.Approved, Client: w.Client} }
	one := New(ledger, scope, []*Worker{w1, w2}, true, "", 8, time.Second)
	two := New(ledger, scope, []*Worker{clone(w1), clone(w2)}, true, "", 8, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	one.Tick(ctx)
	if p1.executions.Load()+p2.executions.Load() != 0 {
		t.Fatal("run dispatched before durable receipt")
	}
	ledger.mu.Lock()
	ledger.received["run-1"] = true
	ledger.received["run-2"] = true
	ledger.mu.Unlock()
	for i := 0; i < 4; i++ {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); one.Tick(ctx) }()
		go func() { defer wg.Done(); two.Tick(ctx) }()
		wg.Wait()
	}
	deadline := time.Now().Add(time.Second)
	for p1.executions.Load()+p2.executions.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.reservations != 2 || len(ledger.accepted) != 2 || p1.executions.Load() != 1 || p2.executions.Load() != 1 {
		t.Fatalf("reservations=%d accepted=%d worker executions=(%d,%d)", ledger.reservations, len(ledger.accepted), p1.executions.Load(), p2.executions.Load())
	}
	if one.Metrics.DispatchErrors.Load()+two.Metrics.DispatchErrors.Load() == 0 {
		t.Fatal("lost acknowledgements were not exercised")
	}
	ids := map[string]bool{}
	for _, r := range ledger.runs {
		ids[r.CurrentAttempt.WorkerInstanceID] = true
	}
	if len(ids) != 2 {
		t.Fatal("both runs occupied same reserved worker")
	}
}

func TestManagerRestartPollRecoversReceivedWorkWithoutBrokerRedelivery(t *testing.T) {
	ledger := fixtureLedger(t)
	delete(ledger.runs, "run-2")
	ledger.received["run-1"] = true
	w, processor, _ := grpcFixtureWorker(t, ledger.manifest, "recovered")
	scope := contract.Scope{InstallationID: ledger.manifest.InstallationID, Namespace: ledger.manifest.Namespace}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	restarted := New(ledger, scope, []*Worker{w}, true, "", 8, time.Second)
	restarted.Tick(ctx)
	deadline := time.Now().Add(time.Second)
	for processor.executions.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if processor.executions.Load() != 1 {
		t.Fatal("durably received work stranded after manager restart")
	}
}

func TestCoordinatorBatchBeyondLocalLimitStillDispatches(t *testing.T) {
	ledger := fixtureLedger(t)
	ledger.received["run-1"], ledger.received["run-2"] = true, true
	w, processor, _ := grpcFixtureWorker(t, ledger.manifest, "bounded")
	scope := contract.Scope{InstallationID: ledger.manifest.InstallationID, Namespace: ledger.manifest.Namespace}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := New(ledger, scope, []*Worker{w}, true, "", 1, time.Second)
	m.Tick(ctx)
	deadline := time.Now().Add(time.Second)
	for processor.executions.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if processor.executions.Load() != 1 || ledger.reservations != 1 || m.Metrics.PollErrors.Load() != 0 {
		t.Fatalf("larger backlog stalled: executions=%d reservations=%d polls=%d", processor.executions.Load(), ledger.reservations, m.Metrics.PollErrors.Load())
	}
}
