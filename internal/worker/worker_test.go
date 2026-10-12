package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zlovtnik/ssl-proxy/services/delip-queve/pkg/contract"
	"github.com/zlovtnik/ssl-proxy/services/delip-queve/pkg/rpc"
	"github.com/zlovtnik/ssl-proxy/services/delip-queve/pkg/sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type resources struct{ err error }

func (r resources) ForAttempt(contract.RunRequest) (sdk.Artifacts, error) {
	return unusedArtifacts{}, r.err
}

type unusedArtifacts struct{}

func (unusedArtifacts) OpenRead(context.Context, string, contract.ArtifactRef) (io.ReadCloser, error) {
	return nil, errors.New("unused")
}
func (unusedArtifacts) CreateOutput(context.Context, string, string) (sdk.Output, error) {
	return nil, errors.New("unused")
}

type blocking struct {
	calls   atomic.Int64
	release chan struct{}
	started chan struct{}
}

func (p *blocking) Run(ctx context.Context, req contract.RunRequest, _ sdk.RunContext) (contract.OutputManifest, error) {
	p.calls.Add(1)
	if p.started != nil {
		select {
		case p.started <- struct{}{}:
		default:
		}
	}
	select {
	case <-ctx.Done():
		return contract.OutputManifest{}, ctx.Err()
	case <-p.release:
	}
	ref := contract.ArtifactRef{ArtifactID: "output", InstallationID: req.InstallationID, Namespace: req.Namespace, URI: "s3://dq-artifacts/i/n/runs/" + req.RunID + "/attempts/" + req.AttemptID + "/output", VersionID: "version", ETag: "etag", SHA256: strings.Repeat("a", 64), SchemaID: "proposal", MediaType: "application/json", SizeBytes: 4, ProducerRunID: req.RunID, ProducerAttemptID: req.AttemptID}
	return contract.OutputManifest{SchemaID: "proposal", OutputRefs: []contract.ArtifactRef{ref}, SourceRevisionDigest: req.SourceRevisionDigest, Summary: json.RawMessage(`{"count":1}`)}, nil
}

type reports struct {
	mu     sync.Mutex
	events []contract.Envelope
	err    error
}

type boundedReporter struct{}

func (boundedReporter) Report(ctx context.Context, _ contract.Envelope) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("report timeout missing")
	}
	return nil
}

func (r *reports) Report(_ context.Context, event contract.Envelope) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
	return r.err
}
func (r *reports) last() contract.Envelope {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.events) == 0 {
		return contract.Envelope{}
	}
	return r.events[len(r.events)-1]
}
func fixtureManifest() contract.Manifest {
	schema := json.RawMessage(`{"$schema":"https://json-schema.org/draft/2020-12/schema","additionalProperties":false,"properties":{"x":{"maximum":10,"minimum":1,"type":"integer"}},"required":["x"],"type":"object"}`)
	return contract.Manifest{InstallationID: "i", Namespace: "n", ProcessorID: "test", Version: "1", ImplementationDigest: "sha256:" + strings.Repeat("a", 64), ProtocolVersion: contract.ProtocolVersion, PolicyVersion: "1", ParameterSchema: schema, ParameterSchemaSHA256: contract.SHA256(schema), Resources: contract.ResourceLimits{Concurrency: 1, TimeoutSeconds: 45, MaxInputBytes: 1024, MaxOutputBytes: 1024}}
}
func newFixture(t testing.TB, cache int) (*Worker, *blocking, *reports) {
	t.Helper()
	p := &blocking{release: make(chan struct{}), started: make(chan struct{}, 1)}
	r := &reports{}
	w, err := New(Config{Manifest: fixtureManifest(), CacheEntries: cache}, p, resources{}, r)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Close)
	return w, p, r
}
func request(w *Worker) contract.RunRequest {
	return contract.RunRequest{Scope: contract.Scope{InstallationID: "i", Namespace: "n"}, RunID: "run", AttemptID: "attempt", Fence: 2, WorkerInstanceID: w.identity, WorkerSlot: 0, ProcessorID: "test", ProcessorVersion: "1", ImplementationDigest: w.cfg.Manifest.ImplementationDigest, PolicyVersion: "1", Parameters: json.RawMessage(`{"x":1}`), SourceRevisionDigest: strings.Repeat("b", 64), DeadlineAt: time.Now().Add(time.Minute), LeaseExpiresAt: time.Now().Add(time.Minute)}
}
func waitDone(t testing.TB, w *Worker, req contract.RunRequest, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		w.mu.Lock()
		entry := w.attempts[req.AttemptID]
		done := entry != nil && entry.done
		got := ""
		if entry != nil {
			got = entry.status
		}
		w.mu.Unlock()
		if done {
			if got != want {
				t.Fatalf("status %s want %s", got, want)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("execution did not finish")
}

func TestImmutableDuplicateAndAtomicSlot(t *testing.T) {
	w, p, r := newFixture(t, 8)
	req := request(w)
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := w.Run(context.Background(), req); err != nil {
				t.Errorf("duplicate: %v", err)
			}
		}()
	}
	wg.Wait()
	<-p.started
	if p.calls.Load() != 1 {
		t.Fatalf("executions %d", p.calls.Load())
	}
	health, _ := w.Health(context.Background())
	if len(health.AvailableSlots) != 0 {
		t.Fatal("active slot advertised")
	}
	conflicting := req
	conflicting.Parameters = json.RawMessage(`{"x":2}`)
	if _, err := w.Run(context.Background(), conflicting); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflict %v", err)
	}
	other := req
	other.RunID = "other"
	other.AttemptID = "other"
	if _, err := w.Run(context.Background(), other); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("slot %v", err)
	}
	stale := req
	stale.AttemptID = "stale"
	stale.Fence = 1
	if _, err := w.Run(context.Background(), stale); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale %v", err)
	}
	close(p.release)
	waitDone(t, w, req, "output_ready")
	if r.last().EventType != "output_ready" || len(r.last().Artifacts) != 1 {
		t.Fatal("output report missing")
	}
	got, err := w.Run(context.Background(), req)
	if err != nil || got.Status != "output_ready" {
		t.Fatalf("lost ack retry %v %v", got, err)
	}
}
func TestRejectsReservationBoundaries(t *testing.T) {
	w, _, _ := newFixture(t, 8)
	cases := []struct {
		name   string
		mutate func(*contract.RunRequest)
		code   codes.Code
	}{{"boot", func(r *contract.RunRequest) { r.WorkerInstanceID = "old-boot" }, codes.FailedPrecondition}, {"version", func(r *contract.RunRequest) { r.ProcessorVersion = "2" }, codes.FailedPrecondition}, {"digest", func(r *contract.RunRequest) { r.ImplementationDigest = "wrong" }, codes.FailedPrecondition}, {"scope", func(r *contract.RunRequest) { r.Namespace = "other" }, codes.FailedPrecondition}, {"slot", func(r *contract.RunRequest) { r.WorkerSlot = 1 }, codes.InvalidArgument}, {"expired lease", func(r *contract.RunRequest) { r.LeaseExpiresAt = time.Now().Add(-time.Second) }, codes.InvalidArgument}, {"deadline", func(r *contract.RunRequest) { r.DeadlineAt = time.Now().Add(-time.Second) }, codes.InvalidArgument}, {"schema", func(r *contract.RunRequest) { r.Parameters = json.RawMessage(`{"x":11}`) }, codes.InvalidArgument}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := request(w)
			tc.mutate(&req)
			if _, err := w.Run(context.Background(), req); status.Code(err) != tc.code {
				t.Fatalf("code %v: %v", status.Code(err), err)
			}
		})
	}
}
func TestLeaseExpiryDoesNotCancelRenewableExecution(t *testing.T) {
	w, p, _ := newFixture(t, 8)
	req := request(w)
	req.LeaseExpiresAt = time.Now().Add(30 * time.Millisecond)
	if _, err := w.Run(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	<-p.started
	time.Sleep(50 * time.Millisecond)
	w.mu.Lock()
	done := w.attempts[req.AttemptID].done
	w.mu.Unlock()
	if done {
		t.Fatal("dispatch-time lease expiry cancelled the execution")
	}
	if _, err := w.Cancel(context.Background(), contract.CancelRequest{Scope: req.Scope, RunID: req.RunID, AttemptID: req.AttemptID, Fence: req.Fence}); err != nil {
		t.Fatal(err)
	}
	waitDone(t, w, req, "cancelled")
}

func TestTerminalReportSurvivesWorkerContextCancellation(t *testing.T) {
	w, _, _ := newFixture(t, 8)
	w.reporter = boundedReporter{}
	w.cancel()
	if err := w.report(contract.Envelope{}); err != nil {
		t.Fatal(err)
	}
}
func TestCacheNeverEvictsActiveOrUnexpired(t *testing.T) {
	w, p, _ := newFixture(t, 1)
	req := request(w)
	if _, err := w.Run(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	<-p.started
	close(p.release)
	waitDone(t, w, req, "output_ready")
	next := req
	next.RunID = "next"
	next.AttemptID = "next"
	if _, err := w.Run(context.Background(), next); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("unexpired reservation evicted: %v", err)
	}
}
func TestDependencyFailureReleasesCredit(t *testing.T) {
	w, p, r := newFixture(t, 8)
	r.err = errors.New("storage credentials must never enter events")
	req := request(w)
	if _, err := w.Run(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	waitDone(t, w, req, "failed")
	if p.calls.Load() != 0 {
		t.Fatal("computed despite report outage")
	}
	health, _ := w.Health(context.Background())
	if len(health.AvailableSlots) != 1 {
		t.Fatal("slot leaked")
	}
	if r.last().ErrorCode != "dependency_unavailable" {
		t.Fatal("unsanitized failure")
	}
}
func TestPersistentGRPCLifecycleLostAcknowledgement(t *testing.T) {
	w, p, _ := newFixture(t, 8)
	listener := bufconn.Listen(128 * 1024)
	server := grpc.NewServer()
	rpc.Register(server, w)
	go server.Serve(listener)
	defer server.Stop()
	conn, err := grpc.NewClient("passthrough:///worker", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := rpc.NewClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	m := w.cfg.Manifest
	init, err := client.Init(ctx, contract.InitRequest{ProcessorID: m.ProcessorID, ProcessorVersion: m.Version, ImplementationDigest: m.ImplementationDigest, ProtocolVersion: m.ProtocolVersion})
	if err != nil || init.WorkerInstanceID != w.identity {
		t.Fatalf("init: %v", err)
	}
	req := request(w)
	if _, err = client.Run(ctx, req); err != nil {
		t.Fatal(err)
	}
	if _, err = client.Run(ctx, req); err != nil {
		t.Fatal(err)
	}
	<-p.started
	if p.calls.Load() != 1 {
		t.Fatal("duplicate grpc execution")
	}
	health, err := client.Health(ctx)
	if err != nil || len(health.AvailableSlots) != 0 {
		t.Fatalf("health %v", err)
	}
	if _, err = client.Cancel(ctx, contract.CancelRequest{Scope: req.Scope, RunID: req.RunID, AttemptID: req.AttemptID, Fence: req.Fence}); err != nil {
		t.Fatal(err)
	}
	waitDone(t, w, req, "cancelled")
}
func TestBootIdentityChanges(t *testing.T) {
	one, _, _ := newFixture(t, 8)
	two, _, _ := newFixture(t, 8)
	if one.identity == two.identity {
		t.Fatal("boot identity reused")
	}
	req := request(one)
	if _, err := two.Run(context.Background(), req); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("restart accepted old reservation")
	}
}
func BenchmarkWarmDuplicateDispatch(b *testing.B) {
	w, p, _ := newFixture(b, 8)
	req := request(w)
	if _, err := w.Run(context.Background(), req); err != nil {
		b.Fatal(err)
	}
	<-p.started
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := w.Run(context.Background(), req); err != nil {
			b.Fatal(err)
		}
	}
}
