package manager

import (
	"context"
	"testing"
	"time"

	"github.com/zlovtnik/ssl-proxy/services/dq/internal/config"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
)

// recordingLifecycle captures the deadline the lease monitor attaches to its
// worker RPCs so the three-second bound can be asserted without a real worker.
type recordingLifecycle struct {
	Lifecycle
	healthDeadline chan time.Time
	cancelDeadline chan time.Time
	health         contract.HealthResponse
}

func (c *recordingLifecycle) Health(ctx context.Context) (contract.HealthResponse, error) {
	deadline, _ := ctx.Deadline()
	c.healthDeadline <- deadline
	return c.health, nil
}

func (c *recordingLifecycle) Cancel(ctx context.Context, _ contract.CancelRequest) (contract.CancelResponse, error) {
	deadline, _ := ctx.Deadline()
	c.cancelDeadline <- deadline
	return contract.CancelResponse{Status: "cancelled"}, nil
}

func assertThreeSecondBound(t *testing.T, what string, deadline time.Time) {
	t.Helper()
	remaining := time.Until(deadline)
	if deadline.IsZero() || remaining <= 0 {
		t.Fatalf("%s RPC context had no live deadline", what)
	}
	if remaining > 3*time.Second+100*time.Millisecond {
		t.Fatalf("%s RPC deadline %v exceeds the three-second bound", what, remaining)
	}
	if remaining < 2*time.Second {
		t.Fatalf("%s RPC deadline %v is unexpectedly small", what, remaining)
	}
}

func TestLeaseMonitorBoundsWorkerRPCsWithThreeSecondTimeouts(t *testing.T) {
	ledger := fixtureLedger(t)
	manifest := ledger.manifest
	client := &recordingLifecycle{
		healthDeadline: make(chan time.Time, 8),
		cancelDeadline: make(chan time.Time, 8),
		health: contract.HealthResponse{
			InitResponse: contract.InitResponse{
				WorkerInstanceID:     "worker-monitor-1",
				ProtocolVersion:      contract.ProtocolVersion,
				ProcessorID:          manifest.ProcessorID,
				ProcessorVersion:     manifest.Version,
				ImplementationDigest: manifest.ImplementationDigest,
				Concurrency:          1,
			},
			Ready:          true,
			AvailableSlots: []int{0},
		},
	}
	w := &Worker{
		Approved: config.Worker{Address: "monitor:9091", ProcessorID: manifest.ProcessorID, Version: manifest.Version, Digest: manifest.ImplementationDigest, Service: manifest.WorkerService},
		Client:   client,
	}
	scope := contract.Scope{InstallationID: manifest.InstallationID, Namespace: manifest.Namespace}
	m := New(ledger, scope, []*Worker{w}, true, "", 8, time.Second)

	ledger.mu.Lock()
	r := ledger.runs["run-1"]
	a := contract.Attempt{
		AttemptID:        "run-1-attempt-1",
		RunID:            r.RunID,
		WorkerInstanceID: "worker-monitor-1",
		LeaseToken:       "monitor-lease-token",
		Fence:            1,
		WorkerSlot:       0,
		LeaseExpiresAt:   time.Now().Add(time.Minute),
		Status:           "accepted",
	}
	r.CurrentAttempt = &a
	r.Status = "running"
	ledger.runs["run-1"] = r
	ledger.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.startMonitor(ctx, r, a, w)

	select {
	case deadline := <-client.healthDeadline:
		assertThreeSecondBound(t, "health", deadline)
	case <-time.After(5 * time.Second):
		t.Fatal("monitor never issued the health RPC")
	}

	// A cancelled run takes the terminal path, which issues a bounded cancel.
	ledger.mu.Lock()
	current := ledger.runs["run-1"]
	current.Status = "cancelled"
	ledger.runs["run-1"] = current
	ledger.mu.Unlock()

	select {
	case deadline := <-client.cancelDeadline:
		assertThreeSecondBound(t, "cancel", deadline)
	case <-time.After(5 * time.Second):
		t.Fatal("monitor never issued the cancel RPC")
	}

	giveUp := time.Now().Add(2 * time.Second)
	for time.Now().Before(giveUp) {
		m.mu.Lock()
		_, monitored := m.monitored[a.AttemptID]
		m.mu.Unlock()
		if monitored {
			time.Sleep(10 * time.Millisecond)
		} else {
			return
		}
	}
	t.Fatal("monitor goroutine did not exit after the cancel RPC")
}
