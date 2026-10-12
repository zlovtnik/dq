package manager

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zlovtnik/ssl-proxy/services/dq/internal/config"
	"github.com/zlovtnik/ssl-proxy/services/dq/internal/control"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/telemetry"
)

type Lifecycle interface {
	Init(context.Context, contract.InitRequest) (contract.InitResponse, error)
	Run(context.Context, contract.RunRequest) (contract.RunResponse, error)
	Cancel(context.Context, contract.CancelRequest) (contract.CancelResponse, error)
	Health(context.Context) (contract.HealthResponse, error)
}

type Worker struct {
	Approved config.Worker
	Client   Lifecycle
	Health   contract.HealthResponse
}
type Metrics struct {
	Dispatches          atomic.Uint64
	DispatchErrors      atomic.Uint64
	DispatchNanoseconds atomic.Uint64
	Receipts            atomic.Uint64
	Quarantines         atomic.Uint64
	Renewals            atomic.Uint64
	PollErrors          atomic.Uint64
}
type Manager struct {
	Control      control.API
	Scope        contract.Scope
	BetaEnabled  bool
	RemoteOrigin string
	Limit        int
	Interval     time.Duration
	Metrics      Metrics
	workers      []*Worker
	mu           sync.RWMutex
	// These entries only bound local lease-monitor goroutines. Octopus owns the
	// attempts and recovers them independently when this process disappears.
	monitored map[string]bool
}

func New(api control.API, scope contract.Scope, workers []*Worker, beta bool, remoteOrigin string, limit int, interval time.Duration) *Manager {
	return &Manager{Control: api, Scope: scope, workers: workers, BetaEnabled: beta, RemoteOrigin: remoteOrigin, Limit: limit, Interval: interval, monitored: map[string]bool{}}
}

func (m *Manager) Manifest(ctx context.Context, scope contract.Scope, id, version string) (contract.Manifest, error) {
	var manifest contract.Manifest
	if !contract.ValidSegment(id) || version != "" && !contract.ValidSegment(version) {
		return manifest, errors.New("invalid processor identity")
	}
	if version == "" {
		var catalog struct {
			Items []contract.Manifest `json:"items"`
		}
		if err := m.Control.Do(ctx, http.MethodGet, "/processors", control.Query(scope.InstallationID, scope.Namespace), nil, &catalog); err != nil {
			return manifest, err
		}
		for _, item := range catalog.Items {
			if item.ProcessorID == id {
				manifest = item
				break
			}
		}
		if manifest.ProcessorID == "" {
			return manifest, &control.StatusError{Status: http.StatusNotFound}
		}
	} else {
		if err := m.Control.Do(ctx, http.MethodGet, "/processor-versions/"+url.PathEscape(id)+"/"+url.PathEscape(version), control.Query(scope.InstallationID, scope.Namespace), nil, &manifest); err != nil {
			return manifest, err
		}
	}
	if manifest.InstallationID != scope.InstallationID || manifest.Namespace != scope.Namespace || manifest.ProcessorID != id || version != "" && manifest.Version != version {
		return manifest, errors.New("coordinator manifest identity mismatch")
	}
	if err := ValidateManifest(manifest, m.RemoteOrigin); err != nil {
		return manifest, err
	}
	return manifest, nil
}

func (m *Manager) Admit(ctx context.Context, req contract.AdmitRequest) (contract.Run, error) {
	ctx = telemetry.Extract(ctx, req.Traceparent, req.Tracestate)
	ctx, span := telemetry.Start(ctx, "dq.admit")
	defer span.End()
	req.Traceparent, req.Tracestate = telemetry.Fields(ctx)
	var run contract.Run
	if !m.BetaEnabled {
		return run, &control.StatusError{Status: http.StatusForbidden}
	}
	if req.IdempotencyKey == "" || len(req.IdempotencyKey) > 200 || req.Operation == "" || len(req.Operation) > 100 {
		return run, errors.New("invalid invocation identity")
	}
	// Resolve invocation identity before consulting a mutable default version.
	// Octopus performs the fingerprint check before returning an existing run.
	// This preserves an omitted version across a later default-version change.
	var existing contract.Run
	lookup := m.Control.Do(ctx, http.MethodGet, "/invocations/"+url.PathEscape(req.IdempotencyKey), control.Query(req.InstallationID, req.Namespace), nil, &existing)
	if lookup == nil {
		canonical, err := CanonicalJSON(req.Parameters)
		if err != nil {
			return run, err
		}
		req.Parameters = canonical
		err = m.Control.Do(ctx, http.MethodPost, "/runs", nil, req, &run)
		return run, err
	}
	var notFound *control.StatusError
	if !errors.As(lookup, &notFound) || notFound.Status != http.StatusNotFound {
		return run, lookup
	}
	manifest, err := m.Manifest(ctx, contract.Scope{InstallationID: req.InstallationID, Namespace: req.Namespace}, req.ProcessorID, req.ProcessorVersion)
	if err != nil {
		return run, err
	}
	parameters, err := ValidateParameters(manifest, req.Parameters)
	if err != nil {
		return run, err
	}
	req.Parameters = parameters
	err = m.Control.Do(ctx, http.MethodPost, "/runs", nil, req, &run)
	return run, err
}

func (m *Manager) Register(ctx context.Context, manifest contract.Manifest) error {
	if err := ValidateManifest(manifest, m.RemoteOrigin); err != nil {
		return err
	}
	m.mu.RLock()
	ready := false
	for _, worker := range m.workers {
		if matches(worker.Approved, manifest) && worker.Health.Ready && validHealth(worker.Approved, worker.Health) {
			ready = true
			break
		}
	}
	m.mu.RUnlock()
	if !ready {
		return &control.StatusError{Status: http.StatusServiceUnavailable}
	}
	return m.Control.Do(ctx, http.MethodPost, "/processor-versions", nil, manifest, nil)
}

func (m *Manager) Run(ctx context.Context) {
	ticker := time.NewTicker(m.Interval)
	defer ticker.Stop()
	for {
		m.Tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *Manager) Tick(ctx context.Context) {
	ctx = control.ServiceContext(ctx)
	m.refreshWorkers(ctx)
	if !m.BetaEnabled {
		return
	}
	var pending contract.Dispatches
	query := control.Query(m.Scope.InstallationID, m.Scope.Namespace)
	query.Set("limit", itoa(m.Limit))
	if err := m.Control.Do(ctx, http.MethodGet, "/dispatches", query, nil, &pending); err != nil {
		m.Metrics.PollErrors.Add(1)
		return
	}
	if len(pending.Items) > m.Limit {
		pending.Items = pending.Items[:m.Limit]
	}
	credits := make(chan struct{}, m.Limit)
	var wg sync.WaitGroup
	for _, item := range pending.Items {
		if ctx.Err() != nil {
			return
		}
		credits <- struct{}{}
		wg.Add(1)
		go func(item contract.Dispatch) {
			defer wg.Done()
			defer func() { <-credits }()
			if err := m.dispatch(ctx, item); err != nil {
				m.Metrics.DispatchErrors.Add(1)
			}
		}(item)
	}
	wg.Wait()
}

func (m *Manager) refreshWorkers(ctx context.Context) {
	for _, worker := range m.workers {
		callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		health, err := worker.Client.Health(callCtx)
		if err == nil && validHealth(worker.Approved, health) {
			init, initErr := worker.Client.Init(callCtx, contract.InitRequest{ProtocolVersion: contract.ProtocolVersion, ProcessorID: worker.Approved.ProcessorID, ProcessorVersion: worker.Approved.Version, ImplementationDigest: worker.Approved.Digest})
			if initErr != nil || init.WorkerInstanceID != health.WorkerInstanceID || init.ImplementationDigest != health.ImplementationDigest {
				err = errors.New("worker initialization mismatch")
			}
		} else {
			err = errors.New("worker unavailable")
		}
		cancel()
		m.mu.Lock()
		if err != nil {
			health.Ready = false
		}
		worker.Health = health
		m.mu.Unlock()
		if health.WorkerInstanceID == "" {
			continue
		}
		body := contract.Heartbeat{Scope: m.Scope, ProcessorID: worker.Approved.ProcessorID, ProcessorVersion: worker.Approved.Version, ImplementationDigest: worker.Approved.Digest, WorkerInstanceID: health.WorkerInstanceID, Address: worker.Approved.Address, Concurrency: health.Concurrency, Ready: health.Ready}
		if err := m.Control.Do(ctx, http.MethodPost, "/workers/"+url.PathEscape(health.WorkerInstanceID)+"/heartbeat", nil, body, nil); err != nil {
			m.Metrics.PollErrors.Add(1)
		}
	}
}

func validHealth(w config.Worker, h contract.HealthResponse) bool {
	if h.WorkerInstanceID == "" || h.ProtocolVersion != contract.ProtocolVersion || h.ProcessorID != w.ProcessorID || h.ProcessorVersion != w.Version || h.ImplementationDigest != w.Digest || h.Concurrency < 1 || h.Concurrency > 128 {
		return false
	}
	seen := map[int]bool{}
	for _, slot := range h.AvailableSlots {
		if slot < 0 || slot >= h.Concurrency || seen[slot] {
			return false
		}
		seen[slot] = true
	}
	return true
}
func matches(w config.Worker, m contract.Manifest) bool {
	return w.ProcessorID == m.ProcessorID && w.Version == m.Version && w.Digest == m.ImplementationDigest && w.Service == m.WorkerService
}

func (m *Manager) dispatch(ctx context.Context, item contract.Dispatch) error {
	r := item.Run
	ctx = telemetry.Extract(ctx, r.Traceparent, r.Tracestate)
	ctx, span := telemetry.Start(ctx, "dq.dispatch")
	defer span.End()
	if r.InstallationID != m.Scope.InstallationID || r.Namespace != m.Scope.Namespace || r.ProcessorID != item.Manifest.ProcessorID || r.ProcessorVersion != item.Manifest.Version || r.ImplementationDigest != item.Manifest.ImplementationDigest {
		return errors.New("dispatch identity mismatch")
	}
	if err := ValidateManifest(item.Manifest, m.RemoteOrigin); err != nil {
		return err
	}
	if canonical, err := ValidateParameters(item.Manifest, r.Parameters); err != nil || contract.SHA256(canonical) != r.ParametersSHA256 {
		return errors.New("dispatch parameters checksum mismatch")
	}
	var selected *Worker
	var slot int
	var selectedWorkerID string
	m.mu.Lock()
	for _, w := range m.workers {
		if !matches(w.Approved, item.Manifest) || !w.Health.Ready || !validHealth(w.Approved, w.Health) {
			continue
		}
		if item.Attempt != nil {
			if w.Health.WorkerInstanceID == item.Attempt.WorkerInstanceID {
				selected = w
				slot = item.Attempt.WorkerSlot
				break
			}
		} else if len(w.Health.AvailableSlots) > 0 {
			selected = w
			slot = w.Health.AvailableSlots[0]
			selectedWorkerID = w.Health.WorkerInstanceID
			w.Health.AvailableSlots = removeSlot(w.Health.AvailableSlots, slot)
			break
		}
	}
	m.mu.Unlock()
	if selected == nil {
		return errors.New("no approved capacity")
	}
	reservation := contract.Reservation{Run: r, Manifest: item.Manifest}
	if item.Attempt != nil {
		reservation.Attempt = *item.Attempt
	} else {
		body := contract.ReservationRequest{Scope: m.Scope, WorkerInstanceID: selectedWorkerID, WorkerSlot: slot}
		if err := m.Control.Do(ctx, http.MethodPost, "/runs/"+url.PathEscape(r.RunID)+"/attempts/reserve", nil, body, &reservation); err != nil {
			m.mu.Lock()
			for _, w := range m.workers {
				if w.Health.WorkerInstanceID == selectedWorkerID {
					w.Health.AvailableSlots = addSlot(w.Health.AvailableSlots, slot)
					break
				}
			}
			m.mu.Unlock()
			return err
		}
		// A racing manager may already have reserved another worker. Always use
		// the authoritative worker returned by Octopus, never the local choice.
		reservedWorker := (*Worker)(nil)
		m.mu.Lock()
		for _, w := range m.workers {
			if w.Health.WorkerInstanceID == reservation.Attempt.WorkerInstanceID && matches(w.Approved, reservation.Manifest) && w.Health.Ready {
				reservedWorker = w
				w.Health.AvailableSlots = removeSlot(w.Health.AvailableSlots, reservation.Attempt.WorkerSlot)
				break
			}
		}
		if selectedWorkerID != reservation.Attempt.WorkerInstanceID || slot != reservation.Attempt.WorkerSlot {
			for _, w := range m.workers {
				if w.Health.WorkerInstanceID == selectedWorkerID {
					w.Health.AvailableSlots = addSlot(w.Health.AvailableSlots, slot)
					break
				}
			}
		}
		selected = reservedWorker
		m.mu.Unlock()
		if selected == nil {
			return errors.New("reserved worker unavailable")
		}
	}
	a := reservation.Attempt
	r = reservation.Run
	if reservation.WorkerAddress != "" && reservation.WorkerAddress != selected.Approved.Address {
		return errors.New("reserved address is not approved")
	}
	if a.AttemptID == "" || a.RunID != r.RunID || a.Fence < 1 || a.LeaseToken == "" || a.LeaseExpiresAt.Before(time.Now()) || a.WorkerSlot < 0 {
		return errors.New("invalid reservation")
	}
	started := time.Now()
	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	traceparent, tracestate := telemetry.Fields(ctx)
	response, err := selected.Client.Run(callCtx, contract.RunRequest{Scope: m.Scope, RunID: r.RunID, AttemptID: a.AttemptID, Fence: a.Fence, WorkerInstanceID: a.WorkerInstanceID, WorkerSlot: a.WorkerSlot, ProcessorID: r.ProcessorID, ProcessorVersion: r.ProcessorVersion, ImplementationDigest: r.ImplementationDigest, Operation: r.Operation, Parameters: r.Parameters, InputRefs: r.InputRefs, PolicyVersion: r.PolicyVersion, SourceRevisionDigest: r.SourceRevisionDigest, DeadlineAt: r.DeadlineAt, LeaseExpiresAt: a.LeaseExpiresAt, Traceparent: traceparent, Tracestate: tracestate})
	if err != nil {
		return errors.New("worker acknowledgement unavailable")
	}
	if response.AttemptID != a.AttemptID || response.Fence != a.Fence || response.Status == "" {
		return errors.New("worker acknowledgement mismatch")
	}
	lease := contract.LeaseMutation{Scope: m.Scope, RunID: r.RunID, WorkerInstanceID: a.WorkerInstanceID, LeaseToken: a.LeaseToken, Fence: a.Fence}
	if err := m.Control.Do(ctx, http.MethodPost, "/attempts/"+url.PathEscape(a.AttemptID)+"/accepted", nil, lease, nil); err != nil {
		return err
	}
	m.Metrics.Dispatches.Add(1)
	m.Metrics.DispatchNanoseconds.Add(uint64(time.Since(started)))
	m.startMonitor(ctx, r, a, selected)
	return nil
}

func removeSlot(slots []int, target int) []int {
	for index, slot := range slots {
		if slot == target {
			return append(slots[:index], slots[index+1:]...)
		}
	}
	return slots
}

func addSlot(slots []int, target int) []int {
	for _, slot := range slots {
		if slot == target {
			return slots
		}
	}
	return append(slots, target)
}

func (m *Manager) startMonitor(ctx context.Context, r contract.Run, a contract.Attempt, w *Worker) {
	m.mu.Lock()
	if m.monitored[a.AttemptID] {
		m.mu.Unlock()
		return
	}
	m.monitored[a.AttemptID] = true
	m.mu.Unlock()
	go func() {
		defer func() { m.mu.Lock(); delete(m.monitored, a.AttemptID); m.mu.Unlock() }()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			var current contract.Run
			if err := m.Control.Do(ctx, http.MethodGet, "/runs/"+url.PathEscape(r.RunID), control.Query(m.Scope.InstallationID, m.Scope.Namespace), nil, &current); err != nil {
				continue
			}
			if current.CurrentAttempt == nil || current.CurrentAttempt.AttemptID != a.AttemptID || current.CurrentAttempt.Fence != a.Fence || current.Status == "cancelled" || current.Status == "failed" || current.Status == "succeeded" {
				callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				_, _ = w.Client.Cancel(callCtx, contract.CancelRequest{Scope: m.Scope, RunID: r.RunID, AttemptID: a.AttemptID, Fence: a.Fence})
				cancel()
				return
			}
			healthCtx, cancelHealth := context.WithTimeout(ctx, 3*time.Second)
			health, err := w.Client.Health(healthCtx)
			cancelHealth()
			if err != nil {
				continue
			}
			if health.WorkerInstanceID != a.WorkerInstanceID || !validHealth(w.Approved, health) {
				return
			}
			if time.Now().After(r.DeadlineAt) {
				callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				_, _ = w.Client.Cancel(callCtx, contract.CancelRequest{Scope: m.Scope, RunID: r.RunID, AttemptID: a.AttemptID, Fence: a.Fence})
				cancel()
				return
			}
			if time.Until(current.CurrentAttempt.LeaseExpiresAt) > 5*time.Second {
				continue
			}
			body := contract.LeaseMutation{Scope: m.Scope, RunID: r.RunID, WorkerInstanceID: a.WorkerInstanceID, LeaseToken: a.LeaseToken, Fence: a.Fence}
			if err := m.Control.Do(ctx, http.MethodPost, "/attempts/"+url.PathEscape(a.AttemptID)+"/renew", nil, body, nil); err != nil {
				callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				_, _ = w.Client.Cancel(callCtx, contract.CancelRequest{Scope: m.Scope, RunID: r.RunID, AttemptID: a.AttemptID, Fence: a.Fence})
				cancel()
				return
			}
			m.Metrics.Renewals.Add(1)
		}
	}()
}

func (m *Manager) CancelWorker(ctx context.Context, run contract.Run) {
	if run.CurrentAttempt == nil {
		return
	}
	a := run.CurrentAttempt
	m.mu.RLock()
	var client Lifecycle
	for _, w := range m.workers {
		if w.Health.WorkerInstanceID == a.WorkerInstanceID {
			client = w.Client
			break
		}
	}
	m.mu.RUnlock()
	if client == nil {
		return
	}
	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, _ = client.Cancel(callCtx, contract.CancelRequest{Scope: contract.Scope{InstallationID: run.InstallationID, Namespace: run.Namespace}, RunID: run.RunID, AttemptID: a.AttemptID, Fence: a.Fence})
}

func itoa(v int) string { b, _ := json.Marshal(v); return string(b) }
