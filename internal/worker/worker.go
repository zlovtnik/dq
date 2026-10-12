// Package worker hosts a warm, isolated processor with bounded attempt state.
package worker

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/sdk"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/telemetry"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Config struct {
	Manifest       contract.Manifest
	InstancePrefix string
	CacheEntries   int
	ReportTimeout  time.Duration
}
type execution struct {
	request         contract.RunRequest
	fingerprint     string
	status          string
	cancel          context.CancelFunc
	deadline        time.Time
	done            bool
	cancelRequested bool
}
type Worker struct {
	cfg              Config
	identity         string
	processor        sdk.Processor
	resources        sdk.ResourceFactory
	reporter         sdk.Reporter
	schema           *jsonschema.Schema
	mu               sync.Mutex
	attempts         map[string]*execution
	slots            []string
	closed           bool
	unavailableUntil time.Time
	ctx              context.Context
	cancel           context.CancelFunc
	wait             sync.WaitGroup
}
type denyLoader struct{}

func (denyLoader) Load(string) (any, error) { return nil, errors.New("external schema forbidden") }
func New(cfg Config, processor sdk.Processor, resources sdk.ResourceFactory, reporter sdk.Reporter) (*Worker, error) {
	m := cfg.Manifest
	if processor == nil || resources == nil || reporter == nil || m.ProcessorID == "" || m.Version == "" || m.ImplementationDigest == "" || m.ProtocolVersion != contract.ProtocolVersion || m.Resources.Concurrency < 1 || m.Resources.Concurrency > 128 || m.Resources.TimeoutSeconds < 1 || m.Resources.TimeoutSeconds > 3600 {
		return nil, errors.New("invalid worker prerequisites")
	}
	if cfg.CacheEntries <= 0 {
		cfg.CacheEntries = 4096
	}
	if cfg.CacheEntries < m.Resources.Concurrency {
		return nil, errors.New("worker cache smaller than concurrency")
	}
	if cfg.ReportTimeout <= 0 {
		cfg.ReportTimeout = 10 * time.Second
	}
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader(m.ParameterSchema))
	if err != nil {
		return nil, errors.New("invalid parameter schema")
	}
	obj, ok := document.(map[string]any)
	if !ok || obj["$schema"] != "https://json-schema.org/draft/2020-12/schema" {
		return nil, errors.New("JSON Schema 2020-12 required")
	}
	canonical, err := contract.CanonicalJSON(m.ParameterSchema)
	if err != nil || contract.SHA256(canonical) != m.ParameterSchemaSHA256 {
		return nil, errors.New("schema checksum mismatch")
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.AssertFormat()
	compiler.UseLoader(denyLoader{})
	if err = compiler.AddResource("urn:dq:worker-parameters", document); err != nil {
		return nil, err
	}
	schema, err := compiler.Compile("urn:dq:worker-parameters")
	if err != nil {
		return nil, err
	}
	var boot [16]byte
	if _, err := rand.Read(boot[:]); err != nil {
		return nil, err
	}
	identity := cfg.InstancePrefix + "-" + hex.EncodeToString(boot[:])
	if cfg.InstancePrefix == "" {
		identity = "worker-" + hex.EncodeToString(boot[:])
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Worker{cfg: cfg, identity: identity, processor: processor, resources: resources, reporter: reporter, schema: schema, attempts: map[string]*execution{}, slots: make([]string, m.Resources.Concurrency), ctx: ctx, cancel: cancel}, nil
}
func (w *Worker) initResponse() contract.InitResponse {
	m := w.cfg.Manifest
	return contract.InitResponse{WorkerInstanceID: w.identity, ProtocolVersion: contract.ProtocolVersion, ProcessorID: m.ProcessorID, ProcessorVersion: m.Version, ImplementationDigest: m.ImplementationDigest, Concurrency: m.Resources.Concurrency}
}
func (w *Worker) Init(_ context.Context, req contract.InitRequest) (contract.InitResponse, error) {
	m := w.cfg.Manifest
	if req.ProtocolVersion != m.ProtocolVersion || req.ProcessorID != m.ProcessorID || req.ProcessorVersion != m.Version || req.ImplementationDigest != m.ImplementationDigest {
		return contract.InitResponse{}, status.Error(codes.FailedPrecondition, "deployment identity mismatch")
	}
	return w.initResponse(), nil
}
func (w *Worker) Health(_ context.Context) (contract.HealthResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	slots := []int{}
	for i, attempt := range w.slots {
		if attempt == "" {
			slots = append(slots, i)
		}
	}
	return contract.HealthResponse{InitResponse: w.initResponse(), Ready: !w.closed && !time.Now().Before(w.unavailableUntil), AvailableSlots: slots}, nil
}
func (w *Worker) Run(callCtx context.Context, req contract.RunRequest) (contract.RunResponse, error) {
	m := w.cfg.Manifest
	now := time.Now()
	if req.InstallationID != m.InstallationID || req.Namespace != m.Namespace || req.WorkerInstanceID != w.identity || req.ProcessorID != m.ProcessorID || req.ProcessorVersion != m.Version || req.ImplementationDigest != m.ImplementationDigest || req.PolicyVersion != m.PolicyVersion {
		return contract.RunResponse{}, status.Error(codes.FailedPrecondition, "reservation deployment mismatch")
	}
	if !contract.ValidSegment(req.RunID) || !contract.ValidSegment(req.AttemptID) || req.Fence < 1 || req.WorkerSlot < 0 || req.WorkerSlot >= len(w.slots) || !req.DeadlineAt.After(now) || !req.LeaseExpiresAt.After(now) || !contract.ValidSHA256(req.SourceRevisionDigest) || !json.Valid(req.Parameters) {
		return contract.RunResponse{}, status.Error(codes.InvalidArgument, "invalid attempt reservation")
	}
	canonical, err := contract.CanonicalJSON(req.Parameters)
	if err != nil || len(req.Traceparent) > 128 || len(req.Tracestate) > 512 {
		return contract.RunResponse{}, status.Error(codes.InvalidArgument, "invalid attempt document")
	}
	req.Parameters = canonical
	// A retried dispatch has a new transport span. Trace context does not
	// change the immutable work identity or cause a second execution.
	identity := req
	identity.Traceparent, identity.Tracestate = "", ""
	encoded, err := json.Marshal(identity)
	if err != nil || len(encoded) > contract.MaxRecordBytes {
		return contract.RunResponse{}, status.Error(codes.InvalidArgument, "invalid attempt size")
	}
	fingerprint := contract.SHA256(encoded)
	parameters, err := jsonschema.UnmarshalJSON(bytes.NewReader(req.Parameters))
	if err != nil || w.schema.Validate(parameters) != nil {
		return contract.RunResponse{}, status.Error(codes.InvalidArgument, "parameters violate pinned schema")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return contract.RunResponse{}, status.Error(codes.Unavailable, "worker draining")
	}
	if now.Before(w.unavailableUntil) {
		return contract.RunResponse{}, status.Error(codes.Unavailable, "worker dependency backoff")
	}
	if existing, ok := w.attempts[req.AttemptID]; ok {
		if existing.fingerprint != fingerprint {
			return contract.RunResponse{}, status.Error(codes.AlreadyExists, "conflicting attempt duplicate")
		}
		return contract.RunResponse{AttemptID: req.AttemptID, Fence: req.Fence, Status: existing.status}, nil
	}
	for id, entry := range w.attempts {
		if entry.done && !entry.deadline.After(now) {
			delete(w.attempts, id)
			continue
		}
		if entry.request.RunID == req.RunID && entry.request.Scope == req.Scope && entry.request.Fence >= req.Fence {
			return contract.RunResponse{}, status.Error(codes.FailedPrecondition, "stale fence")
		}
	}
	if len(w.attempts) >= w.cfg.CacheEntries {
		return contract.RunResponse{}, status.Error(codes.ResourceExhausted, "attempt cache full")
	}
	if w.slots[req.WorkerSlot] != "" {
		return contract.RunResponse{}, status.Error(codes.ResourceExhausted, "reserved slot unavailable")
	}
	resources, err := w.resources.ForAttempt(req)
	if err != nil {
		return contract.RunResponse{}, status.Error(codes.InvalidArgument, "invalid artifact reservation")
	}
	deadline := now.Add(time.Duration(m.Resources.TimeoutSeconds) * time.Second)
	if req.DeadlineAt.Before(deadline) {
		deadline = req.DeadlineAt
	}
	parent := telemetry.Extract(w.ctx, req.Traceparent, req.Tracestate)
	if remote := trace.SpanContextFromContext(callCtx); remote.IsValid() {
		parent = trace.ContextWithSpanContext(w.ctx, remote)
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	entry := &execution{request: req, fingerprint: fingerprint, status: "running", cancel: cancel, deadline: req.DeadlineAt}
	w.attempts[req.AttemptID] = entry
	w.slots[req.WorkerSlot] = req.AttemptID
	w.wait.Add(1)
	go w.execute(ctx, entry, resources)
	return contract.RunResponse{AttemptID: req.AttemptID, Fence: req.Fence, Status: "running"}, nil
}
func (w *Worker) envelope(req contract.RunRequest, kind string, sequence int64) contract.Envelope {
	return contract.Envelope{SchemaVersion: 1, EventID: req.AttemptID + "-" + kind + "-" + time.Now().UTC().Format("150405.000000000"), EventType: kind, InstallationID: req.InstallationID, Namespace: req.Namespace, RunID: req.RunID, AttemptID: req.AttemptID, ProcessorID: req.ProcessorID, ProcessorVersion: req.ProcessorVersion, Fence: req.Fence, OccurredAt: time.Now().UTC(), CorrelationID: req.RunID, Traceparent: req.Traceparent, Tracestate: req.Tracestate, Artifacts: []contract.ArtifactRef{}, SourceRevisionDigest: req.SourceRevisionDigest, Sequence: sequence}
}
func (w *Worker) report(envelope contract.Envelope) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(w.ctx), w.cfg.ReportTimeout)
	defer cancel()
	return w.reporter.Report(ctx, envelope)
}
func (w *Worker) execute(ctx context.Context, entry *execution, resources sdk.Artifacts) {
	defer w.wait.Done()
	defer entry.cancel()
	req := entry.request
	ctx, span := telemetry.Start(ctx, "worker.run")
	defer span.End()
	req.Traceparent, req.Tracestate = telemetry.Fields(ctx)
	var sequenceMu sync.Mutex
	var lastProgressSequence int64
	reportSequence := int64(1)
	reportsClosed := false
	started := w.envelope(req, "started", reportSequence)
	err := w.report(started)
	var output contract.OutputManifest
	if err == nil {
		output, err = w.processor.Run(ctx, req, sdk.RunContext{Artifacts: resources, Progress: func(ctx context.Context, sequence int64, metadata []byte) error {
			sequenceMu.Lock()
			defer sequenceMu.Unlock()
			if reportsClosed || sequence <= lastProgressSequence || len(metadata) > 4096 || !json.Valid(metadata) {
				return sdk.Permanent("progress_invalid")
			}
			reportSequence++
			progress := w.envelope(req, "progress", reportSequence)
			progress.Summary = append([]byte(nil), metadata...)
			if _, err := contract.EncodeEnvelope(progress); err != nil {
				return err
			}
			if err := w.reporter.Report(ctx, progress); err != nil {
				return err
			}
			lastProgressSequence = sequence
			return nil
		}})
	}
	sequenceMu.Lock()
	reportsClosed = true
	reportSequence++
	terminalSequence := reportSequence
	sequenceMu.Unlock()
	state := "output_ready"
	if err == nil {
		if ctx.Err() != nil {
			err = ctx.Err()
		} else if output.SourceRevisionDigest != req.SourceRevisionDigest || len(output.OutputRefs) == 0 {
			err = sdk.Permanent("output_invalid")
		}
	}
	if err == nil {
		for _, ref := range output.OutputRefs {
			if ref.Validate() != nil || ref.ProducerRunID != req.RunID || ref.ProducerAttemptID != req.AttemptID || ref.InstallationID != req.InstallationID || ref.Namespace != req.Namespace {
				err = sdk.Permanent("output_invalid")
				break
			}
		}
	}
	if err == nil {
		event := w.envelope(req, "output_ready", terminalSequence)
		event.Artifacts = output.OutputRefs
		event.Summary = output.Summary
		if _, err = contract.EncodeEnvelope(event); err == nil {
			err = w.report(event)
		}
	}
	if err != nil {
		telemetry.Fail(span)
		state = "failed"
		event := w.envelope(req, "attempt_failed", terminalSequence+1)
		event.ErrorClass = "transient"
		event.ErrorCode = "dependency_unavailable"
		var failure *sdk.Failure
		if errors.As(err, &failure) {
			event.ErrorClass = failure.Class
			event.ErrorCode = failure.Code
		}
		if errors.Is(err, context.Canceled) {
			w.mu.Lock()
			cancelRequested := entry.cancelRequested
			w.mu.Unlock()
			if cancelRequested {
				state = "cancelled"
				event.ErrorCode = "cancelled"
				event.ErrorClass = "permanent"
			} else {
				event.ErrorCode = "worker_interrupted"
				event.ErrorClass = "transient"
			}
		}
		if errors.Is(err, context.DeadlineExceeded) {
			event.ErrorCode = "attempt_deadline"
		}
		_ = w.report(event)
	}
	w.mu.Lock()
	if err != nil && state == "failed" {
		var failure *sdk.Failure
		if !errors.As(err, &failure) {
			w.unavailableUntil = time.Now().Add(5 * time.Second)
		}
	}
	entry.status = state
	entry.done = true
	if w.slots[req.WorkerSlot] == req.AttemptID {
		w.slots[req.WorkerSlot] = ""
	}
	w.mu.Unlock()
}
func (w *Worker) Cancel(_ context.Context, req contract.CancelRequest) (contract.CancelResponse, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	entry, ok := w.attempts[req.AttemptID]
	if !ok {
		return contract.CancelResponse{}, status.Error(codes.NotFound, "attempt not found")
	}
	if req.Scope != entry.request.Scope || req.RunID != entry.request.RunID || req.Fence != entry.request.Fence {
		return contract.CancelResponse{}, status.Error(codes.FailedPrecondition, "stale cancellation fence")
	}
	if !entry.done {
		entry.cancelRequested = true
		entry.cancel()
		entry.status = "cancelling"
	}
	return contract.CancelResponse{Status: entry.status}, nil
}
func (w *Worker) Close() { w.mu.Lock(); w.closed = true; w.cancel(); w.mu.Unlock(); w.wait.Wait() }
