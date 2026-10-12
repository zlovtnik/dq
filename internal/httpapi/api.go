package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/zlovtnik/ssl-proxy/services/dq/internal/config"
	"github.com/zlovtnik/ssl-proxy/services/dq/internal/control"
	"github.com/zlovtnik/ssl-proxy/services/dq/internal/manager"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
)

type API struct {
	Manager    *manager.Manager
	Identities []config.Identity
}
type mutation struct {
	contract.Scope
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/processors", a.catalog)
	mux.HandleFunc("GET /v1/processor-versions/{processor_id}/{version}", a.version)
	mux.HandleFunc("POST /v1/processor-versions", a.register)
	mux.HandleFunc("POST /v1/runs", a.admit)
	mux.HandleFunc("GET /v1/runs/{run_id}", a.status)
	mux.HandleFunc("POST /v1/runs/{run_id}/cancel", a.cancel)
	mux.HandleFunc("POST /v1/runs/{run_id}/replay", a.replay)
	mux.HandleFunc("GET /v1/artifacts/{artifact_id}", a.artifact)
	mux.HandleFunc("POST /v1/artifacts/{artifact_id}/access", a.artifactAccess)
	return Auth(a.Identities, mux)
}

func (a *API) readScope(w http.ResponseWriter, r *http.Request) (contract.Scope, bool) {
	s := contract.Scope{InstallationID: r.URL.Query().Get("installation_id"), Namespace: r.URL.Query().Get("namespace")}
	if !contract.ValidSegment(s.InstallationID) || !contract.ValidSegment(s.Namespace) {
		writeError(w, 400, "invalid_scope")
		return s, false
	}
	if !principal(r).Can("viewer") || !principal(r).Allows(s.InstallationID, s.Namespace, "") {
		writeError(w, 403, "forbidden")
		return s, false
	}
	return s, true
}
func require(w http.ResponseWriter, r *http.Request, role string, s contract.Scope, scope string) bool {
	if !contract.ValidSegment(s.InstallationID) || !contract.ValidSegment(s.Namespace) {
		writeError(w, 400, "invalid_scope")
		return false
	}
	if !principal(r).Can(role) || !principal(r).Allows(s.InstallationID, s.Namespace, scope) {
		writeError(w, 403, "forbidden")
		return false
	}
	return true
}

func decode(w http.ResponseWriter, r *http.Request, out any) bool {
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		writeError(w, http.StatusUnsupportedMediaType, "json_required")
		return false
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, contract.MaxRecordBytes))
	if err != nil {
		writeError(w, 413, "control_record_too_large")
		return false
	}
	canonical, err := manager.CanonicalJSON(data)
	if err != nil {
		writeError(w, 400, "invalid_json")
		return false
	}
	d := json.NewDecoder(strings.NewReader(string(canonical)))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		writeError(w, 400, "invalid_request")
		return false
	}
	return true
}

func (a *API) catalog(w http.ResponseWriter, r *http.Request) {
	s, ok := a.readScope(w, r)
	if !ok {
		return
	}
	var catalog struct {
		Items []contract.Manifest `json:"items"`
	}
	if err := a.Manager.Control.Do(r.Context(), http.MethodGet, "/processors", control.Query(s.InstallationID, s.Namespace), nil, &catalog); err != nil {
		failure(w, err)
		return
	}
	if catalog.Items == nil {
		catalog.Items = []contract.Manifest{}
	}
	for _, manifest := range catalog.Items {
		if manifest.InstallationID != s.InstallationID || manifest.Namespace != s.Namespace || manager.ValidateManifest(manifest, a.Manager.RemoteOrigin) != nil {
			writeError(w, 502, "invalid_coordinator_response")
			return
		}
	}
	writeJSON(w, 200, catalog)
}
func (a *API) version(w http.ResponseWriter, r *http.Request) {
	s, ok := a.readScope(w, r)
	if !ok {
		return
	}
	manifest, err := a.Manager.Manifest(r.Context(), s, r.PathValue("processor_id"), r.PathValue("version"))
	if err != nil {
		failure(w, err)
		return
	}
	writeJSON(w, 200, manifest)
}
func (a *API) register(w http.ResponseWriter, r *http.Request) {
	var manifest contract.Manifest
	if !decode(w, r, &manifest) {
		return
	}
	if !require(w, r, "processor_owner", contract.Scope{InstallationID: manifest.InstallationID, Namespace: manifest.Namespace}, "") {
		return
	}
	if err := a.Manager.Register(r.Context(), manifest); err != nil {
		failure(w, err)
		return
	}
	writeJSON(w, 201, manifest)
}
func (a *API) admit(w http.ResponseWriter, r *http.Request) {
	var req contract.AdmitRequest
	if !decode(w, r, &req) {
		return
	}
	scope := manager.ParameterScope(req.Parameters)
	if scope == "" {
		writeError(w, 400, "authorized_scope_required")
		return
	}
	if !require(w, r, "operator", contract.Scope{InstallationID: req.InstallationID, Namespace: req.Namespace}, scope) {
		return
	}
	if req.Traceparent == "" {
		req.Traceparent = r.Header.Get("traceparent")
	}
	if req.Tracestate == "" {
		req.Tracestate = r.Header.Get("tracestate")
	}
	if len(req.Traceparent) > 128 || len(req.Tracestate) > 512 {
		writeError(w, 400, "invalid_trace_context")
		return
	}
	run, err := a.Manager.Admit(r.Context(), req)
	if err != nil {
		failure(w, err)
		return
	}
	publicRun(&run)
	writeJSON(w, 202, run)
}
func (a *API) getRun(w http.ResponseWriter, r *http.Request, s contract.Scope, id string) (contract.Run, bool) {
	var run contract.Run
	if !contract.ValidSegment(id) {
		writeError(w, 400, "invalid_run_id")
		return run, false
	}
	if err := a.Manager.Control.Do(r.Context(), http.MethodGet, "/runs/"+url.PathEscape(id), control.Query(s.InstallationID, s.Namespace), nil, &run); err != nil {
		failure(w, err)
		return run, false
	}
	if run.InstallationID != s.InstallationID || run.Namespace != s.Namespace || run.RunID != id {
		writeError(w, 502, "invalid_coordinator_response")
		return run, false
	}
	if scope := manager.ParameterScope(run.Parameters); scope == "" || !principal(r).Allows(s.InstallationID, s.Namespace, scope) {
		writeError(w, 403, "forbidden")
		return run, false
	}
	return run, true
}
func (a *API) status(w http.ResponseWriter, r *http.Request) {
	s, ok := a.readScope(w, r)
	if !ok {
		return
	}
	run, ok := a.getRun(w, r, s, r.PathValue("run_id"))
	if !ok {
		return
	}
	publicRun(&run)
	writeJSON(w, 200, run)
}
func (a *API) cancel(w http.ResponseWriter, r *http.Request) {
	var body mutation
	if !decode(w, r, &body) || !require(w, r, "operator", body.Scope, "") {
		return
	}
	before, ok := a.getRun(w, r, body.Scope, r.PathValue("run_id"))
	if !ok {
		return
	}
	var after contract.Run
	if err := a.Manager.Control.Do(r.Context(), http.MethodPost, "/runs/"+url.PathEscape(before.RunID)+"/cancel", nil, body, &after); err != nil {
		failure(w, err)
		return
	}
	a.Manager.CancelWorker(r.Context(), before)
	publicRun(&after)
	writeJSON(w, 200, after)
}
func (a *API) replay(w http.ResponseWriter, r *http.Request) {
	var body mutation
	if !decode(w, r, &body) || !require(w, r, "operator", body.Scope, "") {
		return
	}
	if !a.Manager.BetaEnabled {
		writeError(w, 403, "beta_disabled")
		return
	}
	if body.IdempotencyKey == "" || len(body.IdempotencyKey) > 200 {
		writeError(w, 400, "new_idempotency_key_required")
		return
	}
	before, ok := a.getRun(w, r, body.Scope, r.PathValue("run_id"))
	if !ok {
		return
	}
	var run contract.Run
	if err := a.Manager.Control.Do(r.Context(), http.MethodPost, "/runs/"+url.PathEscape(before.RunID)+"/replay", nil, body, &run); err != nil {
		failure(w, err)
		return
	}
	publicRun(&run)
	writeJSON(w, 202, run)
}
func (a *API) artifact(w http.ResponseWriter, r *http.Request) {
	s, ok := a.readScope(w, r)
	if !ok {
		return
	}
	id := r.PathValue("artifact_id")
	if !contract.ValidSegment(id) {
		writeError(w, 400, "invalid_artifact_id")
		return
	}
	ref, ok := a.authorizedArtifact(w, r, s, id)
	if !ok {
		return
	}
	writeJSON(w, 200, ref)
}
func (a *API) authorizedArtifact(w http.ResponseWriter, r *http.Request, s contract.Scope, id string) (contract.ArtifactRef, bool) {
	var ref contract.ArtifactRef
	if err := a.Manager.Control.Do(r.Context(), http.MethodGet, "/artifacts/"+url.PathEscape(id), control.Query(s.InstallationID, s.Namespace), nil, &ref); err != nil {
		failure(w, err)
		return ref, false
	}
	if ref.InstallationID != s.InstallationID || ref.Namespace != s.Namespace || ref.ArtifactID != id || ref.Validate() != nil {
		writeError(w, 502, "invalid_coordinator_response")
		return ref, false
	}
	if ref.ProducerRunID == "" {
		writeError(w, 403, "artifact_scope_unavailable")
		return ref, false
	}
	if _, ok := a.getRun(w, r, s, ref.ProducerRunID); !ok {
		return ref, false
	}
	return ref, true
}
func (a *API) artifactAccess(w http.ResponseWriter, r *http.Request) {
	var body contract.Scope
	if !decode(w, r, &body) || !require(w, r, "viewer", body, "") {
		return
	}
	id := r.PathValue("artifact_id")
	if !contract.ValidSegment(id) {
		writeError(w, 400, "invalid_artifact_id")
		return
	}
	if _, ok := a.authorizedArtifact(w, r, body, id); !ok {
		return
	}
	var result json.RawMessage
	if err := a.Manager.Control.Do(r.Context(), http.MethodPost, "/artifacts/"+url.PathEscape(id)+"/access", nil, body, &result); err != nil {
		failure(w, err)
		return
	}
	writeJSON(w, 200, result)
}

func publicRun(run *contract.Run) {
	if run.CurrentAttempt != nil {
		attempt := *run.CurrentAttempt
		attempt.LeaseToken = ""
		run.CurrentAttempt = &attempt
	}
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func writeError(w http.ResponseWriter, status int, code string) {
	writeJSON(w, status, map[string]string{"error_code": code})
}
func failure(w http.ResponseWriter, err error) {
	var status *control.StatusError
	if errors.As(err, &status) {
		code := status.Status
		if code < 400 || code > 599 {
			code = 502
		}
		writeError(w, code, http.StatusText(code))
		return
	}
	if errors.Is(err, control.ErrUnavailable) {
		writeError(w, 503, "coordinator_unavailable")
		return
	}
	if errors.Is(err, control.ErrResponse) {
		writeError(w, 502, "invalid_coordinator_response")
		return
	}
	writeError(w, 400, "request_validation_failed")
}
