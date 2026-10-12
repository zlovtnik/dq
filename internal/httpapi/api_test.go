package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/zlovtnik/ssl-proxy/services/dq/internal/config"
	"github.com/zlovtnik/ssl-proxy/services/dq/internal/control"
	"github.com/zlovtnik/ssl-proxy/services/dq/internal/manager"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
)

type apiFixture struct {
	calls       int
	accessCalls int
	run         contract.Run
	artifact    contract.ArtifactRef
	fail        error
}

func (f *apiFixture) Do(_ context.Context, method, path string, _ url.Values, body, out any) error {
	f.calls++
	if f.fail != nil {
		return f.fail
	}
	if method == http.MethodGet && strings.HasPrefix(path, "/artifacts/") {
		b, _ := json.Marshal(f.artifact)
		return json.Unmarshal(b, out)
	}
	if method == http.MethodPost && strings.HasSuffix(path, "/access") {
		f.accessCalls++
		return json.Unmarshal([]byte(`{"url":"https://example.test/object"}`), out)
	}
	if method == http.MethodGet && strings.HasPrefix(path, "/runs/") {
		b, _ := json.Marshal(f.run)
		return json.Unmarshal(b, out)
	}
	return errors.New("unexpected route")
}
func apiHandler(store *apiFixture, role string, beta bool) http.Handler {
	identity := config.Identity{ID: "caller", Token: strings.Repeat("t", 32), Roles: []string{role}, InstallationID: "test", Namespaces: []string{"ns"}, Scopes: []string{"floor-a"}}
	m := manager.New(store, contract.Scope{InstallationID: "test", Namespace: "ns"}, nil, beta, "", 8, time.Second)
	return (&API{Manager: m, Identities: []config.Identity{identity}}).Handler()
}
func request(handler http.Handler, method, path, body, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}

func TestAuthNamespaceRoleAndScopeFailClosed(t *testing.T) {
	validToken := strings.Repeat("t", 32)
	for _, tc := range []struct {
		role, method, path, body, token string
		want                            int
	}{
		{"operator", "GET", "/v1/processors?installation_id=test&namespace=ns", "", "", 401},
		{"viewer", "POST", "/v1/runs", `{"installation_id":"test","namespace":"ns","processor_id":"p","operation":"run","parameters":{"authorized_scope":"floor-a"},"idempotency_key":"key"}`, validToken, 403},
		{"operator", "POST", "/v1/runs", `{"installation_id":"test","namespace":"ns","processor_id":"p","operation":"run","parameters":{"authorized_scope":"floor-b"},"idempotency_key":"key"}`, validToken, 403},
		{"admin", "GET", "/v1/processors?installation_id=other&namespace=ns", "", validToken, 403},
		{"admin", "GET", "/v1/processors?installation_id=test&namespace=other", "", validToken, 403},
	} {
		store := &apiFixture{}
		w := request(apiHandler(store, tc.role, true), tc.method, tc.path, tc.body, tc.token)
		if w.Code != tc.want || store.calls != 0 {
			t.Fatalf("%s %s: status=%d coordinator calls=%d", tc.method, tc.path, w.Code, store.calls)
		}
	}
}
func TestBetaAndRequestBounds(t *testing.T) {
	store := &apiFixture{}
	handler := apiHandler(store, "operator", false)
	body := `{"installation_id":"test","namespace":"ns","processor_id":"p","operation":"run","parameters":{"authorized_scope":"floor-a"},"idempotency_key":"key"}`
	w := request(handler, "POST", "/v1/runs", body, strings.Repeat("t", 32))
	if w.Code != 403 || store.calls != 0 {
		t.Fatal("disabled beta admitted a run")
	}
	w = request(handler, "POST", "/v1/runs", strings.Repeat("x", contract.MaxRecordBytes+1), strings.Repeat("t", 32))
	if w.Code != 413 {
		t.Fatal(w.Code)
	}
	w = request(handler, "POST", "/v1/runs", `{"installation_id":"test","installation_id":"other"}`, strings.Repeat("t", 32))
	if w.Code != 400 {
		t.Fatal("duplicate names accepted")
	}
}
func TestReadChecksRunScopeAndRedactsLeaseCredential(t *testing.T) {
	store := &apiFixture{run: contract.Run{InstallationID: "test", Namespace: "ns", RunID: "run-1", Parameters: json.RawMessage(`{"authorized_scope":"floor-b"}`), CurrentAttempt: &contract.Attempt{AttemptID: "attempt-1", LeaseToken: "do-not-expose"}}}
	handler := apiHandler(store, "viewer", false)
	w := request(handler, "GET", "/v1/runs/run-1?installation_id=test&namespace=ns", "", strings.Repeat("t", 32))
	if w.Code != 403 {
		t.Fatal("read leaked another scope")
	}
	store.run.Parameters = json.RawMessage(`{"authorized_scope":"floor-a"}`)
	w = request(handler, "GET", "/v1/runs/run-1?installation_id=test&namespace=ns", "", strings.Repeat("t", 32))
	if w.Code != 200 || strings.Contains(w.Body.String(), "do-not-expose") {
		t.Fatal(w.Body.String())
	}
	if store.run.CurrentAttempt.LeaseToken != "do-not-expose" {
		t.Fatal("public redaction modified authority data")
	}
}
func TestDependencyErrorsAreSanitized(t *testing.T) {
	store := &apiFixture{fail: control.ErrUnavailable}
	w := request(apiHandler(store, "viewer", false), "GET", "/v1/runs/run-1?installation_id=test&namespace=ns", "", strings.Repeat("t", 32))
	if w.Code != 503 || w.Body.String() != "{\"error_code\":\"coordinator_unavailable\"}\n" {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestArtifactEndpointsRequireProducerRunScope(t *testing.T) {
	token := strings.Repeat("t", 32)
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		store := &apiFixture{
			run:      contract.Run{InstallationID: "test", Namespace: "ns", RunID: "run-1", Parameters: json.RawMessage(`{"authorized_scope":"floor-b"}`)},
			artifact: contract.ArtifactRef{ArtifactID: "artifact-1", InstallationID: "test", Namespace: "ns", ProducerRunID: "run-1", URI: "s3://bucket/key", VersionID: "version", ETag: "etag", SHA256: strings.Repeat("a", 64), SchemaID: "schema", MediaType: "application/json", SizeBytes: 1},
		}
		path := "/v1/artifacts/artifact-1?installation_id=test&namespace=ns"
		body := ""
		if method == http.MethodPost {
			path = "/v1/artifacts/artifact-1/access"
			body = `{"installation_id":"test","namespace":"ns"}`
		}
		w := request(apiHandler(store, "viewer", false), method, path, body, token)
		if w.Code != http.StatusForbidden || store.calls != 2 || store.accessCalls != 0 {
			t.Fatalf("%s: status=%d calls=%d access calls=%d", method, w.Code, store.calls, store.accessCalls)
		}
	}
}

func TestArtifactWithoutProducerRunIsDenied(t *testing.T) {
	store := &apiFixture{artifact: contract.ArtifactRef{ArtifactID: "artifact-1", InstallationID: "test", Namespace: "ns", URI: "s3://bucket/key", VersionID: "version", ETag: "etag", SHA256: strings.Repeat("a", 64), SchemaID: "schema", MediaType: "application/json", SizeBytes: 1}}
	w := request(apiHandler(store, "viewer", false), http.MethodGet, "/v1/artifacts/artifact-1?installation_id=test&namespace=ns", "", strings.Repeat("t", 32))
	if w.Code != http.StatusForbidden || store.calls != 1 {
		t.Fatalf("status=%d calls=%d", w.Code, store.calls)
	}
}
