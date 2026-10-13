package control

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
)

func TestScopedActorAndSanitizedErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer service-secret" || r.Header.Get("X-DQ-Actor") != "hashed" || r.Header.Get("X-DQ-Scopes") != "floor-a" || r.Header.Get("X-DQ-Roles") != "viewer" {
			t.Error("missing authenticated delegated identity")
		}
		w.WriteHeader(403)
		_, _ = w.Write([]byte("sensitive upstream SQL password"))
	}))
	defer server.Close()
	client := New(server.URL, "service-secret")
	ctx := WithActor(context.Background(), Actor{ID: "hashed", Scopes: []string{"floor-a"}, Roles: []string{"viewer"}})
	err := client.Do(ctx, "GET", "/runs/id", Query("test", "ns"), nil, nil)
	var status *StatusError
	if !errors.As(err, &status) || status.Status != 403 || strings.Contains(err.Error(), "password") {
		t.Fatal(err)
	}
}
func TestRejectRedirectAndUnboundedResponse(t *testing.T) {
	targetCalls := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetCalls++; w.WriteHeader(200) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "redirect") {
			http.Redirect(w, r, target.URL, 302)
			return
		}
		_, _ = w.Write([]byte(strings.Repeat("a", MaxRecordBytes+1)))
	}))
	defer server.Close()
	client := New(server.URL, "secret")
	if client.Do(context.Background(), "GET", "/redirect", nil, nil, nil) == nil || targetCalls != 0 {
		t.Fatal("service credential redirect followed")
	}
	var out json.RawMessage
	if !errors.Is(client.Do(context.Background(), "GET", "/oversized", nil, nil, &out), ErrResponse) {
		t.Fatal("unbounded response accepted")
	}
}

func TestServiceContextDropsDelegatedIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, header := range []string{"X-DQ-Actor", "X-DQ-Roles", "X-DQ-Scopes"} {
			if r.Header.Get(header) != "" {
				t.Errorf("service request retained delegated %s", header)
			}
		}
		if r.Header.Get("Authorization") != "Bearer service-secret" {
			t.Error("missing service authentication")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	ctx := WithActor(context.Background(), Actor{ID: "user", Roles: []string{"viewer"}, Scopes: []string{"floor-a"}})
	if err := New(server.URL, "service-secret").Do(ServiceContext(ctx), http.MethodGet, "/dispatches", nil, nil, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEscapePathKeepsIDsWithinOneRouteSegment(t *testing.T) {
	got, err := escapePath("/runs/run-id%20one/cancel")
	if err != nil || got != "/runs/run-id%20one/cancel" {
		t.Fatalf("escaped path = %q, %v", got, err)
	}
	for _, path := range []string{"/runs/a%2Fb", "/runs/../cancel", "/runs/a?scope=other", "//runs/id"} {
		if _, err := escapePath(path); err == nil {
			t.Errorf("unsafe path accepted: %q", path)
		}
	}
}

func TestReceiptAllowanceDoesNotIncreaseEnvelopeOrOtherRequestLimits(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client := New(server.URL, "service-secret")
	envelope := contract.Envelope{SchemaVersion: 1, EventID: "event", EventType: "invoke", InstallationID: "i", Namespace: "n", RunID: "run", OccurredAt: time.Now().UTC(), Artifacts: []contract.ArtifactRef{}, Summary: json.RawMessage(`{"padding":"` + strings.Repeat("a", MaxRecordBytes) + `"}`)}
	receipt := contract.Receipt{Envelope: &envelope}
	if client.Do(context.Background(), http.MethodPost, "/receipts", nil, receipt, nil) == nil {
		t.Fatal("receipt allowance admitted an oversized enclosed envelope")
	}
	receipt.Envelope = nil
	receipt.QuarantineReason = strings.Repeat("a", MaxRecordBytes)
	if client.Do(context.Background(), http.MethodPost, "/runs", nil, receipt, nil) == nil {
		t.Fatal("ordinary control request inherited receipt allowance")
	}
	receipt.QuarantineReason = strings.Repeat("a", MaxReceiptBytes)
	if client.Do(context.Background(), http.MethodPost, "/receipts", nil, receipt, nil) == nil {
		t.Fatal("receipt metadata exceeded its bounded allowance")
	}
	if calls != 0 {
		t.Fatal("invalid bodies reached the coordinator")
	}
}

func TestDispatchBatchResponseHasSeparateBoundedHTTPBudget(t *testing.T) {
	scope := contract.Scope{InstallationID: "test", Namespace: "ns"}
	parameters := json.RawMessage(`{"scope":"floor-a"}`)
	schema := json.RawMessage(`{"type":"object","description":"` + strings.Repeat("a", 1400) + `","properties":{"scope":{"type":"string"}}}`)
	manifest := contract.Manifest{InstallationID: scope.InstallationID, Namespace: scope.Namespace, ProcessorID: "atheros.dedup", Version: "0.1.0", OwnerTeam: "atheros", ImplementationDigest: "sha256:" + strings.Repeat("a", 64), WorkerService: "worker-v1", ProtocolVersion: contract.ProtocolVersion, ParameterSchema: schema, ParameterSchemaSHA256: contract.SHA256(schema), Resources: contract.ResourceLimits{TimeoutSeconds: 45, Concurrency: 4, MaxInputBytes: 32 << 20, MaxOutputBytes: 1 << 20}}
	ref := contract.ArtifactRef{ArtifactID: "source", InstallationID: scope.InstallationID, Namespace: scope.Namespace, URI: "s3://dq-artifacts/test/ns/runs/run/source.json", VersionID: "version-1", ETag: "etag", SHA256: strings.Repeat("b", 64), SchemaID: "dq.atheros.dedup.snapshot.v1", MediaType: "application/json", SizeBytes: 10, ProducerRunID: "run"}
	batch := struct {
		Items []contract.Dispatch `json:"items"`
	}{}
	for range 32 {
		batch.Items = append(batch.Items, contract.Dispatch{Run: contract.Run{InstallationID: scope.InstallationID, Namespace: scope.Namespace, RunID: "run", ProcessorID: manifest.ProcessorID, ProcessorVersion: manifest.Version, ImplementationDigest: manifest.ImplementationDigest, Parameters: parameters, ParametersSHA256: contract.SHA256(parameters), InputRefs: []contract.ArtifactRef{ref}, OutputRefs: []contract.ArtifactRef{}, SourceRevisionDigest: strings.Repeat("c", 64), DeadlineAt: time.Now().Add(time.Minute)}, Manifest: manifest})
	}
	data, err := json.Marshal(batch)
	if err != nil || len(data) <= MaxRecordBytes || len(data) > MaxDispatchResponseBytes {
		t.Fatalf("realistic dispatch batch fixture: %d bytes, %v", len(data), err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("oversized") != "" {
			_, _ = w.Write([]byte(strings.Repeat("a", MaxDispatchResponseBytes+1)))
			return
		}
		_, _ = w.Write(data)
	}))
	defer server.Close()
	client := New(server.URL, "service-secret")
	var received struct {
		Items []contract.Dispatch `json:"items"`
	}
	if err := client.Do(context.Background(), http.MethodGet, "/dispatches", Query("test", "ns"), nil, &received); err != nil || len(received.Items) != 32 || len(received.Items[0].Run.InputRefs) != 1 {
		t.Fatalf("default dispatch batch failed: %d items, %v", len(received.Items), err)
	}
	if !errors.Is(client.Do(context.Background(), http.MethodGet, "/processors", nil, nil, &received), ErrResponse) {
		t.Fatal("ordinary response inherited dispatch allowance")
	}
	query := Query("test", "ns")
	query.Set("oversized", "true")
	if !errors.Is(client.Do(context.Background(), http.MethodGet, "/dispatches", query, nil, &received), ErrResponse) {
		t.Fatal("unbounded dispatch response accepted")
	}
}
