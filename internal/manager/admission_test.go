package manager

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/zlovtnik/ssl-proxy/services/dq/internal/control"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
)

type admissionFixture struct {
	t        *testing.T
	existing bool
	manifest contract.Manifest
	request  contract.AdmitRequest
	posts    int
}

func (f *admissionFixture) Do(_ context.Context, method, path string, query url.Values, body, out any) error {
	if method == http.MethodGet && path == "/invocations/original-key" {
		if query.Get("installation_id") != "test" || query.Get("namespace") != "default" {
			f.t.Fatal("invocation lookup is not scoped")
		}
		if !f.existing {
			return &control.StatusError{Status: http.StatusNotFound}
		}
		return jsonCopy(contract.Run{RunID: "original", ProcessorVersion: "1.0.0"}, out)
	}
	if method == http.MethodGet && path == "/processors" {
		if f.existing {
			f.t.Fatal("retry consulted mutable default version")
		}
		return jsonCopy(struct {
			Items []contract.Manifest `json:"items"`
		}{[]contract.Manifest{f.manifest}}, out)
	}
	if method == http.MethodPost && path == "/runs" {
		f.request = body.(contract.AdmitRequest)
		f.posts++
		if f.existing && string(f.request.Parameters) != `{"authorized_scope":"floor-a","count":2}` {
			return &control.StatusError{Status: http.StatusConflict}
		}
		return jsonCopy(contract.Run{RunID: "original", ProcessorVersion: "1.0.0"}, out)
	}
	return errors.New("unexpected admission route")
}

func TestOmittedVersionRetryPreservesOriginalRequestAfterDefaultChanges(t *testing.T) {
	f := &admissionFixture{t: t, manifest: fixtureManifest(t)}
	m := New(f, contract.Scope{InstallationID: "test", Namespace: "default"}, nil, true, "", 8, time.Second)
	req := contract.AdmitRequest{InstallationID: "test", Namespace: "default", ProcessorID: f.manifest.ProcessorID, Operation: "calculate", IdempotencyKey: "original-key", Parameters: json.RawMessage(`{ "count":2, "authorized_scope":"floor-a" }`)}
	if _, err := m.Admit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if f.request.ProcessorVersion != "" {
		t.Fatal("admission replaced original omitted version")
	}
	f.existing = true
	f.manifest.Version = "2.0.0"
	f.manifest.ParameterSchema = json.RawMessage(`{"type":"object","properties":{"count":{"maximum":1}}}`)
	run, err := m.Admit(context.Background(), req)
	if err != nil || run.ProcessorVersion != "1.0.0" {
		t.Fatalf("retry lost original pinned run: %+v %v", run, err)
	}
	if f.request.ProcessorVersion != "" || f.posts != 2 {
		t.Fatal("retry changed invocation identity")
	}
	req.Parameters = json.RawMessage(`{"authorized_scope":"floor-a","count":3}`)
	_, err = m.Admit(context.Background(), req)
	var conflict *control.StatusError
	if !errors.As(err, &conflict) || conflict.Status != http.StatusConflict {
		t.Fatalf("changed retry did not reach authoritative fingerprint validation: %v", err)
	}
}
