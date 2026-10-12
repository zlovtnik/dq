package control

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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
