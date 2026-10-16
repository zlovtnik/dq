package httpapi

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEmbeddedShellFramePolicy(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "embedded"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{"index.html", "embedded/embedded.html"} {
		if err := os.WriteFile(filepath.Join(dir, file), []byte("shell"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	handler := Shell(dir, "https://extensions.example.test")
	for _, tc := range []struct{ path, ancestors string }{{"/dq/processors", "'none'"}, {"/dq-ui/embedded.html", "'self'"}} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest("GET", tc.path, nil))
		if recorder.Code != 200 || !strings.Contains(recorder.Header().Get("Content-Security-Policy"), "frame-ancestors "+tc.ancestors) {
			t.Fatalf("unexpected response: %d %v", recorder.Code, recorder.Header())
		}
	}
}
