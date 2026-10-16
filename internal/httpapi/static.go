package httpapi

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/zlovtnik/ssl-proxy/services/dq/internal/control"
	"github.com/zlovtnik/ssl-proxy/services/dq/internal/manager"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/contract"
)

func Shell(directory, remoteOrigin string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		ancestors := "'none'"
		embedded := strings.HasPrefix(r.URL.Path, "/dq-ui/")
		if embedded {
			ancestors = "'self'"
			w.Header().Set("Access-Control-Allow-Origin", "*")
		}
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; frame-src "+frameSource(remoteOrigin)+"; object-src 'none'; base-uri 'none'; form-action 'none'; frame-ancestors "+ancestors)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		clean := path.Clean("/" + r.URL.Path)
		if embedded {
			clean = "/embedded/" + strings.TrimPrefix(clean, "/dq-ui/")
		}
		if clean == "/" || strings.HasPrefix(clean, "/dq/") || clean == "/dq" {
			clean = "/index.html"
		}
		name := filepath.Join(directory, filepath.FromSlash(strings.TrimPrefix(clean, "/")))
		file, err := os.Open(name)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		defer file.Close()
		stat, err := file.Stat()
		if err != nil || !stat.Mode().IsRegular() || stat.Size() > 8<<20 {
			http.NotFound(w, r)
			return
		}
		if strings.HasPrefix(clean, "/assets/") {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			w.Header().Set("Cache-Control", "no-store")
		}
		http.ServeContent(w, r, stat.Name(), stat.ModTime(), file)
	})
}
func frameSource(origin string) string {
	if origin == "" {
		return "'none'"
	}
	return origin
}

var scriptContent = regexp.MustCompile(`(?s)<script\b[^>]*>(.*?)</script>`)
var styleContent = regexp.MustCompile(`(?s)<style\b[^>]*>(.*?)</style>`)

// Remotes serves only a registered immutable remote's entry and module. It does
// not fetch arbitrary URLs. Browser isolation also applies when the entry is
// opened directly, through CSP sandbox rather than relying on shell markup.
func Remotes(directory string, m *manager.Manager) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		segments := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
		if len(segments) != 4 || !contract.ValidSegment(segments[0]) || !contract.ValidSegment(segments[1]) || !contract.ValidSHA256(segments[2]) || (segments[3] != "index.html" && segments[3] != "remote.js") {
			http.NotFound(w, r)
			return
		}
		ctx := control.ServiceContext(r.Context())
		manifest, err := m.Manifest(ctx, m.Scope, segments[0], segments[1])
		if err != nil || manifest.UIRemote == nil || manifest.UIRemote.BundleDigest != segments[2] || manifest.UIRemote.ImmutableURL != m.RemoteOrigin+"/"+strings.Join(segments[:3], "/")+"/index.html" {
			http.NotFound(w, r)
			return
		}
		base := filepath.Join(directory, segments[0], segments[1], segments[2])
		module, err := boundedFile(filepath.Join(base, "remote.js"), 4<<20)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		digest := sha256.Sum256(module)
		if hex.EncodeToString(digest[:]) != segments[2] {
			writeError(w, 503, "remote_integrity_failure")
			return
		}
		data := module
		if segments[3] == "index.html" {
			data, err = boundedFile(filepath.Join(base, "index.html"), contract.MaxRecordBytes)
			if err != nil {
				http.NotFound(w, r)
				return
			}
		}
		scripts := "script-src " + m.RemoteOrigin
		if segments[3] == "index.html" {
			for _, match := range scriptContent.FindAllSubmatch(data, -1) {
				sum := sha256.Sum256(match[1])
				scripts += " 'sha256-" + base64.StdEncoding.EncodeToString(sum[:]) + "'"
			}
		}
		styles := "style-src 'none'"
		if segments[3] == "index.html" {
			for _, match := range styleContent.FindAllSubmatch(data, -1) {
				if styles == "style-src 'none'" {
					styles = "style-src"
				}
				sum := sha256.Sum256(match[1])
				styles += " 'sha256-" + base64.StdEncoding.EncodeToString(sum[:]) + "'"
			}
		}
		w.Header().Set("Content-Security-Policy", "default-src 'none'; "+scripts+"; "+styles+"; connect-src 'none'; img-src data:; object-src 'none'; base-uri 'none'; form-action 'none'; sandbox allow-scripts")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Cross-Origin-Resource-Policy", "cross-origin")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		if segments[3] == "index.html" {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
		} else {
			w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		}
		http.ServeContent(w, r, segments[3], time.Time{}, bytes.NewReader(data))
	})
}

func boundedFile(name string, limit int64) ([]byte, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil || !stat.Mode().IsRegular() || stat.Size() > limit {
		return nil, os.ErrInvalid
	}
	return io.ReadAll(io.LimitReader(f, limit+1))
}
