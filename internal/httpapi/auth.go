package httpapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"strings"

	"github.com/zlovtnik/ssl-proxy/services/dq/internal/config"
	"github.com/zlovtnik/ssl-proxy/services/dq/internal/control"
	"github.com/zlovtnik/ssl-proxy/services/dq/pkg/telemetry"
)

type principalKey struct{}
type Principal struct{ Identity config.Identity }

func (p Principal) Can(role string) bool {
	for _, r := range p.Identity.Roles {
		if r == "admin" || r == role || role == "viewer" && r == "operator" {
			return true
		}
	}
	return false
}
func (p Principal) Allows(installation, namespace, scope string) bool {
	if installation != p.Identity.InstallationID {
		return false
	}
	namespaceAllowed := false
	for _, n := range p.Identity.Namespaces {
		if n == namespace {
			namespaceAllowed = true
			break
		}
	}
	if !namespaceAllowed {
		return false
	}
	if scope == "" {
		return true
	}
	for _, s := range p.Identity.Scopes {
		if s == "*" || s == scope {
			return true
		}
	}
	return false
}

func Auth(identities []config.Identity, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := telemetry.Extract(r.Context(), r.Header.Get("traceparent"), r.Header.Get("tracestate"))
		ctx, span := telemetry.Start(ctx, "dq.http")
		defer span.End()
		r = r.WithContext(ctx)
		header := r.Header.Get("Authorization")
		if !strings.HasPrefix(header, "Bearer ") || len(header) > 4096 {
			writeError(w, http.StatusUnauthorized, "authentication_required")
			return
		}
		token := strings.TrimPrefix(header, "Bearer ")
		tokenHash := sha256.Sum256([]byte(token))
		var found *config.Identity
		for _, identity := range identities {
			expected := sha256.Sum256([]byte(identity.Token))
			if subtle.ConstantTimeCompare(tokenHash[:], expected[:]) == 1 {
				i := identity
				found = &i
			}
		}
		if found == nil {
			writeError(w, http.StatusUnauthorized, "authentication_required")
			return
		}
		principal := Principal{Identity: *found}
		ctx = context.WithValue(r.Context(), principalKey{}, principal)
		actorHash := sha256.Sum256([]byte(found.ID))
		ctx = control.WithActor(ctx, control.Actor{ID: hex.EncodeToString(actorHash[:]), Scopes: found.Scopes, Roles: found.Roles})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func principal(r *http.Request) Principal {
	p, _ := r.Context().Value(principalKey{}).(Principal)
	return p
}
