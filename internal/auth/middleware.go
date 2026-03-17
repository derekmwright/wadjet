package auth

import (
	"context"
	"log/slog"
	"net/http"
)

type contextKey struct{}
type rowFilterKey struct{}
type tableDecisionsKey struct{}

// IdentityFromContext extracts the authenticated identity from a request context.
// Returns nil if no identity is present (auth disabled or unauthenticated).
func IdentityFromContext(ctx context.Context) *Identity {
	id, _ := ctx.Value(contextKey{}).(*Identity)
	return id
}

// ContextWithIdentity returns a new context with the given identity.
func ContextWithIdentity(ctx context.Context, id *Identity) context.Context {
	return context.WithValue(ctx, contextKey{}, id)
}

// RowFilters is a map of table name → SQL predicate for row-level security.
type RowFilters map[string]string

// TableDecisions maps table names to their ABAC-evaluated access decisions.
type TableDecisions map[string]*TableDecision

// ContextWithTableDecisions returns a context carrying ABAC table decisions.
func ContextWithTableDecisions(ctx context.Context, decisions TableDecisions) context.Context {
	return context.WithValue(ctx, tableDecisionsKey{}, decisions)
}

// TableDecisionsFromContext extracts ABAC table decisions from context.
func TableDecisionsFromContext(ctx context.Context) TableDecisions {
	td, _ := ctx.Value(tableDecisionsKey{}).(TableDecisions)
	return td
}

// ContextWithRowFilters returns a context carrying row filter predicates.
func ContextWithRowFilters(ctx context.Context, filters RowFilters) context.Context {
	return context.WithValue(ctx, rowFilterKey{}, filters)
}

// RowFiltersFromContext extracts row filter predicates from context.
func RowFiltersFromContext(ctx context.Context) RowFilters {
	rf, _ := ctx.Value(rowFilterKey{}).(RowFilters)
	return rf
}

// Middleware returns HTTP middleware that authenticates requests.
// Health check endpoint is always allowed without auth.
func Middleware(authn *Authenticator, logger *slog.Logger) func(http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Health checks skip auth (load balancers, k8s probes)
			if r.URL.Path == "/v1/health" {
				next.ServeHTTP(w, r)
				return
			}

			id, err := authn.Authenticate(r)
			if err != nil {
				logger.Warn("authentication failed",
					"path", r.URL.Path,
					"remote", r.RemoteAddr,
					"error", err,
				)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"error":"unauthorized"}`))
				return
			}

			logger.Debug("authenticated request",
				"identity", id.String(),
				"path", r.URL.Path,
			)

			ctx := ContextWithIdentity(r.Context(), id)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// ProviderMiddleware returns HTTP middleware that reads the current Authenticator
// from a Provider on every request. This enables hot-reload: when auth config
// changes, the Provider is atomically updated and subsequent requests see the
// new config with zero downtime.
func ProviderMiddleware(provider *Provider, logger *slog.Logger) func(http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Health checks always pass
			if r.URL.Path == "/v1/health" {
				next.ServeHTTP(w, r)
				return
			}

			// Read current auth state — single atomic load
			if !provider.Enabled() {
				next.ServeHTTP(w, r)
				return
			}

			authn := provider.Authenticator()
			if authn == nil {
				next.ServeHTTP(w, r)
				return
			}

			id, err := authn.Authenticate(r)
			if err != nil {
				logger.Warn("authentication failed",
					"path", r.URL.Path,
					"remote", r.RemoteAddr,
					"error", err,
				)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				w.Write([]byte(`{"error":"unauthorized"}`))
				return
			}

			logger.Debug("authenticated request",
				"identity", id.String(),
				"path", r.URL.Path,
			)

			ctx := ContextWithIdentity(r.Context(), id)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
