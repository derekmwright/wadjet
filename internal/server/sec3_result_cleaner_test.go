package server

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/derekmwright/wadjet/internal/coordinator"
)

// A result cleaner with no object store is not a cleaner, and the ops
// handlers' own "result cleaner not available" branch is what says so.
//
// `Coordinator.Cleaner` memoises on first call, and the two ops handlers call
// it as `Cleaner(nil, "")` — they expect the store the startup path already
// registered (`cmd/wadjet/main.go` does that at boot). On a coordinator where
// that registration never happened they got back a cleaner holding a NIL
// store, and `CleanStale` / `CleanQuery` nil-deref on their first
// `rc.store.List`: net/http answers that by dropping the connection, so the
// caller saw an EOF rather than a status. The 503 branch beside them was dead
// code, because `Cleaner` never returns nil.
func TestOpsResultEndpointsRefuseWithNoStoreInsteadOfPanicking(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	// No catalog, no NATS: this coordinator exists only to answer Cleaner().
	coord := coordinator.New(coordinator.Config{}, nil, nil, nil, logger)
	ops := NewOpsAPI(coord)

	deleteReq := httptest.NewRequest(http.MethodDelete, "/v1/results/q1", nil)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("queryID", "q1")
	deleteReq = deleteReq.WithContext(context.WithValue(deleteReq.Context(), chi.RouteCtxKey, rctx))

	for _, tc := range []struct {
		name    string
		request *http.Request
		call    func(http.ResponseWriter, *http.Request)
	}{
		{"cleanup", httptest.NewRequest(http.MethodPost, "/v1/results/cleanup",
			strings.NewReader("{}")), ops.handleCleanupResults},
		{"delete", deleteReq, ops.handleDeleteResults},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			tc.call(w, tc.request)
			if w.Code != http.StatusServiceUnavailable {
				t.Errorf("status %d; want 503 with no store configured (body %s)",
					w.Code, w.Body.String())
			}
		})
	}
}
