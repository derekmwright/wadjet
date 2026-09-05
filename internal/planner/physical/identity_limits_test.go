package physical

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/config"
)

// tightestLimits is where the deployment's cost guard and the calling
// identity's meet, and the whole claim of the `query_limit` obligation rests
// on it narrowing and never widening (ADR-0033). It had no direct test: the
// round-1 review had to probe it by hand to confirm the property the arc's
// docs, ADR and release note all state.
func TestTightestLimitsNarrowsAndNeverWidens(t *testing.T) {
	dep := &config.QueryLimits{MaxScanRows: 50, MaxScanBytes: 100, MaxScanFiles: 2}
	wide := &config.QueryLimits{MaxScanRows: 1e9, MaxScanBytes: 1 << 40, MaxScanFiles: 9999}
	narrow := &config.QueryLimits{MaxScanRows: 5, MaxScanBytes: 7, MaxScanFiles: 1}

	for _, tc := range []struct {
		name                 string
		configured, identity *config.QueryLimits
		rows, bytes          int64
		files                int
		wantNil              bool
	}{
		{name: "neither", wantNil: true},
		{name: "configured_only", configured: dep, rows: 50, bytes: 100, files: 2},
		// A deployment with NO guard still enforces the obligation: that is
		// what makes a policy ceiling meaningful on an unlimited deployment.
		{name: "identity_only", identity: narrow, rows: 5, bytes: 7, files: 1},
		// The whole point: an obligation naming a bigger number does not
		// widen the deployment's guard.
		{name: "identity_wider", configured: dep, identity: wide, rows: 50, bytes: 100, files: 2},
		{name: "identity_tighter", configured: dep, identity: narrow, rows: 5, bytes: 7, files: 1},
		// Per KNOB, not per struct: the tighter of each is taken separately.
		{name: "per_knob", configured: &config.QueryLimits{MaxScanRows: 5, MaxScanBytes: 1 << 30},
			identity: &config.QueryLimits{MaxScanRows: 1e9, MaxScanBytes: 8, MaxScanFiles: 3},
			rows:     5, bytes: 8, files: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var beforeC, beforeI config.QueryLimits
			if tc.configured != nil {
				beforeC = *tc.configured
			}
			if tc.identity != nil {
				beforeI = *tc.identity
			}
			got := tightestLimits(tc.configured, tc.identity)
			if tc.wantNil {
				if got != nil {
					t.Fatalf("tightestLimits(nil, nil) = %+v, want nil — no guard means no guard", got)
				}
				return
			}
			if got == nil {
				t.Fatal("tightestLimits returned nil where a ceiling was set")
			}
			if got.MaxScanRows != tc.rows || got.MaxScanBytes != tc.bytes || got.MaxScanFiles != tc.files {
				t.Errorf("rows=%d bytes=%d files=%d, want %d/%d/%d",
					got.MaxScanRows, got.MaxScanBytes, got.MaxScanFiles, tc.rows, tc.bytes, tc.files)
			}
			// Neither input is mutated: the configured limits belong to the
			// Planner and outlive the statement, and the identity's are
			// cached on a policy decision for the rest of the query.
			if tc.configured != nil && *tc.configured != beforeC {
				t.Error("tightestLimits mutated the CONFIGURED limits; they belong to the " +
					"Planner and would carry into the next statement")
			}
			if tc.identity != nil && *tc.identity != beforeI {
				t.Error("tightestLimits mutated the IDENTITY's limits; they are cached on a " +
					"policy decision and would carry into the next relation")
			}
		})
	}
}

// TestIdentityQueryLimitsRideTheContext is the carriage the merge depends on:
// auth.EnforcePlanPolicies puts the ceiling on the context after every planner
// has been built, and enforceQueryLimits reads it back where the guard runs.
func TestIdentityQueryLimitsRideTheContext(t *testing.T) {
	if got := IdentityQueryLimitsFromContext(context.Background()); got != nil {
		t.Errorf("a bare context carries %+v, want nil", got)
	}
	if got := IdentityQueryLimitsFromContext(nil); got != nil { //nolint:staticcheck // nil is a caller shape
		t.Errorf("a nil context carries %+v, want nil", got)
	}
	// A nil ceiling does not put a typed nil on the context, which would make
	// the merge see a guard of all zeroes rather than no guard at all.
	if ctx := WithIdentityQueryLimits(context.Background(), nil); IdentityQueryLimitsFromContext(ctx) != nil {
		t.Error("WithIdentityQueryLimits(ctx, nil) put something on the context")
	}
	lim := &config.QueryLimits{MaxScanRows: 3}
	ctx := WithIdentityQueryLimits(context.Background(), lim)
	if got := IdentityQueryLimitsFromContext(ctx); got != lim {
		t.Errorf("the context carries %+v, want the ceiling that was put on it", got)
	}
}
