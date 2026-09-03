package coordinator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// qlConfig writes a serve config file and returns what the shipped extractor
// makes of it. The whole point of #803's gate is that the limits arrive the
// way a deployment's limits arrive — through config.Load and
// Config.EffectiveQueryLimits — not as a struct the test typed out.
func qlConfig(t *testing.T, yaml string) (*config.QueryLimits, map[string]*config.QueryLimits) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wadjet.yaml")
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.Load(path)
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}
	return cfg.EffectiveQueryLimits()
}

const qlYAML = `
query_limits:
  max_scan_bytes: 1024
auth:
  enabled: true
  api_keys:
    - key: "analyst-key"
      name: "analyst"
      role: analyst
    - key: "admin-key"
      name: "admin"
      role: admin
  roles:
    - name: analyst
      tables: ["*"]
      allow: [read]
      query_limits:
        max_scan_files: 1
    - name: admin
      tables: ["*"]
      allow: [read, write, admin]
      # no query_limits -> unlimited, overriding the global cap
`

// qlIdentity is a bare identity for a role name — enough for
// resolveQueryLimits, which reads only Role.
func qlIdentity(ctx context.Context, role string) context.Context {
	return auth.ContextWithIdentity(ctx, &auth.Identity{Name: role, Role: role, Method: "apikey"})
}

// TestConfiguredQueryLimitsRejectOnEveryCoordinatorArm is #803's gate.
//
// server.Config.QueryLimits / RoleLimits were populated only in tests, and the
// only planner that ever read them was the HTTP server's no-coordinator path.
// Every `wadjet serve` mode answers through the coordinator, so `query_limits:`
// bounded nothing on any protocol — an operator relying on max_scan_bytes to
// cap spend had no control at all.
//
// Both coordinator arms are asserted, because the coordinator builds a
// separate planner for each: the in-process fast path and the stage DAG.
func TestConfiguredQueryLimitsRejectOnEveryCoordinatorArm(t *testing.T) {
	ctx := context.Background()
	global, perRole := qlConfig(t, qlYAML)
	if global == nil || global.MaxScanBytes != 1024 {
		t.Fatalf("config extraction: global = %+v, want MaxScanBytes 1024", global)
	}
	if lim, ok := perRole["admin"]; !ok || lim != nil {
		t.Fatalf("config extraction: admin should map to nil (unlimited), got (%v, present=%v)", lim, ok)
	}
	if lim, ok := perRole["analyst"]; !ok || lim == nil || lim.MaxScanFiles != 1 {
		t.Fatalf("config extraction: analyst = %v (present=%v), want MaxScanFiles 1", lim, ok)
	}

	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)

	arms := []struct {
		name  string
		coord *Coordinator
	}{
		// LocalFastPathBytes 0 = every query takes the stage DAG.
		{"dag", tmdCoordinator(t, ctx, infra)},
		// A threshold above the fixture routes every query in-process.
		{"local-fastpath", tmdCoordinator(t, ctx, infra, func(c *Config) {
			c.LocalFastPathBytes = 1 << 30
		})},
	}

	sql := "SELECT COUNT(*) AS n FROM " + typematrix.Table

	for _, arm := range arms {
		t.Run(arm.name, func(t *testing.T) {
			arm.coord.SetQueryLimits(global, perRole)

			// Unauthenticated: the global cap applies and this scan exceeds it.
			_, err := arm.coord.ExecuteSQL(ctx, sql)
			if err == nil {
				t.Fatal("a query over the configured max_scan_bytes must be rejected; it was not")
			}
			if got := sqlerr.StateOf(err); got != physical.QueryLimitSQLState {
				t.Errorf("SQLSTATE = %q, want %q (%v)", got, physical.QueryLimitSQLState, err)
			}
			if !strings.Contains(err.Error(), "exceeding limit") {
				t.Errorf("rejection %q does not say what was exceeded", err)
			}

			// analyst: its own max_scan_files=1 replaces the global cap.
			_, err = arm.coord.ExecuteSQL(qlIdentity(ctx, "analyst"), sql)
			if err == nil {
				t.Fatal("analyst's per-role limit must be enforced; the query was not rejected")
			}
			if got := sqlerr.StateOf(err); got != physical.QueryLimitSQLState {
				t.Errorf("analyst SQLSTATE = %q, want %q (%v)", got, physical.QueryLimitSQLState, err)
			}

			// admin declares no limits, which OVERRIDES the global cap rather
			// than inheriting it — the same query must succeed. This is the
			// half a "reject everything" fix would fail.
			res, err := arm.coord.ExecuteSQL(qlIdentity(ctx, "admin"), sql)
			if err != nil {
				t.Fatalf("admin has no configured limits and must not be capped: %v", err)
			}
			rows, err := res.Rows()
			if err != nil {
				t.Fatalf("admin rows: %v", err)
			}
			if len(rows) != 1 {
				t.Fatalf("admin: expected 1 row, got %d", len(rows))
			}

			// And with no limits configured at all, nothing is capped.
			arm.coord.SetQueryLimits(nil, nil)
			if _, err := arm.coord.ExecuteSQL(ctx, sql); err != nil {
				t.Fatalf("no configured limits must mean unlimited: %v", err)
			}
		})
	}
}
