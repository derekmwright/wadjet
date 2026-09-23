// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The ROUTE, not only the rows: on a stage-DAG coordinator the refusal moves
// the plan to the coordinator-local pipeline (the routing counter moves by
// one), and with the local fast path enabled — the default for a small scan —
// the query never reaches the stage planner and the counter stays put. Both
// answer PostgreSQL's rows.
func TestArcDCAMergedResidualRoutesLocalOnTheDAGAndStaysOnTheFastPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: embedded cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	dag := tmdCoordinator(t, ctx, infra)
	fast := tmdCoordinator(t, ctx, infra, func(c *Config) { c.LocalFastPathBytes = 64 << 20 })
	for _, tc := range []dcRound3Case{
		{name: "wrapped/derived/EXISTS",
			sql:  "SELECT o.id AS a FROM dc_out o WHERE EXISTS (SELECT 1 FROM (SELECT k,total FROM (SELECT k,amt AS total FROM dc_in) x) b WHERE b.k=o.id AND total>o.total) ORDER BY a",
			want: "rows=2 1 | 2"},
		{name: "probe_rename/derived/EXISTS",
			sql:  "SELECT o.id AS a FROM (SELECT id,total AS amt FROM dc_out) o WHERE EXISTS (SELECT 1 FROM (SELECT k,amt FROM dc_in) b WHERE b.k=o.id AND amt>o.amt) ORDER BY a",
			want: "rows=2 1 | 2"},
	} {
		before := dag.ResidualSidesLocalRoutes()
		if got := r1DAG(ctx, dag, tc.sql); got != tc.want {
			t.Errorf("dag %s\n  got  %s\n  want %s", tc.name, got, tc.want)
		}
		if moved := dag.ResidualSidesLocalRoutes() - before; moved != 1 {
			t.Errorf("dag %s: ResidualSidesLocalRoutes moved by %d, want 1", tc.name, moved)
		}
		fb := fast.ResidualSidesLocalRoutes()
		if got := r1DAG(ctx, fast, tc.sql); got != tc.want {
			t.Errorf("fastpath %s\n  got  %s\n  want %s", tc.name, got, tc.want)
		}
		if moved := fast.ResidualSidesLocalRoutes() - fb; moved != 0 {
			t.Errorf("fastpath %s: ResidualSidesLocalRoutes moved by %d, want 0 (the fast path runs single-process)", tc.name, moved)
		}
	}
}

// The shadowing-WITH refusal carries its SQLSTATE through every door that
// runs it: the embedded single-process query and the stage-DAG coordinator,
// which routes the correlated subquery to its local pipeline.
func TestArcDCAShadowingBodyWithRefusalIs0A000(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: embedded cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	const sql = "WITH d AS (SELECT k,amt FROM dc_in) SELECT o.id AS a FROM dc_out o " +
		"WHERE EXISTS (WITH d AS (SELECT k,amt AS total FROM dc_in) SELECT 1 FROM d b " +
		"WHERE b.k=o.id AND total>o.total) ORDER BY a"
	single := tmdStandalone(t, ctx)
	if _, err := single.Query(ctx, sql); sqlerr.StateOf(err) != "0A000" {
		t.Errorf("single: SQLSTATE %q (%v), want 0A000", sqlerr.StateOf(err), err)
	}
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	dag := tmdCoordinator(t, ctx, infra)
	_, err := dag.ExecuteSQL(ctx, sql)
	if sqlerr.StateOf(err) != "0A000" {
		t.Errorf("dag: SQLSTATE %q (%v), want 0A000", sqlerr.StateOf(err), err)
	}
}
