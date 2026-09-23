// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/testutil/tcpflagcases"
	"github.com/derekmwright/wadjet/internal/worker"
)

// The AST coverage census, including the round-7 recursive-CTE regressions.
// Refusals precede dispatch: every routing counter must remain unchanged.
func TestTCPFlagASTCoverage(t *testing.T) {
	if testing.Short() {
		t.Skip("-short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	t.Cleanup(cancel)

	prevFlag := scan.FlagDictPushdown.Set(true)
	t.Cleanup(func() { scan.FlagDictPushdown.Set(prevFlag) })

	single := tmdStandalone(t, ctx)
	spilled := na2Standalone(t, ctx, 512*1024)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })
	infraM := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraM, nil)
	coordM := tmdCoordinatorWithWorkers(t, ctx, infraM,
		func(w *worker.Config) { w.MorselWorkers = 4 })

	budgeted := func(sql string) ([]string, error) {
		restoreDrain := exec.ForceAggDrainEvery(1)
		restoreRuns := exec.ForceSmallSpillRuns(512)
		out, err := na2Run(tmdRunSingle(ctx, spilled, sql))
		restoreRuns()
		exec.ForceAggDrainEvery(restoreDrain)
		return out, err
	}

	arms := []struct {
		name  string
		coord *Coordinator
		run   func(sql string) ([]string, error)
	}{
		{"single", nil, func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, single, sql)) }},
		{"single+budget", nil, budgeted},
		{"dag", coord, func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, sql)) }},
		{"dag-shuffled", coordB, func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, sql)) }},
		{"dag-morsel4", coordM, func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordM, sql)) }},
	}

	for _, tc := range tcpflagcases.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			for _, a := range arms {
				q := strings.ReplaceAll(strings.ReplaceAll(tc.SQL, "users", "tcpflow"), "name LIKE", "CAST(id AS VARCHAR) LIKE")
				q = strings.ReplaceAll(q, "visits", "f8")
				before := a2fReadRoutes(a.coord)
				out, err := a.run(q)
				// `set_order_by` was a named residual here: the set-level term
				// had no output slot, the DAG routed it locally and the empty
				// sort never evaluated it. Since arc BR the binder holds that
				// term to PostgreSQL's rule and transforms it first, so it is
				// 22023 before any route — the residual is closed.
				a2fCheckRoutes(t, a.name, a.coord, before, q)
				door := "single"
				if a.coord != nil {
					door = "dag"
				}
				want := tcpflagcases.State(tc.Name, door)
				if want == "" && strings.HasPrefix(tc.Name, "merge_") && (err != nil || len(out) != 1 || out[0] != "result=MERGE 0") {
					t.Errorf("MERGE residual changed: %v %v", out, err)
				}
				if want == "" && !strings.HasPrefix(tc.Name, "merge_") && (err != nil || len(out) != 0) {
					t.Errorf("empty SELECT residual changed: %v %v", out, err)
				}
				if sqlerr.StateOf(err) != want || (want == "22023" && (err == nil || !strings.Contains(err.Error(), "BOGUS"))) {
					t.Errorf("%s SQL=%s rows=%v err=%v state=%s; want %s", a.name, q, out, err, sqlerr.StateOf(err), want)
				}
			}
		})
	}

	for _, tc := range tcpflagcases.RecursiveControls {
		t.Run(tc.Name, func(t *testing.T) {
			for _, a := range arms {
				q := strings.ReplaceAll(tc.SQL, "users", "tcpflow")
				before := a2fReadRoutes(a.coord)
				out, err := a.run(q)
				a2fCheckRoutes(t, a.name, a.coord, before, q)
				if err != nil || len(out) != 0 {
					t.Fatalf("%s valid recursive name: rows=%v error=%v", a.name, out, err)
				}
			}
		})
	}
}
