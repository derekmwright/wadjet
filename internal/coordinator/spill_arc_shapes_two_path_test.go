package coordinator

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/worker"
)

// The spill arc's shapes, on three DAG arms, with the drain forced.
//
// #782 (a JOIN's spilled probe replay landing in the wrong aggregate sink),
// its DECIMAL/FLOAT twin (a drain on the batch that migrated the key path),
// #779 (an ungrouped aggregate buffering rows it must not read) and #632 (a
// BYTES group key rendered as display text by the row spill) were all found
// and fixed on the single-process engine. Each lives in code the stage DAG
// also runs — HashAggregate's drains and its row buffer, the memory layer's
// row spill — but reaches it through a different runner (the worker's
// fragment executor, not exec.Pipeline), so "fixed on one path" is a claim
// about the other path, not evidence about it (ADR-0018 §3).
//
// THE FORCED DRAIN IS WHAT MAKES THIS A GATE. Under a budget alone the DAG
// arms pass on e96640c6 with all four defects present: three workers over a
// 5000-row fixture never accumulate enough state to drain, so the comparison
// is between two in-memory runs and would hold with the fixes deleted. The
// first version of this file recorded that as a property of the DAG ("a DAG
// arm that reproduces a spill defect wants a fixture large enough to pressure
// a worker"). It is not: it is a property of the fixture. Arming
// exec.ForceAggDrainEvery(1) around the DAG arms — the cluster's workers run
// in this process, so they inherit the knob — puts a drain on every batch, and
// on e96640c6 the gate then FAILS — 3 to 4 of the 6 shapes depending on the
// run, always including the three below, always on dag+budgeted-workers:
//
//	SUM(c_dec) GROUP BY g    ->  0.0000       (correct: 952796.2701)
//	SUM(SUM(c_dec)) OVER ()  ->  0.0000       (correct: 12375061.3824)
//	SUM(c_f64), AVG(c_f64)   ->  0, 0         with COUNT(c_f64) = 375 kept
//
// which is M2's spec loss reaching the worker's fragment executor: the count
// survives and the value is gone, because the run header said the accumulator
// was an integer. The class DOES live on the DAG; the tip fixes it there too.
// (779_scalar_count also fails on some base runs — it needs the raw-row branch,
// which real pressure rather than this knob decides, so it is run-dependent.)
//
// The single-process REFERENCE is taken with the knob disarmed, so the two
// sides differ in one thing only — the drain — and a divergence is the DAG's.
func TestSpillArcShapesAgreeOnBothDistributionArms(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })
	// A third arm whose WORKERS are budgeted. The default DAG workers get a
	// 64 MiB cache and no memory budget; this one is sized to the same order as
	// the 512 KiB the single-process arm uses (per task, so the shared pool is
	// MaxConcurrent times it).
	infraC := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraC, nil)
	coordC := tmdCoordinatorWithWorkers(t, ctx, infraC,
		func(w *worker.Config) { w.MemoryBudget = 512 * 1024; w.CacheBytes = 1 << 20 })

	for _, tc := range []struct {
		name, sql string
		// noAggDrain names why this shape cannot take the external-merge
		// drain, for the cells where the forcing knob legitimately fires
		// nothing. Empty means the knob MUST engage.
		noAggDrain string
	}{
		{
			// #782 itself: the join is what produces a deferred
			// spilled-partition flush, and the GROUP BY above it is what the
			// flush was landing in unrouted.
			"782_join_group_by",
			`SELECT z.g AS w, COUNT(*) AS n FROM typemx x JOIN typemx z ON x.id = z.id GROUP BY z.g ORDER BY w`, "",
		},
		{
			// #782's DECIMAL twin: a NULL group key migrates the int-keyed
			// path mid-consume, and a drain after that migration wrote the
			// DECIMAL accumulator down the integer arm.
			"782_decimal_window",
			`SELECT g AS k, ROW_NUMBER() OVER (ORDER BY g) AS r, SUM(c_dec) AS s FROM typemx GROUP BY g ORDER BY k`, "",
		},
		{
			"782_decimal_over_agg",
			`SELECT g AS k, SUM(SUM(c_dec)) OVER () AS w FROM typemx GROUP BY g ORDER BY k`, "",
		},
		{
			// The FLOAT half of the same loss.
			"782_float_sum",
			`SELECT g AS k, SUM(c_f64) AS s, AVG(c_f64) AS a, COUNT(c_f64) AS n FROM typemx GROUP BY g ORDER BY k`, "",
		},
		{
			// #779: an ungrouped aggregate over a column whose every use is a
			// SHAPE use, so the scan ships lengths and no bytes.
			"779_scalar_count",
			`SELECT COUNT(c_str) AS n, COUNT(c_bytes) AS nb, COUNT(*) AS all_rows FROM typemx`,
			// M3's whole point: an ungrouped aggregate no longer buffers or
			// drains anything, so a forced drain has nothing to write. Its
			// engagement evidence is that it FAILED LOUDLY on e96640c6.
			"ungrouped: canUseExternalMerge is false and M3 removed the row buffer",
		},
		{
			// #832 on the DAG. A join whose ON equates two EXPRESSIONS has no
			// equi-key and is planned as a CROSS join, whose probe reads every
			// build row — so its build cannot be grace-partitioned, and a
			// budgeted worker now takes the flat build for it. The spilled arm
			// is gated by the sweep's crossjoin family; without these cells the
			// fix is gated on one arm out of four, and the arm that changed on
			// the DAG (a worker WITH a memory budget) is not it.
			"832_computed_key_upper",
			`SELECT COUNT(*) AS n FROM typemx a JOIN typemx b ON UPPER(a.c_str) = UPPER(b.c_str) ` +
				`WHERE a.id < 200 AND b.id < 200`,
			"ungrouped COUNT(*): no group state for a forced drain to write",
		},
		{
			// The arithmetic spelling, which reaches the same disposition by a
			// different route through the planner.
			"832_computed_key_numeric",
			`SELECT COUNT(*) AS n FROM typemx a JOIN typemx b ON a.id + 1 = b.id + 1 ` +
				`WHERE a.id < 200 AND b.id < 200`,
			"ungrouped COUNT(*): no group state for a forced drain to write",
		},
		{
			// The WIDE build — the whole fixture, not two hundred rows. It is
			// the cell that says #832's residual is NOT a distributed one: the
			// single-process arm REFUSES this at 512 KiB, because a cross
			// join's build cannot spill and must fit, while the budgeted DAG
			// arm answers it — the DAG splits the build across workers, so no
			// single build has to fit one worker's budget.
			"832_computed_key_wide",
			`SELECT COUNT(*) AS n FROM typemx a JOIN typemx b ON UPPER(a.c_str) = UPPER(b.c_str)`,
			"ungrouped COUNT(*): no group state for a forced drain to write",
		},
		{
			// #791 on the DAG: a shape-only column counted beside a
			// NON-SIMPLE aggregate, which is the route onto the raw-row buffer
			// the fix taught to carry lengths.
			"791_grouped_shape_only",
			`SELECT g AS k, COUNT(c_str) AS n, COUNT(DISTINCT id) AS d FROM typemx GROUP BY g ORDER BY k`,
			"non-simple aggregate: the legacy raw-row path, not the external merge",
		},
		{
			// A shape use that reads the LENGTH rather than only the null
			// mask, so the box has to CARRY the length across the DAG's own
			// spill, not merely refuse the value.
			"791_grouped_shape_only_length",
			`SELECT g AS k, AVG(LENGTH(c_str)) AS a, COUNT(DISTINCT id) AS d FROM typemx GROUP BY g ORDER BY k`,
			"non-simple aggregate: the legacy raw-row path, not the external merge",
		},
		{
			// The GROUPING SETS route onto the same buffer, which needs no
			// second aggregate at all.
			"791_grouped_shape_only_rollup",
			`SELECT g AS k, COUNT(c_str) AS n FROM typemx GROUP BY ROLLUP(g)`,
			"GROUPING SETS: the legacy raw-row path, not the external merge",
		},
		{
			// #632: a top-level BYTES group key, on the raw-row path that
			// COUNT(DISTINCT) forces.
			"632_bytes_key",
			`SELECT c_bytes AS k, COUNT(DISTINCT id) AS n FROM typemx WHERE id < 400 GROUP BY c_bytes ORDER BY k`,
			// COUNT(DISTINCT) is non-simple, so this is the LEGACY raw-row
			// path, which the aggregate-drain knob does not reach. #632's
			// engagement evidence is
			// exec.TestRawRowSpillPreservesEveryFlatGroupKeyType, which
			// asserts a raw-row file actually reached disk.
			"non-simple aggregate: the legacy raw-row path, not the external merge",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want, err := tmdRunSingle(ctx, single, tc.sql)
			if err != nil {
				t.Fatalf("single-process arm: %v\n  SQL: %s", err, tc.sql)
			}
			if len(want.Rows) == 0 {
				t.Fatalf("the single-process arm returned no rows — this shape would compare nothing\n  SQL: %s", tc.sql)
			}
			w := spillArcRows(want.Columns, want.Rows)

			// Force a drain on every batch for the DAG arms only. The reference
			// above was taken disarmed on purpose: with the knob on BOTH sides a
			// shared defect would cancel out and the comparison would agree.
			forcedBefore := exec.ForcedAggDrains.Load()
			restore := exec.ForceAggDrainEvery(1)
			defer exec.ForceAggDrainEvery(restore)
			for _, arm := range []struct {
				name  string
				coord *Coordinator
			}{{"dag", coord}, {"dag+broadcast-override", coordB}, {"dag+budgeted-workers", coordC}} {
				got, err := tmdRunDAG(ctx, arm.coord, tc.sql)
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s", arm.name, err, tc.sql)
					continue
				}
				g := spillArcRows(got.Columns, got.Rows)
				if len(g) != len(w) {
					t.Errorf("%s arm: %d rows, want %d\n  SQL: %s", arm.name, len(g), len(w), tc.sql)
					continue
				}
				for i := range w {
					if g[i] != w[i] {
						t.Errorf("%s arm: row %d differs\n  %s: %s\n  single: %s\n  SQL: %s",
							arm.name, i, arm.name, g[i], w[i], tc.sql)
						break
					}
				}
			}
			switch {
			case tc.noAggDrain != "":
				t.Logf("no forced drain expected here (%s)", tc.noAggDrain)
			case exec.ForcedAggDrains.Load() == forcedBefore:
				t.Error("the forcing knob fired no drain on any DAG arm — the workers did not " +
					"inherit it, so this cell compared in-memory runs and proves nothing")
			}
		})
	}
}

// spillArcRows renders a result to sorted comparable text with the Go TYPE
// beside each value: a []byte and the display text a lossy encoder turns it
// into print identically under %v, which is exactly what #632 produced.
func spillArcRows(columns []string, rows []map[string]any) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		s := ""
		for _, c := range columns {
			v := r[c]
			if v == nil {
				s += c + "=NULL|"
				continue
			}
			switch t := v.(type) {
			case float64:
				// A float sum's last digits move with accumulation order,
				// which differs between one process and three workers
				// (ADR-0013 nondeterminism class 9). DECIMAL is not rounded.
				s += fmt.Sprintf("%s=float:%.6g|", c, t)
			case float32:
				s += fmt.Sprintf("%s=float:%.6g|", c, float64(t))
			default:
				s += fmt.Sprintf("%s=%T:%v|", c, v, v)
			}
		}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}
