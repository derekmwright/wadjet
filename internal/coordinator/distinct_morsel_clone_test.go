package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/worker"
)

// A DISTINCT aggregate does not survive being cloned — in the WORKER either.
//
// #703 fenced exec.Pipeline's CloneSink. The worker has its own, in
// runBreakerConsumeParallel, and that is the only one a fragment's
// morsel-parallel breaker reaches. With it unguarded, four morsel workers each
// built their own DISTINCT value set over a disjoint slice of the input and the
// merge ADDED the four sums: SUM(DISTINCT a) answered 64.96 for 16.24, exactly
// four times, on a fixture built so the split really happens.
//
// The census covers these shapes once per arm. This gate is the review's
// explicit ask and covers what a single run cannot:
//
//   - REPLICATED five times per cell, because the defect's magnitude depends on
//     how the morsels divided and a single passing run proves nothing about the
//     next one (ADR-0027's rule, applied to a clone rather than a spill);
//   - both DAG shapes at the fixed width — plain, and with every join
//     broadcast — because probe-split and broadcast reach the worker's
//     breaker through different dispatch;
//   - grouped AND ungrouped, which are different operators: the grouped form
//     keeps its value sets in the flat SoA arrays and the ungrouped one in a
//     single accumulator;
//   - the SPILLED arm, where the drain writes and reads partial state, with the
//     reference arm disarmed (ADR-0027 §6).
//
// revdup is 7 500 rows across four batches with every value present in every
// batch, so any split of the input sees every value and a clone merge that adds
// rather than unions shows up as a clean multiple of the right answer.
func TestDistinctAggregateSurvivesMorselCloningOnTheDAG(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	infraM := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraM, nil)
	coordM := tmdCoordinatorWithWorkers(t, ctx, infraM,
		func(w *worker.Config) { w.MorselWorkers = 4 })

	infraMB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraMB, nil)
	coordMB := tmdCoordinatorWithWorkers(t, ctx, infraMB,
		func(w *worker.Config) { w.MorselWorkers = 4 },
		func(c *Config) { c.BroadcastBytesOverride = 1 })

	spilled := na2Standalone(t, ctx, 512*1024)

	arms := []struct {
		name string
		run  func(string) ([]string, error)
	}{
		{"dag+morsel4", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordM, sql)) }},
		{"dag+morsel4+broadcast", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordMB, sql)) }},
		{"single+budget+forced-drain", func(sql string) ([]string, error) {
			restore := exec.ForceAggDrainEvery(1)
			defer exec.ForceAggDrainEvery(restore)
			return na2Run(tmdRunSingle(ctx, spilled, sql))
		}},
	}

	// PostgreSQL 17 over revdup, same values the census asserts.
	for _, tc := range []struct {
		name, sql string
		want      []string
	}{
		{"ungrouped", `SELECT SUM(DISTINCT a) AS sd, AVG(DISTINCT a) AS ad, ` +
			`COUNT(DISTINCT a) AS cd, SUM(DISTINCT i) AS si FROM revdup`,
			[]string{"sd=16.24|ad=5.413333|cd=int64:3|si=33"}},
		{"grouped", `SELECT g AS k, SUM(DISTINCT a) AS sd, COUNT(DISTINCT a) AS cd ` +
			`FROM revdup GROUP BY g ORDER BY k`,
			[]string{"k=int32:0|sd=16.24|cd=int64:3", "k=int32:1|sd=16.24|cd=int64:3"}},
		// The half a clone merge can get right while getting the other half
		// wrong: the plain SUM must keep summing every row.
		{"distinct_beside_plain", `SELECT SUM(DISTINCT a) AS sd, COUNT(*) AS n, SUM(i) AS si FROM revdup`,
			[]string{"sd=16.24|n=int64:7500|si=82500"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				for rep := 1; rep <= 5; rep++ {
					got, err := arm.run(tc.sql)
					if err != nil {
						t.Fatalf("%s arm, rep %d: %v\n  SQL: %s", arm.name, rep, err, tc.sql)
					}
					if len(got) != len(tc.want) {
						t.Fatalf("%s arm, rep %d: %d rows, want %d\n  got  %v\n  want %v",
							arm.name, rep, len(got), len(tc.want), got, tc.want)
					}
					for i := range got {
						if got[i] != tc.want[i] {
							t.Fatalf("%s arm, rep %d, row %d\n  got  %s\n  want %s (live PostgreSQL 17)\n"+
								"  SQL: %s\n  A clean multiple of the right answer is the clone count: "+
								"the sink was cloned and its DISTINCT sets were added, not unioned.",
								arm.name, rep, i, got[i], tc.want[i], tc.sql)
						}
					}
				}
			}
		})
	}
}
