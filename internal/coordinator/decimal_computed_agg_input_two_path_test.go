package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// A COMPUTED DECIMAL aggregate argument declares its output from the
// projection it is materialized under — on both paths, identity row included.
//
// #685's review found this class through the reader guard. An aggregate over
// anything that is not a bare column reference had NO declared output:
// aggSpecOutputType declines a computed argument and falls to aggOutputType's
// float64. The partials that saw a row emitted whatever the projected vector
// actually was — DECIMAL — while the partial whose filter matched nothing
// emitted the identity row under the float64 default, so one stage's files
// described two different relations. On main at eed973a9 that was a silent
// 10^scale answer; with the guard it became a refused read. Neither is an
// answer, and the class is wide: every CAST, every column-to-column operator,
// unary minus, ABS/ROUND/GREATEST, division, CASE — and SUM(a * (1 - b)),
// which is TPC-H Q1 and Q6's revenue expression.
//
// The fix is by CONSTRUCTION rather than by inference, which is what makes it
// total: the worker builds the pre-aggregate projection from
// AggSpec.InputType/InputPrecision/InputScale, so the vector every non-empty
// partial observes IS that declaration; reading the aggregate's output off the
// same triple (physical.aggOutputFromInputDecl) makes the identity row and its
// siblings agree whatever the triple says, including the float64 fallback for
// an expression nothing can type. The single-process path reads the same rule
// off the local equivalent (the synthetic pre-projection column's declaration),
// so the two paths declare one thing for one query.
//
// Every expectation below is live PostgreSQL 17.11 over the identical nine
// rows, captured before the fix. The two exceptions are DIGITS KEPT, both
// already-recorded divergences of ADR-0024/ADR-0012 rather than anything this
// gate is about, and both are spelled out at cadWadjet.

// cadShape is one aggregate argument and what PostgreSQL answers for it under
// each id bound. "" means NULL (no row matched).
type cadShape struct {
	name string
	expr string
	pg   map[int64]string
}

var cadShapes = []cadShape{
	{"cast_18_6", `SUM(CAST(a AS DECIMAL(18,6)))`, map[int64]string{5: "38.240000", 4: "38.250000", 100: "52.990000", 0: ""}},
	{"cast_9_2", `SUM(CAST(a AS DECIMAL(9,2)))`, map[int64]string{5: "38.24", 4: "38.25", 100: "52.99", 0: ""}},
	{"cast_bare", `SUM(CAST(a AS DECIMAL))`, map[int64]string{5: "38.24", 4: "38.25", 100: "52.99", 0: ""}},
	{"add_cols", `SUM(a + b)`, map[int64]string{5: "76.4800", 4: "76.5000", 100: "88.4800", 0: ""}},
	{"negate", `SUM(-a)`, map[int64]string{5: "-38.24", 4: "-38.25", 100: "-52.99", 0: ""}},
	{"abs", `SUM(ABS(a))`, map[int64]string{5: "38.26", 4: "38.25", 100: "53.01", 0: ""}},
	{"round", `SUM(ROUND(a,1))`, map[int64]string{5: "38.4", 4: "38.4", 100: "53.2", 0: ""}},
	{"div", `SUM(a / 2)`, map[int64]string{5: "19.12000000000000000000", 4: "19.1250000000000000", 100: "26.49500000000000000000", 0: ""}},
	{"greatest", `SUM(GREATEST(a,b))`, map[int64]string{5: "38.2401", 4: "38.2501", 100: "61.9901", 0: ""}},
	// TPC-H Q1/Q6's revenue shape.
	{"revenue", `SUM(a * (1 - b))`, map[int64]string{5: "-449.447600", 4: "-449.437500", 100: "-467.447600", 0: ""}},
	// The three that were already correct, because an optimizer rewrite hoists
	// the constant out and leaves the aggregate a BARE column. They are the
	// control: they say the trigger is the computed argument, not the filter.
	{"mul_int", `SUM(a * 2)`, map[int64]string{5: "76.48", 4: "76.50", 100: "105.98", 0: ""}},
	{"mul_lit", `SUM(a * 1.5)`, map[int64]string{5: "57.360", 4: "57.375", 100: "79.485", 0: ""}},
	{"add_lit", `SUM(a + 0.00)`, map[int64]string{5: "38.24", 4: "38.25", 100: "52.99", 0: ""}},
	{"case", `SUM(CASE WHEN id > 0 THEN a ELSE NULL END)`, map[int64]string{5: "38.24", 4: "38.25", 100: "52.99", 0: ""}},
	// Not only SUM: MIN/MAX keep the input's (p,s) and AVG widens it, so each
	// reads a different arm of the same rule.
	{"min_cast", `MIN(CAST(a AS DECIMAL(18,6)))`, map[int64]string{5: "-0.010000", 4: "12.750000", 100: "-0.010000", 0: ""}},
	{"max_cast", `MAX(CAST(a AS DECIMAL(18,6)))`, map[int64]string{5: "12.750000", 4: "12.750000", 100: "12.750000", 0: ""}},
	{"avg_cast", `AVG(CAST(a AS DECIMAL(18,6)))`, map[int64]string{5: "9.5600000000000000", 4: "12.7500000000000000", 100: "7.5700000000000000", 0: ""}},
	{"min_expr", `MIN(a * (1 - b))`, map[int64]string{5: "-149.813775", 4: "-149.813775", 100: "-149.813775", 0: ""}},
	{"max_expr", `MAX(a * (1 - b))`, map[int64]string{5: "-0.010100", 4: "-149.811225", 100: "0.000000", 0: ""}},
	{"avg_expr", `AVG(a + b)`, map[int64]string{5: "19.1200000000000000", 4: "25.5000000000000000", 100: "14.7466666666666667", 0: ""}},
	// COUNT over the same argument: input-independent, and the one aggregate
	// #685 never touched. Here to say the fix did not move it.
	{"count_expr", `COUNT(a * (1 - b))`, map[int64]string{5: "4", 4: "3", 100: "6", 0: "0"}},
}

// cadWadjet overrides PostgreSQL's string where the two engines agree on the
// NUMBER and differ in how many digits they keep — both already recorded, both
// nothing to do with this gate:
//
//   - AVG over a DECIMAL is exact division at scale+4 (batch.AvgScaleIncrement,
//     ADR-0012 item 9) where PostgreSQL picks a scale giving at least 16
//     significant digits.
//   - DECIMAL division takes the finite-carrier rule of ADR-0024 item 3
//     (s = max(6, s1+p2+1), capped) where PostgreSQL's numeric is unbounded.
//
// Both agree to min(scale), which is the contract. Nothing else in the table
// needs an override — the other nineteen shapes are byte-identical to
// PostgreSQL under all four bounds.
var cadWadjet = map[string]map[int64]string{
	"div":      {5: "19.120000", 4: "19.125000", 100: "26.495000"},
	"avg_cast": {5: "9.5600000000", 4: "12.7500000000", 100: "7.5700000000"},
	"avg_expr": {5: "19.12000000", 4: "25.50000000", 100: "14.74666667"},
}

// cadBounds are the three task populations the defect turns on plus the empty
// one. decpair is written as three chunks of three rows over three workers, so
// the bound selects how many partial tasks see a row: id<5 leaves ONE with
// none, id<4 leaves TWO, id<100 leaves none empty, id<0 empties them all.
var cadBounds = []struct {
	bound int64
	name  string
}{
	{5, "some_tasks_empty"},
	{4, "one_task_only"},
	{100, "all_match"},
	{0, "no_task_matches"},
}

func TestComputedDecimalAggregateInputTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	dfaAssertChunking(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, sh := range cadShapes {
		for _, b := range cadBounds {
			want := sh.pg[b.bound]
			if o, ok := cadWadjet[sh.name]; ok {
				if w, ok := o[b.bound]; ok {
					want = w
				}
			}
			sql := fmt.Sprintf("SELECT %s AS v FROM %s WHERE id < %d", sh.expr, dbpTable, b.bound)
			t.Run(sh.name+"_"+b.name, func(t *testing.T) {
				for _, arm := range []struct {
					name string
					dag  bool
				}{{"single", false}, {"dag", true}} {
					rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
					if len(rows) != 1 {
						t.Fatalf("%s: %d rows, want exactly 1", arm.name, len(rows))
					}
					got := rows[0]["v"]
					if want == "" {
						if got != nil {
							t.Errorf("%s %s = %#v, want NULL", arm.name, sql, got)
						}
						continue
					}
					// COUNT comes back as an int64; every other shape is
					// DECIMAL text.
					if n, ok := got.(int64); ok {
						if fmt.Sprint(n) != want {
							t.Errorf("%s %s = %d, want %s", arm.name, sql, n, want)
						}
						continue
					}
					s, ok := got.(string)
					if !ok {
						t.Errorf("%s %s = %#v (%T), want the DECIMAL text %q — a non-string box "+
							"is the float64 declaration this gate exists for", arm.name, sql, got, got, want)
						continue
					}
					if s != want {
						t.Errorf("%s %s = %q, want %q", arm.name, sql, s, want)
					}
				}
			})
		}
	}
}
