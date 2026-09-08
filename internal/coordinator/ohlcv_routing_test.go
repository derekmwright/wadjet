package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// THE BAR CROSSES THE DAG AS A STATE, AND THAT IS ASSERTED, NOT INFERRED
// (#965, ADR-0035).
//
// Every value cell in ohlcv_two_path_test.go passes on all three arms whether
// the bar is dispatched as ONE `RawInputAggregate` over raw rows — correct,
// and what `aggNeedsWholeInput` gives a non-re-aggregatable answer — or as the
// partial/merge split over its STATE. Rows cannot tell those apart, which is
// exactly the case the routing rule is for: the claim ADR-0035 makes is the
// SECOND shape, so the second shape is what is asserted, at the two places it
// is observable.
//
//  1. `Coordinator.OhlcvStateRoutes` counts the dispatches that rewrote a bar
//     into its state. The rewrite happens at DISPATCH, over the wire specs, so
//     the planned stage list still says `ohlcv` — reading the plan and
//     concluding anything about the route was this gate's first draft and it
//     was wrong.
//  2. The plan is TWO-LEVEL and not a RawInputAggregate, which is what makes
//     that rewrite reachable at all.
//
// A control runs beside both: a query with no bar must leave the counter
// alone, so "it fired" means something.
func TestTheBarCrossesTheDAGAsAMergeableState(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)

	const barSQL = `SELECT time_bucket(INTERVAL '1' MINUTE, ts) AS bkt,
	                       ohlcv(ts, px_f64, vol_i64) AS b
	                FROM ` + ohlcvTable + ` GROUP BY 1 ORDER BY 1`

	// The control first: an aggregate query with no bar leaves the counter at
	// whatever it was.
	before := coord.OhlcvStateRoutes()
	if _, err := tmdRunDAG(ctx, coord,
		`SELECT COUNT(*) AS n, MAX(px_f64) AS m FROM `+ohlcvTable); err != nil {
		t.Fatalf("control query: %v", err)
	}
	if got := coord.OhlcvStateRoutes(); got != before {
		t.Fatalf("a query with no bar moved OhlcvStateRoutes from %d to %d — the counter "+
			"counts something other than the rewrite it names", before, got)
	}

	if _, err := tmdRunDAG(ctx, coord, barSQL); err != nil {
		t.Fatalf("bar query: %v", err)
	}
	if got := coord.OhlcvStateRoutes(); got <= before {
		t.Errorf("OhlcvStateRoutes stayed at %d: no dispatch decomposed the bar into its "+
			"mergeable state. The query still ANSWERS — the one-level RawInputAggregate over "+
			"raw rows gives the same bar — but the whole input then crosses the exchange and "+
			"an ungrouped bar collapses to one task, which is not what ADR-0035 claims.", got)
	}

	// And the plan is the two-level shape that makes the rewrite reachable.
	stages := planStagesForTest(t, ctx, infra.cat, barSQL, 3, 1)
	var sawProducer, sawFinal, sawRawInput bool
	for _, st := range stages {
		specs := st.AggSpecs
		if len(specs) == 0 {
			specs = st.FusedAggSpecs
		}
		isMerge := st.Type == physical.StageFinalAggregate || st.Type == physical.StageMergeAggregate
		for _, a := range specs {
			if !strings.EqualFold(a.Func, exec.OhlcvFunc) {
				continue
			}
			if len(a.OutputFields) != len(exec.OhlcvFieldNames) {
				t.Errorf("the %s stage's bar carries %d declared fields, want %d. The worker "+
					"has no catalog: without them the final fold cannot build the ROW column "+
					"at all, and a guessed field list writes the right numbers into the wrong "+
					"fields", st.Type, len(a.OutputFields), len(exec.OhlcvFieldNames))
			}
			if !isMerge {
				sawProducer = true
			}
		}
		if isMerge {
			sawFinal = true
		}
		if st.RawInputAggregate {
			sawRawInput = true
		}
	}
	if sawRawInput {
		t.Errorf("the bar is planned as a one-level RawInputAggregate.%s", ohlcvStageShapes(stages))
	}
	if !sawProducer || !sawFinal {
		t.Errorf("the plan is not two-level (producer=%v final=%v), so there is no partial for "+
			"the dispatch to rewrite.%s", sawProducer, sawFinal, ohlcvStageShapes(stages))
	}
}

// decomposeOhlcv's own rewrite, spelled out: the FUNCTION, the synthetic NAME
// and the wire TYPE all move together, and everything else on the spec — the
// three input columns and the declared fields the final fold needs — survives
// untouched.
func TestDecomposeOhlcvRewritesTheBarIntoItsState(t *testing.T) {
	fields, ok := exec.OhlcvOutputFields(
		parquet.Column{Name: "px", Type: parquet.TypeFloat64},
		parquet.Column{Name: "vol", Type: parquet.TypeInt64})
	if !ok {
		t.Fatal("no bar over float8 price and int8 volume")
	}
	in := []distributed.AggSpec{
		{Func: "count", OutputCol: "n"},
		{Func: exec.OhlcvFunc, InputCol: "px", InputCol2: "ts", InputCol3: "vol",
			OutputCol: "b", OutputFields: []distributed.AggFieldSpec{
				{Name: "open"}, {Name: "high"}, {Name: "low"},
				{Name: "close"}, {Name: "volume"}, {Name: "vwap"},
			}},
	}
	out := decomposeOhlcv(in)
	if len(out) != 2 {
		t.Fatalf("produced %d specs, want 2", len(out))
	}
	if out[0].Func != "count" || out[0].OutputCol != "n" {
		t.Errorf("the non-bar spec was rewritten: %+v", out[0])
	}
	b := out[1]
	if b.Func != exec.OhlcvStateFunc {
		t.Errorf("func %q, want %q", b.Func, exec.OhlcvStateFunc)
	}
	if want := exec.OhlcvStateColumn("b"); b.OutputCol != want {
		t.Errorf("output column %q, want %q", b.OutputCol, want)
	}
	if b.OutputType == nil || parquet.TypeID(*b.OutputType) != parquet.TypeString {
		t.Errorf("output type %v, want STRING — the partial state travels as text through "+
			"parquet, .wshf and the NATS gather (ADR-0010)", b.OutputType)
	}
	if b.InputCol != "px" || b.InputCol2 != "ts" || b.InputCol3 != "vol" {
		t.Errorf("the three input columns did not survive: %q %q %q",
			b.InputCol, b.InputCol2, b.InputCol3)
	}
	if len(b.OutputFields) != len(exec.OhlcvFieldNames) {
		t.Errorf("the declared fields did not survive: %d, want %d",
			len(b.OutputFields), len(exec.OhlcvFieldNames))
	}
	// And the output column name round-trips, which is how the final fold
	// finds which bar a state belongs to.
	if name, ok := exec.OhlcvStateOutput(b.OutputCol); !ok || name != "b" {
		t.Errorf("OhlcvStateOutput(%q) = %q, %v; want \"b\", true", b.OutputCol, name, ok)
	}
	if _, ok := exec.OhlcvStateOutput("b"); ok {
		t.Error("OhlcvStateOutput accepted a name that is not a state column")
	}
	_ = fields

	// A spec list with no bar is returned unchanged, not reallocated.
	none := []distributed.AggSpec{{Func: "sum", InputCol: "a", OutputCol: "s"}}
	if got := decomposeOhlcv(none); &got[0] != &none[0] {
		t.Error("a spec list with no bar was reallocated")
	}
}

func ohlcvStageShapes(stages []physical.Stage) string {
	var b strings.Builder
	b.WriteString("\n  stages:")
	for _, st := range stages {
		fmt.Fprintf(&b, "\n    %-18s raw=%v", st.Type, st.RawInputAggregate)
		specs := st.AggSpecs
		if len(specs) == 0 {
			specs = st.FusedAggSpecs
		}
		for _, a := range specs {
			fmt.Fprintf(&b, " [%s -> %s, %d fields]", a.Func, a.OutputCol, len(a.OutputFields))
		}
	}
	return b.String()
}
