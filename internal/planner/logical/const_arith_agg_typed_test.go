package logical

import (
	"fmt"
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #850: the const-arith aggregate lift decides from the column's TYPE.
//
// #841 declined the lift for EVERY integer literal because the builder runs
// before any type is known. This pass runs after annotation, where the type
// and the manifest's bounds are on the Scan, and lifts exactly the aggregates
// whose per-row form provably cannot refuse — and whose LIFTED form provably
// cannot either, which is the half a "check the column" rule would miss.

// caaPlan builds the tree the BUILDER produces for a declined aggregate:
// Project(IsAgg) over Aggregate(the whole expression as the input) over an
// annotated Scan.
func caaPlan(t *testing.T, sqlExpr string, col string, typ parquet.TypeID,
	lo, hi any, rows int64) (*Node, *Node) {
	t.Helper()
	node, err := plansql.ParseExpressionComplete(sqlExpr)
	if err != nil {
		t.Fatalf("parse %q: %v", sqlExpr, err)
	}
	fn, ok := node.(*plansql.FuncCallNode)
	if !ok {
		t.Fatalf("%q is %T, want a function call", sqlExpr, node)
	}
	scan := NewScan("t", "")
	scan.ScanColumns = []string{col, "g"}
	scan.ScanColTypes = map[string]parquet.TypeID{col: typ, "g": parquet.TypeInt32}
	scan.ScanRowEstimate = rows
	if lo != nil {
		scan.ScanColStats = map[string]ScanColumnStats{
			col: {MinValue: lo, MaxValue: hi, TotalRows: rows},
		}
	}
	agg := &Node{Type: NodeAggregate, Children: []*Node{scan}, AggExprs: []AggExpr{{
		Func:      strings.ToLower(fn.Name),
		InputCol:  fn.Args[0].String(),
		OutputCol: "s",
		InputExpr: fn.Args[0],
	}}}
	proj := &Node{Type: NodeProject, Children: []*Node{agg}, Projections: []Projection{{
		Alias: "s", Expr: sqlExpr, IsAgg: true, ASTExpr: fn,
	}}}
	return proj, agg
}

func caaShape(proj, agg *Node) string {
	var sb strings.Builder
	for _, p := range proj.Projections {
		ast := "nil"
		if p.ASTExpr != nil {
			ast = p.ASTExpr.String()
		}
		fmt.Fprintf(&sb, "proj{%s isagg=%v ast=%s} ", p.Alias, p.IsAgg, ast)
	}
	for _, a := range agg.AggExprs {
		fmt.Fprintf(&sb, "agg{%s(%s)->%s} ", a.Func, a.InputCol, a.OutputCol)
	}
	return strings.TrimSpace(sb.String())
}

func TestConstArithLiftDecidesFromTheColumnType(t *testing.T) {
	const million = 1_000_000
	for _, c := range []struct {
		name, expr string
		typ        parquet.TypeID
		lo, hi     any
		rows       int64
		want       string
	}{
		// An INT32 column BOUNDS ITSELF: no statistics are needed, which is
		// what makes the ClickBench columns liftable on a table nobody has
		// ANALYZEd. int32's range plus 3 cannot leave int64, and
		// 3 × 1e6 cannot either.
		{"int32_plus", "SUM(w + 3)", parquet.TypeInt32, nil, nil, million,
			"proj{s isagg=false ast=__agg_0 + 3 * __agg_1} agg{sum(w)->__agg_0} agg{count(w)->__agg_1}"},
		{"int32_minus", "SUM(w - 3)", parquet.TypeInt32, nil, nil, million,
			"proj{s isagg=false ast=__agg_0 - 3 * __agg_1} agg{sum(w)->__agg_0} agg{count(w)->__agg_1}"},
		{"int32_const_minus", "SUM(3 - w)", parquet.TypeInt32, nil, nil, million,
			"proj{s isagg=false ast=3 * __agg_1 - __agg_0} agg{sum(w)->__agg_0} agg{count(w)->__agg_1}"},
		{"int32_times", "SUM(w * 2)", parquet.TypeInt32, nil, nil, million,
			"proj{s isagg=false ast=__agg_0 * 2} agg{sum(w)->__agg_0}"},
		{"int32_avg", "AVG(w + 3)", parquet.TypeInt32, nil, nil, million,
			"proj{s isagg=false ast=__agg_0 + 3} agg{avg(w)->__agg_0}"},
		{"int32_min", "MIN(w + 3)", parquet.TypeInt32, nil, nil, million,
			"proj{s isagg=false ast=__agg_0 + 3} agg{min(w)->__agg_0}"},
		{"int32_max", "MAX(w * 2)", parquet.TypeInt32, nil, nil, million,
			"proj{s isagg=false ast=__agg_0 * 2} agg{max(w)->__agg_0}"},
		// MIN(k - x) is k - MAX(x): the inner aggregate FLIPS.
		{"int32_min_const_minus", "MIN(3 - w)", parquet.TypeInt32, nil, nil, million,
			"proj{s isagg=false ast=3 - __agg_0} agg{max(w)->__agg_0}"},
		// A DOUBLE column DECLINES, and a VALUE says so rather than a
		// disposition: IEEE addition is not associative, so `SUM(f + k)` and
		// `SUM(f) + k*COUNT(f)` are different numbers as soon as the summands
		// span enough magnitude to cancel. Over `1e16, 1, 1, 1, 1` PostgreSQL
		// 17.11 answers 1.0000000000000008e+16 and the lifted form answers
		// …004e+16 (round-1 review, B1).
		// wadjet.TestConstArithLiftIsNotAppliedToFloatColumns is that fixture.
		{"float64", "SUM(f + 3)", parquet.TypeFloat64, nil, nil, million,
			"proj{s isagg=true ast=sum(f + 3)} agg{sum(f + 3)->s}"},
		// A REAL column declines for a sharper version of the same thing: the
		// per-row multiplication widens each value to a double before
		// accumulating while the lifted SUM accumulates at float4's width — a
		// different ACCUMULATOR, not a last-ulp reordering.
		{"float32", "SUM(r + 3)", parquet.TypeFloat32, nil, nil, million,
			"proj{s isagg=true ast=sum(r + 3)} agg{sum(r + 3)->s}"},
		// An INT64 column WITH statistics that bound it.
		{"int64_with_stats", "SUM(b + 3)", parquet.TypeInt64, int64(0), int64(1000), million,
			"proj{s isagg=false ast=__agg_0 + 3 * __agg_1} agg{sum(b)->__agg_0} agg{count(b)->__agg_1}"},

		// ---- the DECLINES, each for a stated reason -----------------------
		// No statistics: min/max is what proves the bound, and a guess would
		// be a wrong ANSWER rather than a slow one.
		{"int64_without_stats", "SUM(b + 3)", parquet.TypeInt64, nil, nil, million,
			"proj{s isagg=true ast=sum(b + 3)} agg{sum(b + 3)->s}"},
		// A bound that does NOT hold: the per-row `b + k` leaves int64 at the
		// column's maximum, which is exactly what #841 refuses to move.
		{"int64_per_row_overflows", "SUM(b + 9223372036854775807)",
			parquet.TypeInt64, int64(0), int64(1000), million,
			"proj{s isagg=true ast=sum(b + 9223372036854775807)} agg{sum(b + 9223372036854775807)->s}"},
		// The LIFTED form's own arithmetic. `b + 4611686018427387904` does not
		// overflow at b ∈ [0, 1000], so a rule that checked only the per-row
		// form would lift — and `k × COUNT(b)` then raises 22003 for a query
		// PostgreSQL answers. This is #841's defect pointing the other way.
		{"lifted_k_times_count_overflows", "SUM(b + 4611686018427387904)",
			parquet.TypeInt64, int64(0), int64(1000), million,
			"proj{s isagg=true ast=sum(b + 4611686018427387904)} agg{sum(b + 4611686018427387904)->s}"},
		// The numeric CARRIER: SUM over an integer is numeric(38,0), and
		// 1e6 rows × 1e30 × 2 has no place in 38 digits.
		{"lifted_sum_leaves_the_carrier", "SUM(b * 2)",
			parquet.TypeInt64, int64(0), caaBig(1e30), million,
			"proj{s isagg=true ast=sum(b * 2)} agg{sum(b * 2)->s}"},
		// A DECIMAL column: unchanged from #841's decline. The engine's own
		// 128-bit carrier can refuse where PostgreSQL answers, and the two
		// forms round at different scales.
		{"decimal", "SUM(d + 3)", parquet.TypeDecimal, nil, nil, million,
			"proj{s isagg=true ast=sum(d + 3)} agg{sum(d + 3)->s}"},
		// A type the arithmetic does not apply to at all.
		{"timestamp", "SUM(ts + 3)", parquet.TypeTimestamp, nil, nil, million,
			"proj{s isagg=true ast=sum(ts + 3)} agg{sum(ts + 3)->s}"},
		// MIN/MAX over a multiplication is order-preserving only for k > 0,
		// the bound the syntactic pass already carries.
		{"min_times_negative", "MIN(w * -2)", parquet.TypeInt32, nil, nil, million,
			"proj{s isagg=true ast=min(w * -2)} agg{min(w * -2)->s}"},
		// No row bound: the manifest could not say how many rows there are, so
		// `k × COUNT` has nothing to be proven against.
		{"no_row_estimate", "SUM(w + 3)", parquet.TypeInt32, nil, nil, 0,
			"proj{s isagg=true ast=sum(w + 3)} agg{sum(w + 3)->s}"},
	} {
		t.Run(c.name, func(t *testing.T) {
			col := caaColOf(t, c.expr)
			proj, agg := caaPlan(t, c.expr, col, c.typ, c.lo, c.hi, c.rows)
			liftConstArithAggsWithTypes(proj)
			if got := caaShape(proj, agg); got != c.want {
				t.Errorf("\n got  %s\n want %s", got, c.want)
			}
		})
	}
}

// caaBig is a value too wide for an int64 literal in the table above, as a
// statistic the catalog could hold.
func caaBig(f float64) any { return f }

func caaColOf(t *testing.T, expr string) string {
	t.Helper()
	for _, c := range []string{"w", "b", "f", "r", "d", "ts"} {
		if strings.Contains(expr, "("+c+" ") || strings.Contains(expr, " "+c+")") {
			return c
		}
	}
	t.Fatalf("no column in %q", expr)
	return ""
}

// The DEDUP: Q30's shape is ninety aggregates over ONE column, and the whole
// recovery is that they collapse to one SUM and one COUNT rather than ninety
// accumulators over ninety per-row expression passes.
func TestConstArithLiftSharesOneAggregatePerColumn(t *testing.T) {
	const n = 90
	scan := NewScan("t", "")
	scan.ScanColumns = []string{"w"}
	scan.ScanColTypes = map[string]parquet.TypeID{"w": parquet.TypeInt32}
	scan.ScanRowEstimate = 1_000_000
	agg := &Node{Type: NodeAggregate, Children: []*Node{scan}}
	proj := &Node{Type: NodeProject, Children: []*Node{agg}}
	for i := 0; i < n; i++ {
		src := fmt.Sprintf("SUM(w + %d)", i)
		node, err := plansql.ParseExpressionComplete(src)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		fn := node.(*plansql.FuncCallNode)
		out := fmt.Sprintf("s%d", i)
		agg.AggExprs = append(agg.AggExprs, AggExpr{
			Func: "sum", InputCol: fn.Args[0].String(), OutputCol: out, InputExpr: fn.Args[0],
		})
		proj.Projections = append(proj.Projections, Projection{
			Alias: out, Expr: src, IsAgg: true, ASTExpr: fn,
		})
	}
	liftConstArithAggsWithTypes(proj)
	if len(agg.AggExprs) != 2 {
		t.Fatalf("%d aggregates after the lift, want 2 (one SUM, one COUNT): %s",
			len(agg.AggExprs), caaShape(proj, agg))
	}
	for i := range proj.Projections {
		if proj.Projections[i].IsAgg {
			t.Fatalf("projection %d was not lifted", i)
		}
	}
}

// The slot allocator, which has NO renameCollidingSlots pass behind it: that
// runs inside AnnotateScanColumns, which is over by the time this pass looks.
// It can do better than the builder could, because the scan's real column list
// is on the node by now — so a table that STORES a column called `__agg_0`
// (#694's shape) must not have it shadowed.
func TestConstArithLiftSlotAvoidsAStoredColumn(t *testing.T) {
	scan := NewScan("t", "")
	scan.ScanColumns = []string{"w", "__agg_0", "__agg_1"}
	scan.ScanColTypes = map[string]parquet.TypeID{
		"w": parquet.TypeInt32, "__agg_0": parquet.TypeInt32, "__agg_1": parquet.TypeInt32,
	}
	scan.ScanRowEstimate = 1000
	node, _ := plansql.ParseExpressionComplete("SUM(w + 3)")
	fn := node.(*plansql.FuncCallNode)
	agg := &Node{Type: NodeAggregate, Children: []*Node{scan}, AggExprs: []AggExpr{{
		Func: "sum", InputCol: fn.Args[0].String(), OutputCol: "s", InputExpr: fn.Args[0],
	}}}
	proj := &Node{Type: NodeProject, Children: []*Node{agg}, Projections: []Projection{{
		Alias: "s", Expr: "SUM(w + 3)", IsAgg: true, ASTExpr: fn,
	}}}
	liftConstArithAggsWithTypes(proj)
	for _, a := range agg.AggExprs {
		for _, stored := range scan.ScanColumns {
			if strings.EqualFold(a.OutputCol, stored) {
				t.Errorf("minted slot %q collides with the STORED column of that name", a.OutputCol)
			}
		}
	}
	if len(agg.AggExprs) != 2 {
		t.Errorf("%s", caaShape(proj, agg))
	}
}

// The KILL SWITCH disarms this pass with the syntactic one, so the
// optimization-invariance oracle covers it for free (#287).
func TestConstArithLiftRidesItsKillSwitch(t *testing.T) {
	prev := constArithAggToggle.Set(false)
	defer constArithAggToggle.Set(prev)
	proj, agg := caaPlan(t, "SUM(w + 3)", "w", parquet.TypeInt32, nil, nil, 1000)
	liftConstArithAggsWithTypes(proj)
	if got, want := caaShape(proj, agg),
		"proj{s isagg=true ast=sum(w + 3)} agg{sum(w + 3)->s}"; got != want {
		t.Errorf("\n got  %s\n want %s", got, want)
	}
}

// A Project, a join or a set operation below the Aggregate can rebind a name,
// so the walk stops there — strictIntArithCols' rule, and for the same reason:
// a wrong type claim is a wrong ANSWER, not a slow one.
func TestConstArithLiftStopsWhereANameCanBeRebound(t *testing.T) {
	for _, below := range []NodeType{NodeProject, NodeJoin, NodeUnion, NodeDistinct} {
		t.Run(below.String(), func(t *testing.T) {
			proj, agg := caaPlan(t, "SUM(w + 3)", "w", parquet.TypeInt32, nil, nil, 1000)
			scan := agg.Children[0]
			agg.Children[0] = &Node{Type: below, Children: []*Node{scan}}
			liftConstArithAggsWithTypes(proj)
			if got, want := caaShape(proj, agg),
				"proj{s isagg=true ast=sum(w + 3)} agg{sum(w + 3)->s}"; got != want {
				t.Errorf("lifted through a %s\n got  %s\n want %s", below, got, want)
			}
		})
	}
	// A Filter, a Limit and a Sort cannot rebind a name, so the walk passes
	// through them — without this the ClickBench shape (which has a WHERE)
	// would decline for the wrong reason.
	for _, below := range []NodeType{NodeFilter, NodeLimit, NodeSort} {
		t.Run("through_"+below.String(), func(t *testing.T) {
			proj, agg := caaPlan(t, "SUM(w + 3)", "w", parquet.TypeInt32, nil, nil, 1000)
			scan := agg.Children[0]
			agg.Children[0] = &Node{Type: below, Children: []*Node{scan}}
			liftConstArithAggsWithTypes(proj)
			if proj.Projections[0].IsAgg {
				t.Errorf("declined through a %s: %s", below, caaShape(proj, agg))
			}
		})
	}
}
