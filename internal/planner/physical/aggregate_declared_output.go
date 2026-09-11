// This file holds aggregate declared output for the physical planner, governed by ADR-0024 and ADR-0026.
package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func aggOutputType(funcName string, distinct bool) parquet.TypeID {
	switch strings.ToLower(funcName) {
	case "count", "count_distinct", "approx_distinct":
		return parquet.TypeInt64
	case "string_agg":
		return parquet.TypeString
	case "bool_and", "every", "bool_or":
		return parquet.TypeBool
	case "ohlcv":
		return parquet.TypeRow
	case exec.OhlcvStateFunc, exec.OhlcvStateMergeFunc:
		// A partial bar travels as its encoded state — text, the way the
		// variance and covariance partials do (ADR-0010).
		return parquet.TypeString
	default:
		return parquet.TypeFloat64
	}
}

// aggOhlcvOutputFields derives ROW fields from PRICE and VOLUME declarations
// through exec.OhlcvOutputFields, the operator's own Consume-time rule.
// Unknown input declarations return ok=false so runtime vectors supply the fields.
// aggSpecOutputType likewise distinguishes unknown from TypeBool's zero TypeID;
// callers must use its bool, not treat BOOL as undeclared (#354, #371).
// See docs/internals/aggregate-output-declaration-contracts.md for the design.
func aggOhlcvOutputFields(node *logical.Node, agg logical.AggExpr) ([]parquet.Column, bool) {
	if strings.ToLower(strings.TrimSpace(agg.Func)) != "ohlcv" {
		return nil, false
	}
	col := func(name string) (parquet.Column, bool) {
		t, ok := aggInputColumnType(node, name)
		if !ok {
			return parquet.Column{}, false
		}
		c := parquet.Column{Name: name, Type: t}
		if t == parquet.TypeDecimal {
			m, known := aggInputColumnDecimal(node, name)
			if !known {
				return parquet.Column{}, false
			}
			c.Precision, c.Scale = m.Precision, m.Scale
		}
		return c, true
	}
	// The PRICE may be a COMPUTED argument, and the plan can type one: it is
	// the same declaration aggSpecOutputType and aggSpecOutputDecimal take for
	// every other aggregate over an expression (#867). Without it the bar had
	// no plan-time declaration at all for `ohlcv(ts, price*2, volume)`, and a
	// declaration the plan declines is one every consumer then invents
	// differently — which is exactly how the DAG and the single path came to
	// declare two things (#965 round 2, B1).
	price, ok := col(agg.InputCol)
	if !ok {
		t, prec, scale, known := aggComputedInputExprDecl(node, agg)
		if !known {
			return nil, false
		}
		price = parquet.Column{Name: agg.InputCol, Type: t, Precision: prec, Scale: scale}
	}
	// The VOLUME must be a bare column: a computed one is refused on every arm
	// (the pre-aggregate projection carries a reference and declines an
	// expression, #713), so there is no shape where this declines and the
	// query answers.
	vol, ok := col(agg.InputCol3)
	if !ok {
		return nil, false
	}
	return exec.OhlcvOutputFields(price, vol)
}

func aggSpecOutputType(node *logical.Node, agg logical.AggExpr) (parquet.TypeID, bool) {
	fn := strings.ToLower(strings.TrimSpace(agg.Func))
	// SUM and AVG join the input-dependent list for ONE input type: over a
	// DECIMAL column they answer in DECIMAL, exactly (#455). Over everything
	// else they are still float64, and an input this cannot resolve — a
	// derived expression, a name no scan below carries, the partial's output
	// column read by a final aggregate — keeps that float64 declaration
	// rather than becoming undeclared, which is what it has always been.
	decimalCapable := false
	switch fn {
	case "min", "max", "min_by", "max_by":
	case "sum", "avg":
		decimalCapable = true
	case "ohlcv":
		// The bar is a ROW whatever its inputs are; the FIELDS depend on
		// them, and they travel on AggSpec.OutputFields rather than here,
		// because a bare TypeID cannot carry a field list.
		return parquet.TypeRow, true
	default:
		return aggOutputType(agg.Func, agg.Distinct), true
	}
	unresolved := func() (parquet.TypeID, bool) {
		if decimalCapable {
			return aggOutputType(agg.Func, agg.Distinct), true
		}
		return 0, false
	}
	if agg.InputExpr != nil {
		if _, bare := agg.InputExpr.(*plansql.ColRef); !bare {
			// A COMPUTED argument is typed from its own EXPRESSION, over the
			// aggregate's input declarations — the same source the runtime
			// AggColumn/AggSpec already read through aggOutputFromInputDecl
			// (plan.go's synDecl override, and the DAG's spec.OutputType).
			//
			// Declining here is what made `SUM(c_i64 * 3000000) + 1` float8:
			// the aggregate's own output was right, the walk OVER it was not,
			// so `__agg_0` was declared FLOAT64 in emittedColDecls and the
			// arithmetic above it took nodeDeclaredType's float fall-through.
			// A float64 cannot hold 36280278840510000001, so the `+ 1`
			// vanished and the answer came back BELOW the sum it was added to
			// (#867).
			if t, known := aggComputedInputOutputType(node, agg); known {
				return t, true
			}
			return unresolved()
		}
	}
	in, ok := aggInputColumnType(node, agg.InputCol)
	if !ok {
		return unresolved()
	}
	if decimalCapable {
		if in == parquet.TypeDecimal {
			// exec.HashAggregate fills in the precision and scale from the
			// vector it observes (outputSchema); the TYPE is what has to
			// agree between the two paths at plan time.
			return parquet.TypeDecimal, true
		}
		if t, ok := aggIntegerOutputType(fn, aggIntegerInputWidth(node, agg.InputCol, in)); ok {
			return t, true
		}
		if fn == "sum" && in == parquet.TypeFloat32 {
			// `pg_typeof(sum(real))` is real, and the accumulator now sums at
			// that width (kernel.Accumulator.SumF32, #760). Declaring double
			// over a float32 accumulator would be the mirror of the defect —
			// a wider OID on a narrower number.
			return parquet.TypeFloat32, true
		}
		return aggOutputType(agg.Func, agg.Distinct), true
	}
	if fn == "min_by" || fn == "max_by" {
		// The VALUE's type, for every type there is. MIN_BY/MAX_BY hand the
		// output vector the box GetValue produced for the winning row, so
		// the only declaration that can hold it is the input's own — see
		// exec.HashAggregate.outputSchema (#392). No switch here: a switch
		// is what fell through to FLOAT64 for the sixteen types outside it
		// and killed the process on the emit goroutine.
		return in, true
	}
	return minMaxDeclaredType(in), true
}

// aggComputedInputDecl types an aggregate whose ARGUMENT is an expression
// rather than a bare column, from that expression's own declaration over the
// aggregate's input columns. It is the declared-schema side of the rule the
// runtime already applies: plan.go's AggColumn override and the DAG's
// AggSpec both call aggOutputFromInputDecl with the projection's declared
// type of the computed argument, and this reads the same function from the
// same source so the DECLARATION and the VALUE cannot disagree (#867).
//
// ok=false when the aggregate has no input node to type, when the child is
// missing, or when the argument's own type is undecided — the caller then
// keeps whatever it declared before.
func aggComputedInputDecl(node *logical.Node, agg logical.AggExpr) (parquet.TypeID, int, int, bool) {
	if agg.InputExpr == nil || node == nil || len(node.Children) == 0 {
		return 0, 0, 0, false
	}
	decls := inputColDecls(node.Children[0])
	if len(decls.types) == 0 {
		decls = emittedColDecls(node.Children[0])
	}
	// A SCALAR SUBQUERY written AS the aggregate's argument — `SUM((SELECT
	// … ))` — has no column for the walk to read; its declaration is the
	// stamp on the plan (subquery_decl_annotation.go).
	decls = withSubqueryDecls(decls, node)
	d, c := nodeDeclaredType(agg.InputExpr, decls)
	if c == expr.Undecided {
		return 0, 0, 0, false
	}
	return aggOutputFromInputDecl(agg.Func, agg.Distinct, d.ID, d.Precision, d.Scale,
		aggInputIsWideInteger(agg.InputExpr, decls))
}

// aggComputedInputExprDecl is the declaration of the EXPRESSION an aggregate
// computes over, as opposed to aggComputedInputDecl's declaration of what the
// aggregate then ANSWERS. The two are the same walk and differ in one step:
// this one stops before aggOutputFromInputDecl.
//
// The bar needs the input's own type because its ROW fields ARE its inputs'
// types — `ohlcv(ts, price*2, volume)` declares open/high/low/close as
// `price*2`'s DECIMAL(20,4), the way MIN of that expression would. Without it
// the plan declined a computed bar entirely, and a declaration the plan
// declines is one every consumer invents differently (#965 round 2, B1).
func aggComputedInputExprDecl(node *logical.Node, agg logical.AggExpr) (parquet.TypeID, int, int, bool) {
	if agg.InputExpr == nil || node == nil || len(node.Children) == 0 {
		return 0, 0, 0, false
	}
	decls := inputColDecls(node.Children[0])
	if len(decls.types) == 0 {
		decls = emittedColDecls(node.Children[0])
	}
	// A SCALAR SUBQUERY written AS the aggregate's argument — `SUM((SELECT
	// … ))` — has no column for the walk to read; its declaration is the
	// stamp on the plan (subquery_decl_annotation.go).
	decls = withSubqueryDecls(decls, node)
	d, c := nodeDeclaredType(agg.InputExpr, decls)
	if c == expr.Undecided {
		return 0, 0, 0, false
	}
	return d.ID, d.Precision, d.Scale, true
}

// aggComputedInputOutputType is aggComputedInputDecl's TypeID-only face.
func aggComputedInputOutputType(node *logical.Node, agg logical.AggExpr) (parquet.TypeID, bool) {
	t, _, _, ok := aggComputedInputDecl(node, agg)
	return t, ok
}

// aggSpecOutputDecimal is aggSpecOutputType's companion for the one piece a
// bare TypeID cannot carry: MIN/MAX/MIN_BY/MAX_BY of a DECIMAL(p,s) column
// answers in that SAME (p,s) — it hands back a value the column already
// holds, not a computed one — so a zero-row result can declare it exactly
// the way declaredOutputSchema declares the type itself (#458).
//
// SUM/AVG widen or rescale their input rather than keeping it, but the
// WIDENING RULE itself is fixed at plan time, not decided by the
// accumulator at runtime: SUM keeps the input's scale and widens precision
// to the carrier's full width, AVG additionally widens the scale by
// batch.AvgScaleIncrement (batch.AvgScale) — exec.HashAggregate.
// decOutputParams computes the identical (precision, scale) from the
// vector it observes, so mirroring the formula here (rather than leaving
// it "unconstrained, precision 0") makes a zero-row SUM/AVG-over-DECIMAL
// result agree with a non-empty one internally, closing the divergence the
// #416 zero-row-schema regression suite could not see because it compared
// only NAME and TYPE, not (precision, scale) (fold-in to #457/#458, FIX 2).
// The WIRE typmod for these is a separate question, answered unconditionally
// -1 for every aggregate regardless of this function's answer — see
// declaredWireUnconstrainedDecimal.
func aggSpecOutputDecimal(node *logical.Node, agg logical.AggExpr) (logical.DecimalMeta, bool) {
	fn := strings.ToLower(strings.TrimSpace(agg.Func))
	switch fn {
	case "min", "max", "min_by", "max_by", "sum", "avg":
	default:
		return logical.DecimalMeta{}, false
	}
	if agg.InputExpr != nil {
		if _, bare := agg.InputExpr.(*plansql.ColRef); !bare {
			// aggSpecOutputType's rule, for the (p,s) half: a computed
			// argument declares what its own expression declares (#867).
			if t, prec, scale, known := aggComputedInputDecl(node, agg); known && t == parquet.TypeDecimal {
				return logical.DecimalMeta{Precision: prec, Scale: scale}, true
			}
			return logical.DecimalMeta{}, false
		}
	}
	// An INTEGER input whose result PostgreSQL answers in numeric declares the
	// carrier's full precision at scale 0 (SUM) or batch.AvgScale(0) (AVG) —
	// #784. It is asked BEFORE the DECIMAL lookup because the input is not a
	// DECIMAL column at all and aggInputColumnDecimal would decline it.
	if t, ok := aggInputColumnType(node, agg.InputCol); ok {
		if m, ok := aggIntegerOutputDecimal(fn, aggIntegerInputWidth(node, agg.InputCol, t)); ok {
			return m, true
		}
	}
	in, ok := aggInputColumnDecimal(node, agg.InputCol)
	if !ok {
		return logical.DecimalMeta{}, false
	}
	switch fn {
	case "sum":
		return logical.DecimalMeta{Precision: batch.MaxDecimalPrecision, Scale: in.Scale}, true
	case "avg":
		return logical.DecimalMeta{Precision: batch.MaxDecimalPrecision, Scale: batch.AvgScale(in.Scale)}, true
	default:
		return in, true
	}
}

// aggIntegerOutputType/aggIntegerOutputDecimal adapt exec.IntegerAccOutputType
// for function names and logical.DecimalMeta; all grouped/window declarations
// and runtime carriers must agree (#784, #685, #813).
// SUM(int4) is bigint, SUM(int8) and AVG of either are numeric; int64 overflow
// must be 22003, never wrap (ADR-0024 item 4).
// AVG uses batch.AvgScale(0)=4, independent of data magnitude (ADR-0024);
// comparison to PostgreSQL is exact to min(scale) (ADR-0012 item 9).
// Return ok=false for types outside the shared rule.
func aggIntegerOutputType(fn string, in parquet.TypeID) (parquet.TypeID, bool) {
	name := strings.ToLower(strings.TrimSpace(fn))
	if name != "sum" && name != "avg" {
		return 0, false
	}
	t, _, _, ok := exec.IntegerAccOutputType(name == "avg", in)
	return t, ok
}

func aggIntegerOutputDecimal(fn string, in parquet.TypeID) (logical.DecimalMeta, bool) {
	name := strings.ToLower(strings.TrimSpace(fn))
	if name != "sum" && name != "avg" {
		return logical.DecimalMeta{}, false
	}
	t, prec, scale, ok := exec.IntegerAccOutputType(name == "avg", in)
	if !ok || t != parquet.TypeDecimal {
		return logical.DecimalMeta{}, false
	}
	return logical.DecimalMeta{Precision: prec, Scale: scale}, true
}

// aggInputColumnType and aggInputColumnDecimal resolve both halves of a bare
// aggregate argument's declaration. Rename/derived-table outputs must be reachable
// when a scan does not carry the name (#728); a type and (p,s) must describe the
// same column. The lookup order below accounts for dispatch-respelled sources.
// See docs/internals/aggregate-argument-declaration-scope.md for the design.
func aggInputColumnType(node *logical.Node, col string) (parquet.TypeID, bool) {
	// The SCANS first; the emitted walk only for a name they do not carry.
	//
	// The ORDER is load-bearing, and it is ADR-0026's own rule: a name the DAG
	// has RE-SPELLED for dispatch is typed BELOW the rename chain, because that
	// is where the worker reads it. This function is asked TWICE for a derived
	// table that SHADOWS — once for the query's name and once for the dispatch
	// spelling — and over `(SELECT c_dec AS v, c_i32 AS c_dec FROM typemx) x`
	// the second question is "what is c_dec": the derived table answers INT32
	// and the SCAN answers DECIMAL(18,4). The worker reads the scan's.
	//
	// Preferring the emitted walk therefore declared INT32; one partial emitted
	// its SUM as INT64 while its siblings emitted DECIMAL, and ADR-0010's
	// shuffle type-consistency guard refused the read — eight shapes that
	// answered PostgreSQL's digits on the DAG became hard failures, on a branch
	// whose single-process half was fixing them.
	//
	// #728's rename case is untouched by the order: over
	// `(SELECT dw AS v FROM decwin) x` no scan below carries `v` at all, the
	// scan walk declines, and the emitted walk answers DECIMAL(38,10).
	if t, ok := scanColumnType(node, col); ok {
		return t, true
	}
	// namingScopeDecls, not emittedColDecls of the immediate child: the name
	// asked about here has ALREADY been re-spelled for dispatch, and a rename
	// Project directly above its scope answers for the ALIAS and not for it.
	// `SUM(w)` over `(SELECT id, SUM(a) OVER () AS w FROM decpair) x` arrives
	// as `SUM(__win_0)`, the Project below the aggregate emits `id` and `w`,
	// and the walk that stopped there found no `__win_0` — so the aggregate
	// declared FLOAT64 over a DECIMAL window slot, its output declared FLOAT64,
	// and the post-aggregate projection `__agg_0 * 2` met an exact DECIMAL at
	// the #361 store guard on both DAG arms (#775). Descending is gated on
	// coverage, so a name the Project DOES answer for still stops there and no
	// rebinding is read past.
	if node != nil && len(node.Children) == 1 {
		if decls, _, ok := namingScopeDecls(&plansql.ColRef{Column: col}, node.Children[0]); ok {
			if t, c := colRefDeclaredType(&plansql.ColRef{Column: col}, decls); c == expr.Decided {
				return t.ID, true
			}
		}
	}
	return 0, false
}

func aggInputColumnDecimal(node *logical.Node, col string) (logical.DecimalMeta, bool) {
	// The same order as aggInputColumnType, for the same reason: the two answer
	// one question about one column and a disagreement between them is a
	// DECIMAL declared with someone else's scale.
	if m, ok := scanColumnDecimal(node, col); ok {
		return m, true
	}
	if _, ok := scanColumnType(node, col); ok {
		// The scan carries the name and it is not a DECIMAL. Saying so is the
		// answer: falling through would let a derived table's SHADOW attach a
		// (p,s) to a column the worker reads as an integer.
		return logical.DecimalMeta{}, false
	}
	// The same descent aggInputColumnType makes, for the same reason and in the
	// same order: the two answer one question about one column, and a
	// disagreement between them is a DECIMAL declared with someone else's scale.
	if node != nil && len(node.Children) == 1 {
		if decls, _, ok := namingScopeDecls(&plansql.ColRef{Column: col}, node.Children[0]); ok {
			if t, c := colRefDeclaredType(&plansql.ColRef{Column: col}, decls); c == expr.Decided {
				if t.ID == parquet.TypeDecimal && t.DecKnown {
					return logical.DecimalMeta{Precision: t.Precision, Scale: t.Scale}, true
				}
			}
		}
	}
	return logical.DecimalMeta{}, false
}

// aggIntegerInputWidth is the type exec.IntegerAccOutputType must be asked
// about for a BARE aggregate argument: the column's declared PostgreSQL WIDTH,
// not the INT64 carrier it happens to be stored in.
//
// For a base column the two are the same fact, because the carrier IS the
// catalog's storage type. For a MATERIALIZED column they are not: every
// integer expression materializes as INT64 (ADR-0024's recorded widening), so
// `SELECT SUM(v) FROM (SELECT BITWISE_AND(id, 3) AS v FROM users) s` asked
// about INT64 and declared numeric, where the identical DIRECT call one level
// down declared bigint and PostgreSQL declares bigint. Same number, two boxes,
// on every arm and both wire formats (#1018 round 5, B1).
//
// This is the ONE reader of the declared width for a bare argument, and it is
// deliberately narrow: only an INT64 carrier can be hiding an int4 width, and
// only a declaration that SAYS int4 narrows it. Silence leaves the carrier
// alone.
func aggIntegerInputWidth(node *logical.Node, col string, carrier parquet.TypeID) parquet.TypeID {
	if carrier != parquet.TypeInt64 {
		return carrier
	}
	if w, ok := aggInputColumnIntWidth(node, col); ok && w == intWidth4 {
		return parquet.TypeInt32
	}
	return carrier
}

// aggInputColumnIntWidth resolves a bare aggregate argument's declared integer
// width, in exactly the order aggInputColumnType resolves its TYPE — the SCANS
// first, then the naming scope below the aggregate — because the two answer one
// question about one column and must not describe different ones.
//
// A name a SCAN carries is answered from the catalog and never from a derived
// table that shadows it, which is the same ADR-0026 rule that ordering exists
// for: the worker reads the scan's column.
func aggInputColumnIntWidth(node *logical.Node, col string) (intWidth, bool) {
	if t, ok := scanColumnType(node, col); ok {
		return catalogIntWidth(t), true
	}
	if node != nil && len(node.Children) == 1 {
		if decls, _, ok := namingScopeDecls(&plansql.ColRef{Column: col}, node.Children[0]); ok {
			if w, ok := decls.colIntWidth(&plansql.ColRef{Column: col}); ok {
				return w, true
			}
			if t, c := colRefDeclaredType(&plansql.ColRef{Column: col}, decls); c == expr.Decided {
				return catalogIntWidth(t.ID), true
			}
		}
	}
	return intWidthUnknown, false
}

// aggOutputFromInputDecl derives a computed argument's aggregate output from
// AggSpec.InputType/InputPrecision/InputScale, the same declaration used by
// worker.buildAggInputProjection. Empty-partial identity rows and non-empty
// partials must agree, even when the input triple is a FLOAT64 fallback (#685).
// Input-independent functions keep aggOutputType's answer. wideInt proves an
// int8-domain operand via aggInputIsWideInteger, preserving the by-width SUM rule
// after computed integer TypeIDs have widened to INT64.
// See docs/internals/computed-aggregate-output-declarations.md for the design.
func aggOutputFromInputDecl(fn string, distinct bool, in parquet.TypeID, precision, scale int, wideInt bool) (
	out parquet.TypeID, outPrecision, outScale int, ok bool,
) {
	name := strings.ToLower(strings.TrimSpace(fn))
	switch name {
	case "min", "max", "min_by", "max_by", "sum", "avg":
	default:
		return aggOutputType(fn, distinct), 0, 0, true
	}
	if in == parquet.TypeDecimal && precision > 0 {
		switch name {
		case "sum":
			return parquet.TypeDecimal, batch.MaxDecimalPrecision, scale, true
		case "avg":
			return parquet.TypeDecimal, batch.MaxDecimalPrecision, batch.AvgScale(scale), true
		default:
			// MIN/MAX/MIN_BY/MAX_BY hand back a value the input HELD, so they
			// keep its (p,s) — the same rule aggSpecOutputDecimal applies to a
			// bare column.
			return parquet.TypeDecimal, precision, scale, true
		}
	}
	switch name {
	case "sum", "avg":
		// Integer AVG is numeric; computed integer SUM uses bigint unless
		// aggInputIsWideInteger proves an int8-domain operand from AST and column
		// declarations (#784, #841). A shape the walk cannot see through keeps the
		// int4 reading, including Q12's CASE of ones; bare columns use real width.
		// Do not infer width from TypeID: every integer expression declares INT64
		// (ADR-0024's recorded divergence).
		if _, ok := aggIntegerOutputType(name, in); ok {
			if name == "avg" {
				m, _ := aggIntegerOutputDecimal("avg", parquet.TypeInt32)
				return parquet.TypeDecimal, m.Precision, m.Scale, true
			}
			if wideInt {
				m, _ := aggIntegerOutputDecimal("sum", parquet.TypeInt64)
				return parquet.TypeDecimal, m.Precision, m.Scale, true
			}
			return parquet.TypeInt64, 0, 0, true
		}
		return aggOutputType(fn, distinct), 0, 0, true
	case "min_by", "max_by":
		return in, 0, 0, true
	default:
		return minMaxDeclaredType(in), 0, 0, true
	}
}

// aggSpecInputDecimal is the (p,s) of a bare DECIMAL COLUMN argument — the
// declaration the aggregate READS, as opposed to the one aggSpecOutputDecimal
// says it writes. Any aggregate, not only the six above: the pair describes the
// column, not the function.
//
// It exists because AVG is not dispatched as AVG: decomposeAvg splits it into
// SUM and COUNT legs, and the SUM leg declares the INPUT's scale where AVG
// declares batch.AvgScale of it. That increment saturates at the carrier's 38
// digits, so AVG's own declaration cannot be inverted back to the input's for
// a scale of 34 or more — the leg has to be told (#685).
//
// Declines for a computed argument, where the derived-expression branch types
// the projection instead (AggSpec.InputType/InputPrecision/InputScale).
func aggSpecInputDecimal(node *logical.Node, agg logical.AggExpr) (logical.DecimalMeta, bool) {
	if agg.InputExpr != nil {
		if _, bare := agg.InputExpr.(*plansql.ColRef); !bare {
			return logical.DecimalMeta{}, false
		}
	}
	// aggInputColumnDecimal, not scanColumnDecimal: the same walk, in the same
	// order, that aggSpecOutputType asks for the input's TYPE. Two functions
	// answering one question about one column with two different walks is
	// ADR-0023 item 5 one layer over, and the disagreement is a DECIMAL
	// declared with nobody's scale — for a WINDOW SLOT (`SUM(__win_0)`), whose
	// declaration lives in the emitted walk and in no scan at all, the scan-only
	// walk answered (0,0) (#775).
	return aggInputColumnDecimal(node, agg.InputCol)
}

// minMaxDeclaredType maps a MIN/MAX input column type to the output type
// exec.HashAggregate emits for it. It mirrors exec.minMaxOutputType, whose
// "0" answer means "keep what the planner declared" — float64 — so that
// case is spelled out here as float64 rather than propagated as undeclared.
func minMaxDeclaredType(in parquet.TypeID) parquet.TypeID {
	switch in {
	case parquet.TypeString:
		return parquet.TypeString
	case parquet.TypeBytes:
		return parquet.TypeBytes
	case parquet.TypeDate:
		return parquet.TypeDate
	case parquet.TypeTimestamp:
		return parquet.TypeTimestamp
	case parquet.TypeIPv4:
		return parquet.TypeIPv4
	case parquet.TypeIPv6:
		return parquet.TypeIPv6
	case parquet.TypeCIDR:
		return parquet.TypeCIDR
	case parquet.TypeUUID:
		return parquet.TypeUUID
	case parquet.TypeMAC:
		return parquet.TypeMAC
	case parquet.TypePort:
		return parquet.TypePort
	case parquet.TypeProtocol:
		return parquet.TypeProtocol
	case parquet.TypeDuration:
		return parquet.TypeDuration
	case parquet.TypeBool:
		return parquet.TypeBool
	case parquet.TypeInt64, parquet.TypeInt32:
		return parquet.TypeInt64
	case parquet.TypeFloat32:
		// MIN/MAX of a REAL is a value the column HOLDS, so it answers in
		// REAL — `pg_typeof(min(real))` is real on the live server. The
		// accumulator already carried the exact value (a float32 widened to a
		// float64 is exact); only the declaration said double precision, so a
		// client read a Double for a column that is a Float everywhere else
		// in the query (#760).
		return parquet.TypeFloat32
	case parquet.TypeDecimal:
		// MIN/MAX of a DECIMAL is a value the column HOLDS, so it answers in
		// DECIMAL — the accumulator was always an exact Int128 and only the
		// declaration said float64, which is where the digits went (#455).
		return parquet.TypeDecimal
	case parquet.TypeArray, parquet.TypeRow, parquet.TypeMap, parquet.TypeVector:
		// A container MIN/MAX answers with an input VALUE (#426), so its
		// declaration is the input's own — the MIN_BY rule above. A
		// FLOAT64 declaration over one of these is the #392 shape: the
		// output vector cannot hold the box at all.
		return in
	}
	return parquet.TypeFloat64
}

// scanColumnType resolves a column name to its catalog type by searching
// the scans below node (ScanColTypes, populated by AnnotateScanColumns).
// A qualified name matches on its bare suffix, since a scan's schema
// carries unqualified names. Two scans that disagree on the type — a
// self-join is not the only way to reach one — report not-found rather
// than picking a side.
func scanColumnType(node *logical.Node, col string) (parquet.TypeID, bool) {
	if node == nil || col == "" {
		return 0, false
	}
	if dot := strings.LastIndexByte(col, '.'); dot >= 0 {
		col = col[dot+1:]
	}
	col = strings.ToLower(col)
	var found parquet.TypeID
	ok := false
	var walk func(n *logical.Node) bool
	walk = func(n *logical.Node) bool {
		if n == nil {
			return true
		}
		if t, present := n.ScanColTypes[col]; present {
			if ok && t != found {
				return false
			}
			found, ok = t, true
		}
		for _, c := range n.Children {
			if !walk(c) {
				return false
			}
		}
		return true
	}
	if !walk(node) {
		return 0, false
	}
	return found, ok
}

// scanColumnDecimal resolves a DECIMAL column name to its (precision, scale)
// by searching the scans below node (ScanColDecimal, populated by
// AnnotateScanColumns) — the same walk as scanColumnType, for the one piece
// of a DECIMAL column's declaration a bare TypeID cannot carry (#458). Two
// scans that disagree report not-found, same as scanColumnType.
func scanColumnDecimal(node *logical.Node, col string) (logical.DecimalMeta, bool) {
	if node == nil || col == "" {
		return logical.DecimalMeta{}, false
	}
	if dot := strings.LastIndexByte(col, '.'); dot >= 0 {
		col = col[dot+1:]
	}
	col = strings.ToLower(col)
	var found logical.DecimalMeta
	ok := false
	var walk func(n *logical.Node) bool
	walk = func(n *logical.Node) bool {
		if n == nil {
			return true
		}
		if m, present := n.ScanColDecimal[col]; present {
			if ok && m != found {
				return false
			}
			found, ok = m, true
		}
		for _, c := range n.Children {
			if !walk(c) {
				return false
			}
		}
		return true
	}
	if !walk(node) {
		return logical.DecimalMeta{}, false
	}
	return found, ok
}

// hasAggregateAncestor checks if a node is an Aggregate, or if it's a
// passthrough node (e.g., Filter for HAVING) whose child is an Aggregate.
func hasAggregateAncestor(node *logical.Node) bool {
	return findAggregateAncestor(node) != nil
}

// aggregateOutputNames returns the column names the pipeline under a
// projection emits, in order, when that pipeline ends in an Aggregate, and
// reports whether they could be determined at all. It mirrors buildAggregate's
// own naming: group keys first (a non-plain GROUP BY expression under its
// `__gb_expr_N` synthetic name), then one column per aggregate under its
// OutputCol, then the constant columns litPostOps re-attaches for the literal
// keys elided from the key set.
//
// "Could not determine" is not a failure: the caller uses this to decide
// whether a projection is redundant, and an undetermined answer keeps the
// projection.
//
// The names are the PLANNER's spelling of each key — `x.a` for `GROUP BY x.a`.
// That is not what the operator publishes; `aggregateEmittedOutputNames` is.
// The redundancy check wants this one: a spelling that is wider than the
// emitted name can only fail to match, and failing to match keeps the
// projection, which is always sound.
func aggregateOutputNames(node *logical.Node) ([]string, bool) {
	return aggregateOutputNameList(node, false)
}

// aggregateEmittedOutputNames is aggregateOutputNames with the GROUP-BY half
// stated the way the aggregate OPERATOR states it: `exec.PublishedGroupKeyNames`
// over the same published list `buildAggregate` hands `exec.NewHashAggregate`,
// which strips a key's relation qualifier and keeps it only where stripping
// would make two KEYS collide.
//
// The distinction is the whole of #968. `SELECT x.a AS b, SUM(x.b) AS a
// FROM decpair x GROUP BY x.a` emits TWO columns called `a` — the key, whose
// qualifier the operator strips, and the aggregate, whose OutputCol is the
// user's alias — and `batch.RecordBatch.ColumnIndex` answers with the first.
// The #575 slot pinning exists for exactly that collision and never saw it,
// because it asked this list for `x.a` and found no duplicate: the projection
// stayed on the name path and `SUM(x.b) AS a` returned the group key's value
// under the aggregate's alias and the KEY's declared type, on the
// single-process arms, silently.
//
// Only a consumer that needs a physical SLOT asks this. A consumer that asks
// "could another column answer to this NAME" wants the resolver's rule, which
// strips the qualifier unconditionally (exec.columnIndexFallback).
func aggregateEmittedOutputNames(node *logical.Node) ([]string, bool) {
	return aggregateOutputNameList(node, true)
}

func aggregateOutputNameList(node *logical.Node, emitted bool) ([]string, bool) {
	if node == nil {
		return nil, false
	}
	switch {
	case node.Type != logical.NodeProject && node.Type != logical.NodeAggregate &&
		logical.AggScopePreservingWrapper(node.Type):
		// A HAVING filter, a Sort, a LIMIT and a WINDOW all leave the
		// aggregate's own output columns visible under their own names —
		// ADR-0026 §4's list, read from `logical.AggScopePreservingWrapper`
		// rather than restated, because restating it is what this function was.
		//
		// It descended NodeFilter ALONE while its own guard at the #575 call
		// site is `findAggregateAncestor`, which reads the full list. With a
		// WINDOW between the aggregate and the SELECT list the guard said "yes,
		// an aggregate is below" and this said "I cannot model that", so the
		// duplicate-name slot pinning was skipped and two same-named outputs
		// collapsed onto the FIRST: `SELECT COUNT(*) AS g, g AS x, ROW_NUMBER()
		// OVER (ORDER BY g) FROM collslot GROUP BY g` answered the KEY's value
		// under the aggregate's alias on the single-process path, where
		// PostgreSQL 17 answers the counts (the DAG was right).
		if len(node.Children) == 0 {
			return nil, false
		}
		return aggregateOutputNameList(node.Children[0], emitted)
	case node.Type == logical.NodeProject:
		// Only the synthetic finalization projections findAggregateAncestor
		// walks through; a real one is the pipeline's output already.
		if !node.PreservesAggOutputs {
			return nil, false
		}
		names := make([]string, 0, len(node.Projections))
		for i := range node.Projections {
			names = append(names, projectionOutputName(node.Projections[i]))
		}
		return names, true
	case node.Type == logical.NodeAggregate:
		// Grouping sets renumber the key columns and add null-group
		// bookkeeping; not modeled here.
		if len(node.GroupingSets) > 0 || len(node.GroupingSetNulls) > 0 {
			return nil, false
		}
		// groupKeyOutputs states buildAggregate's own naming rule — which
		// keys the input already carries, which one of the paths has to
		// materialize, and which literal is elided and re-attached. Read
		// from there rather than restated here: when this list and the
		// aggregate's real output disagree, the projection-elision decision
		// publishes the wrong columns (#568, #590).
		names := make([]string, 0, len(node.GroupBy)+len(node.AggExprs))
		var elidedLits []string
		keyOuts := groupKeyOutputs(node)
		elided := map[int]bool{}
		for i, k := range keyOuts {
			if k.Literal {
				elided[i] = true
				elidedLits = append(elidedLits, k.Name)
				continue
			}
			names = append(names, k.Name)
		}
		if emitted {
			// buildAggregate's own two lines: the derived/delimited/minted
			// keys carry a planner-decided name as GroupByOutNames, and every
			// other key takes exec's rule. Passing the all-empty list when
			// nothing is derived is the same input a nil GroupByOutNames is.
			over, _ := publishedGroupKeyNames(keyOuts, elided)
			names = exec.PublishedGroupKeyNames(names, over, false)
		}
		for i := range node.AggExprs {
			names = append(names, node.AggExprs[i].OutputCol)
		}
		return append(names, elidedLits...), true
	}
	return nil, false
}

// wrapsAWindow reports whether a WINDOW stands between this node and the
// aggregate below it, walking the same list aggregateOutputNames does.
//
// A window APPENDS its output, so "the aggregate's output names" and "this
// node's output names" are two different lists there. Only the second answers
// whether a projection may be ELIDED.
func wrapsAWindow(n *logical.Node) bool {
	for ; n != nil; n = n.Children[0] {
		if n.Type == logical.NodeWindow {
			return true
		}
		if n.Type == logical.NodeAggregate || !logical.AggScopePreservingWrapper(n.Type) ||
			len(n.Children) != 1 {
			return false
		}
	}
	return false
}

// projectionOutputName is the column name a projection publishes, resolved the
// same way buildProject's own ProjectColumn naming resolves it.
func projectionOutputName(proj logical.Projection) string {
	name := proj.Alias
	if name == "" {
		name = proj.Column
	}
	if name == "" {
		name = cleanExpr(proj.Expr)
	}
	return plansql.NormalizeIdentRef(strings.TrimSpace(name))
}

// namesMatchProjections reports whether an aggregate's output columns are
// exactly the projected ones, in order. Names are compared case-insensitively:
// output names come from SQL identifiers, which the rest of the engine matches
// that way.
func namesMatchProjections(names []string, projections []logical.Projection) bool {
	if len(names) != len(projections) {
		return false
	}
	for i := range names {
		if !strings.EqualFold(plansql.NormalizeIdentRef(strings.TrimSpace(names[i])),
			projectionOutputName(projections[i])) {
			return false
		}
	}
	return true
}

// findAggregateAncestor returns the Aggregate node if the given node is one,
// or traverses through the nodes that leave the aggregate's own output columns
// visible to find it: a HAVING Filter, a Sort, a LIMIT, a WINDOW
// (aggScopePreservingWrapper), and a synthetic finalization Project.
//
// It is the single-process half of the walk `aggregateUnderOutput` performs
// for the gather, and the two read one list so they cannot disagree about a
// node kind — which is exactly how a window between the SELECT list and the
// aggregate made a computed group key NULL on BOTH paths (#737).
func findAggregateAncestor(node *logical.Node) *logical.Node {
	if node.Type == logical.NodeAggregate {
		return node
	}
	if aggScopePreservingWrapper(node.Type) && len(node.Children) == 1 {
		return findAggregateAncestor(node.Children[0])
	}
	// Synthetic finalization projections (two-level AVG) pass every
	// aggregate output through by name, so SELECT-list resolution treats
	// the aggregate below as directly visible.
	if node.Type == logical.NodeProject && node.PreservesAggOutputs && len(node.Children) > 0 {
		return findAggregateAncestor(node.Children[0])
	}
	return nil
}
