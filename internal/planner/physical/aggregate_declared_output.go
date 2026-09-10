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

// aggSpecOutputType declares the output type of one aggregate over the
// subtree rooted at the Aggregate node that owns it.
//
// COUNT is input-independent in this engine, so aggOutputType alone is exact
// for it, and it is the same declaration the single-process pipeline compiles
// into exec.AggColumn.OutputType. SUM and AVG are input-independent for every
// type but DECIMAL, over which they answer in DECIMAL (#455).
//
// MIN/MAX are the exception, and MIN_BY/MAX_BY with them: their output IS
// their (first) input's type, which exec.HashAggregate resolves from the
// vector it observes at Consume. To declare the same thing at plan time the
// input column has to resolve to a catalog type, so this walks the
// aggregate's inputs for it and returns 0 — undeclared — when it cannot: a
// derived-expression argument, a column no scan below carries, or two scans
// carrying it at different types.
// ok=false is the undeclared answer; callers fall back to the
// function-name derivation. It is returned as a second value rather than
// as a zero TypeID because TypeBool IS zero: MIN_BY over a BOOL column
// declares BOOL, and a caller reading that as "undeclared" is how a
// declaration goes missing on exactly one path (#354, #371).
// aggOhlcvOutputFields declares a bar's ROW fields from the PRICE and VOLUME
// column declarations, through exec.OhlcvOutputFields — the same function the
// operator asks at Consume, so the declaration and the value are one decision.
//
// ok=false when either input is not a bare column this subtree can type (a
// computed argument, a name no scan below carries, two scans disagreeing).
// The operator then re-derives the list from the vectors it reads, which is
// exactly what aggSpecOutputType's "unresolved" answer does for MIN/MAX.
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
		if t, ok := aggIntegerOutputType(fn, in); ok {
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
		if m, ok := aggIntegerOutputDecimal(fn, t); ok {
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

// aggIntegerOutputType and aggIntegerOutputDecimal are PostgreSQL's result
// types for SUM/AVG over an INTEGER column, taken from the live server (#784):
//
//	pg_typeof(sum(int4)) -> bigint     pg_typeof(sum(int8)) -> numeric
//	pg_typeof(avg(int4)) -> numeric    pg_typeof(avg(int8)) -> numeric
//
// The two SUM rules differ because int4's sum has a wider integer type to grow
// into and int8's does not; a wadjet SUM(int8) in int64 WRAPS past 2^63, which
// ADR-0024 item 4 makes a 22003 rather than an answer. They mirror
// exec.aggIntExact, which decides the CARRIER the accumulator uses, and the
// two must agree: a plan that declares numeric over an int64 accumulator is
// the #685 shape (a partial's identity row contradicting its siblings).
//
// AVG's SCALE is batch.AvgScale(0) = 4, the same +4 rule a DECIMAL input takes.
// PostgreSQL's own numeric division picks a MAGNITUDE-DEPENDENT scale —
// measured on the server, avg(c_i32) renders 16 fraction digits and
// avg(c_i64) renders 8, both targeting about 16-20 significant digits — which
// ADR-0024 rejected as a rule precisely because the same query over more rows
// would change the scale of its own output column. Both engines are exact to
// the digits they keep and agree to min(scale): ADR-0012 item 9's class.
//
// ok=false for every type these rules do not name.
//
// The RULE itself is exec.IntegerAccOutputType, not this pair. The same
// question is asked by the WINDOW's plan-time declaration
// (windowSpecOutputType), by the window operator's runtime correction of it
// (exec.windowAccOutputType) and by exec.aggIntExact, and two tables are two
// chances for the grouped and the windowed spelling of one query to answer
// under different types — which is exactly what #813 was. These two are the
// planner's ADAPTERS onto that table: the function NAME rather than a bool,
// and logical.DecimalMeta rather than a bare (p,s).
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

// aggInputColumnType and aggInputColumnDecimal answer "what does this
// aggregate's bare column argument declare" for the two halves of a
// declaration, and they answer it against the aggregate's OWN INPUT before
// falling back to the scans below it.
//
// scanColumnType/scanColumnDecimal search every Scan beneath a node for a
// column of that NAME, which cannot see a RENAME: over
// `SUM(v) FROM (SELECT dw AS v FROM decw) x` the scan carries `dw` and
// nothing carries `v`, so the aggregate's output declared FLOAT64 while the
// accumulator held an exact Int128 — and `SUM(v * 2)`, which is that
// declaration times two in the projection ABOVE the aggregate, came back
// -1.7283950641728393e+19 where the same query spelled over the base column
// answers -17283950641728394664.17283948 (#728). Two spellings of one
// question, two numbers, on the single-process path.
//
// emittedColDecls is the walk that CROSSES a rename or derived-table Project
// — the same walk buildAggregate already uses for a COMPUTED argument
// (aggInputDecls) and declaredOutputSchema uses for the SELECT list — so the
// aggregate's input, its output and the projection above it now read one map.
// It is consulted FIRST rather than as a fallback because where the two
// disagree the emitted walk is the right one: a Project is free to bind a
// name to a different column than the scan of that name below it
// (`SELECT dw AS other, k AS dw`), and the scan walk would answer for the
// column the query is NOT reading.
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

// aggOutputFromInputDecl is aggSpecOutputType/aggSpecOutputDecimal's rule for a
// COMPUTED argument: the aggregate's declared output, derived from the
// declaration its INPUT PROJECTION is built from.
//
// It exists because an aggregate over a computed argument had no declared
// output at all — aggSpecOutputType declines a non-bare ColRef and falls to
// aggOutputType's float64 — while the partials that saw a row emitted whatever
// the projected vector actually was. On the stage DAG those are the same file
// set: a partial whose filter matched nothing writes the identity row under the
// float64 default, its siblings write DECIMAL, and one stage's files then
// describe two different relations. Before #685's reader guard that was a
// silent 10^scale on SUM(a * (1 - b)) — the TPC-H revenue shape — and after it,
// a refused read. Neither is an answer.
//
// The derivation is BY CONSTRUCTION rather than by inference, which is what
// makes it total: the worker builds the pre-aggregate projection from
// AggSpec.InputType/InputPrecision/InputScale (worker.buildAggInputProjection),
// so the vector every non-empty partial observes IS this declaration. Reading
// the output off the same triple means the identity row and its siblings agree
// whatever the triple says — including when it is the float64 fallback for an
// expression nothing could type.
//
// ok=false only for a function whose output does not follow its input at all;
// those keep aggOutputType's answer, which is already input-independent.
// wideInt says the computed integer argument provably carries an int8-domain
// operand (aggInputIsWideInteger). It is what lets this function apply
// PostgreSQL's by-WIDTH SUM rule to an expression whose declared TypeID this
// engine has already widened to INT64.
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
		// An INTEGER input follows PostgreSQL's own types (#784), with ONE
		// narrowing for a COMPUTED argument: SUM is bigint whatever the
		// integer width, and only AVG becomes numeric.
		//
		// PostgreSQL's SUM rule is by INPUT WIDTH — int4 grows into bigint,
		// int8 has nothing wider and becomes numeric — and wadjet declares
		// EVERY integer expression INT64 (ADR-0024's recorded divergence), so
		// a computed argument cannot tell the two apart. Reading them all as
		// int8 would make `SUM(CASE WHEN … THEN 1 ELSE 0 END)` — TPC-H Q12's
		// shape and a BI staple — numeric where PostgreSQL says bigint, on a
		// sum of ones that cannot overflow anything. Reading them as int4
		// keeps PostgreSQL's OID for that shape; the residual is a computed
		// int8 sum past 2^63, which is the pre-existing "every integer
		// spelling is INT64" divergence and not a new one.
		//
		// A BARE COLUMN is not affected: aggSpecOutputType answers it from the
		// column's real width, where int8 is int8.
		//
		// The residual that paragraph names is CLOSED for every expression
		// whose width can be READ (#841): aggInputIsWideInteger walks the AST
		// and the column declarations and answers "this carries an int8-domain
		// operand", which is exactly what PostgreSQL's rule needs. A shape it
		// cannot see through keeps the int4 reading, so Q12's CASE of ones is
		// still bigint and nothing moves on a shape nobody can point at.
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
