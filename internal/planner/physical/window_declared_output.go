// This file holds window declared output for the physical planner, governed by ADR-0024 and ADR-0026.
package physical

import (
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"strconv"
	"strings"
)

// windowOutputType declares the output type of an INPUT-INDEPENDENT window
// function — the rank family, whose answer is a position or a ratio computed
// from the frame, plus COUNT, which finalizes to int64 whatever it consumed.
//
// SUM and AVG reach this list only as a FALLBACK. Over a DECIMAL they answer
// DECIMAL, exactly as the grouped forms do (#586, ADR-0012 item 9), and
// windowSpecOutputType resolves that from the input column; the float64 here
// is what every other numeric input still gets, and what an input the planner
// could not type at all falls back to.
//
// The value functions — lag, lead, first_value, last_value, nth_value — are
// NOT here: they return a value taken from their input column rather than
// computing one, so their output type IS that column's type and no name list
// can know it. Declaring them float64 typed the window's output vector
// numeric while the value path wrote strings, and exec.Window (unlike
// exec.Project) had no runtime correction, so every string write was dropped
// for the integer 0 (#345). windowSpecOutputType resolves them instead.
//
// MIN/MAX over a window were the last input-dependent family answered from
// this list, and landed on the float64 default: MIN(a_string) OVER (...)
// and MIN(int32_col) OVER (...) had #345's symptom for the same reason
// (#361). They resolve from the input column like the value functions, and
// since #569 for EVERY type the engine has — exec.WindowMinMaxType names
// them all, so what still reaches this list from a MIN/MAX is only an input
// type the planner could not resolve at all.
func windowOutputType(funcName string) parquet.TypeID {
	switch strings.ToLower(funcName) {
	case "row_number", "rank", "dense_rank", "count", "ntile":
		return parquet.TypeInt64
	case "percent_rank", "cume_dist":
		return parquet.TypeFloat64
	default:
		return parquet.TypeFloat64
	}
}

// windowValueFunc reports whether fn returns a value lifted out of its input
// column instead of computing one. Kept as its own predicate because the same
// five names decide the type question in the planner and the re-typing
// question in exec.Window.
func windowValueFunc(fn string) bool {
	switch fn {
	case "lag", "lead", "first_value", "last_value", "nth_value":
		return true
	}
	return false
}

// windowSpecOutputType declares the output type of one window expression over
// the subtree rooted at the Window node that owns it. It is to windowOutputType
// what aggSpecOutputType is to aggOutputType (#329, #333): the name list
// answers everything that is input-independent, and the input column answers
// the rest.
//
// The value functions copy a value out of their argument column, so the
// argument's catalog type is the answer. It is resolved through
// inputColTypes — the Window node's own input schema, which stops at anything
// that can rebind a name — and colRefDeclaredType, so a parameterized type
// (DECIMAL without its scale, VECTOR without its dimension, the nested types)
// declines the same way it does for a projection.
//
// UNDECIDABLE cases fall back to windowOutputType's float64, which is exactly
// today's behavior: a computed argument (`FIRST_VALUE(a || b)`), a column no
// scan below annotates, two scans that disagree, or an input the walk cannot
// describe at all. A confidently wrong type here is worse than the fallback —
// nothing downstream corrects a declaration, which is the whole of #345.
// windowComputedArgDecl types a window aggregate's COMPUTED argument from the
// argument's own AST, and reports whether that expression carries an
// int8-domain operand. It is aggComputedInputDecl's window face and asks the
// same two functions — nodeDeclaredType and aggInputIsWideInteger — over the
// same declarations, because the two spellings of one aggregate have to reach
// the same type.
//
// Both halves are needed and neither is enough alone. The DECLARATION cannot
// tell int4 arithmetic from int8 arithmetic: every integer expression in this
// engine computes in int64 (ADR-0024's recorded widening), so `w_i32 * 1` and
// `w_i64 * 1` both declare INT64. The WIDTH walk cannot tell an integer
// expression from a float one: it answers "not wide" for both. Together they
// say what PostgreSQL says — `sum(int4-domain)` is bigint, `sum(int8-domain)`
// is numeric, and anything that is not an integer keeps the float64 fallback.
//
// Three guards keep it to the shapes it can see:
//
//   - a BARE column declines here and is typed by colRefDeclaredType above:
//     an int4 column already declares INT32, which IntegerAccOutputType
//     answers directly.
//   - no node, or an undecided expression, declines. Unknown keeps the
//     existing fallback rather than narrowing on a guess.
//   - the node must still SPELL the argument the operator will evaluate.
//     respellOverAggregate rewrites InputCol when a window sits above an
//     aggregate, and a rewritten argument resolves its ColRefs against names
//     the stale AST does not carry — so a mismatch declines rather than
//     typing a spelling that no longer applies.
//
// #987 review B1: `SUM(CASE WHEN … THEN 1 ELSE 0 END) OVER ()` — TPC-H Q12's
// shape, bigint in PostgreSQL and bigint in the grouped spelling here — went
// out as DECIMAL(38,0) under OID 1700 where its grouped twin went out under
// 20. One question, two spellings, two boxes.
func windowComputedArgDecl(node *logical.Node, we logical.WindowExpr) (expr.DeclType, bool, bool) {
	if we.InputExpr == nil || node == nil || len(node.Children) == 0 {
		return expr.DeclType{}, false, false
	}
	if _, bare := we.InputExpr.(*plansql.ColRef); bare {
		return expr.DeclType{}, false, false
	}
	if cleanExpr(we.InputExpr.String()) != cleanExpr(we.InputCol) {
		return expr.DeclType{}, false, false
	}
	decls := inputColDecls(node.Children[0])
	if len(decls.types) == 0 {
		decls = emittedColDecls(node.Children[0])
	}
	d, c := nodeDeclaredType(we.InputExpr, decls)
	if c == expr.Undecided {
		return expr.DeclType{}, false, false
	}
	return d, aggInputIsWideInteger(we.InputExpr, decls), true
}

// integerAccArgWidth maps a computed argument's DECLARED type plus the width
// walk's verdict onto the input width exec.IntegerAccOutputType answers about.
// An int64-carried expression that provably holds no int8-domain operand is
// PostgreSQL's int4 case; everything else is its own declaration.
func integerAccArgWidth(declared parquet.TypeID, wide bool) parquet.TypeID {
	if declared == parquet.TypeInt64 && !wide {
		return parquet.TypeInt32
	}
	return declared
}

func windowSpecOutputType(node *logical.Node, we logical.WindowExpr) expr.DeclType {
	fn := strings.ToLower(strings.TrimSpace(we.Func))
	minMax := fn == "min" || fn == "max"
	sumAvg := fn == "sum" || fn == "avg"
	if !windowValueFunc(fn) && !minMax && !sumAvg {
		return expr.Decl(windowOutputType(fn))
	}
	// The same spelling buildWindow hands exec as the input column, so the
	// declaration always describes the vector the operator will read.
	col := cleanExpr(we.InputColumn())
	if col == "" || len(node.Children) != 1 {
		return expr.Decl(windowOutputType(fn))
	}
	// colRefDeclaredType declines every PARAMETERIZED type (DECIMAL without
	// its scale, VECTOR without its dimension, the nested types), so those
	// keep the float64 fallback here and are corrected at runtime instead:
	// exec.Window.retypeValueColumns re-declares from the input vector and
	// exec.windowOutputColumn carries the (p,s)/element/field metadata with
	// it. A ZERO-ROW result has no such vector and is described from this
	// declaration alone, which is why `MIN(dec_col) OVER (...)` matching no
	// row still describes itself float8 while the same query matching rows
	// describes itself numeric — tracked in #587, not fixable by widening
	// colRefDeclaredType, whose decline exists for projections that have no
	// runtime correction at all.
	//
	// inputColDecls, not inputColTypes: it carries the ROW columns' FIELDS
	// too, so a windowed value function or MIN/MAX over a field path
	// (`MIN(rw.f_i64) OVER ()`) resolves the field's type here instead of
	// defaulting to float64 (#568). A field path of a parameterized type
	// still declines and rides the same runtime correction as a column.
	// emittedColDecls, not inputColDecls: the walk that CROSSES a derived
	// table's Project instead of stopping at it (#529's walk, ADR-0026 §5).
	// A window one nesting level above its scan —
	// `SUM(a) OVER () FROM (SELECT id, a FROM t) u` — resolved NOTHING here
	// and fell to the float64 fallback, so the same window that declares
	// numeric directly over the table declared float8 through a derived
	// table, and an aggregate reading it inherited the float box on every
	// arm where PostgreSQL answers numeric (#796). It is the same walk the
	// aggregate's own argument (aggInputDecls) and declaredOutputSchema
	// already use, which is what makes the window's declaration, the
	// aggregate's above it and the wire's agree through a nesting level.
	t, conf := colRefDeclaredType(&plansql.ColRef{Column: col}, emittedColDecls(node.Children[0]))
	if conf != expr.Decided {
		// A COMPUTED argument has no column declaration to read: the
		// pre-window projection materializes `w_i32 * 1` under that name and
		// colRefDeclaredType declines every name it cannot find in a scan.
		// The GROUPED spelling does not stop here — aggComputedInputDecl
		// types the argument from its own AST — so until #987's review this
		// was the whole of B1: `SUM(CASE WHEN … THEN 1 ELSE 0 END) OVER ()`
		// fell to float8 at plan time and was corrected to DECIMAL(38,0) at
		// runtime, while its grouped twin declared bigint, which is
		// PostgreSQL's type.
		if sumAvg {
			if d, wide, ok := windowComputedArgDecl(node, we); ok {
				if out, prec, scale, iok := exec.IntegerAccOutputType(
					fn == "avg", integerAccArgWidth(d.ID, wide)); iok {
					if out == parquet.TypeDecimal {
						return expr.DeclDecimal(prec, scale)
					}
					return expr.Decl(out)
				}
			}
		}
		return expr.Decl(windowOutputType(fn))
	}
	if sumAvg {
		// SUM and AVG do NOT copy an input value, so their declaration is
		// not the input's: they accumulate, and over a DECIMAL they answer
		// what the GROUPED SUM/AVG answer — DECIMAL(38,s) and
		// DECIMAL(38,min(s+4,38)), exactly (#586, #475, ADR-0012 item 9,
		// ADR-0024 item 2). `SUM(d) GROUP BY g` and `SUM(d) OVER (PARTITION
		// BY g)` are the same question written twice; a client that reads
		// both in one result set was getting numeric for one and float8 for
		// the other, with the window's digits past a float64's ~16 already
		// gone.
		//
		// An INTEGER input answers PostgreSQL's own result type — bigint for
		// sum(int4), numeric for sum(int8) and for avg of either — through
		// exec.IntegerAccOutputType, the SAME function the grouped
		// aggregate's declaration asks (aggIntegerOutputType) and the same
		// one the operator's runtime correction asks
		// (exec.windowAccOutputType). Until #987 this fell to float8 while
		// the grouped spelling was exact, so the two spellings of one
		// question disagreed about the type AND, past 2^53, about the digits
		// — an order-dependent total from a float64 accumulator (#813,
		// ADR-0012's divergence list, now deleted).
		//
		// Every other input type keeps the float64 the name list answers.
		if t.ID != parquet.TypeDecimal || !t.DecKnown {
			// t is a COLUMN's declaration here — a computed argument does
			// not resolve through colRefDeclaredType and is typed in the
			// undecided arm above, by windowComputedArgDecl.
			out, prec, scale, ok := exec.IntegerAccOutputType(fn == "avg", t.ID)
			if !ok {
				return expr.Decl(windowOutputType(fn))
			}
			if out == parquet.TypeDecimal {
				return expr.DeclDecimal(prec, scale)
			}
			return expr.Decl(out)
		}
		prec, scale := exec.WindowDecimalAggMeta(parseWindowFunc(fn), t.Scale)
		return expr.DeclDecimal(prec, scale)
	}
	if minMax {
		// MIN/MAX copy an input value through the same GetValue/SetValue
		// route as the value functions, so the declaration is the input's
		// own — for every type, since #569. exec.WindowMinMaxType is asked
		// rather than assumed so the planner and the operator cannot come to
		// different conclusions about a type; !ok keeps the float64
		// fallback, as an unresolvable input type does above.
		out, ok := exec.WindowMinMaxType(t.ID)
		if !ok {
			return expr.Decl(windowOutputType(fn))
		}
		if out == parquet.TypeDecimal {
			// MIN/MAX of a DECIMAL is that DECIMAL, (p,s) and all — which
			// is what the operator actually emits (exec.windowOutputColumn
			// carries the input vector's Precision/Scale). Before ADR-0024
			// this branch could not be reached, because colRefDeclaredType
			// declined every DECIMAL argument, so a ZERO-ROW result — which
			// is described from this declaration alone, with no vector to
			// re-type from — went out as float8 where the same query over
			// rows went out as numeric (#587).
			return t
		}
		return expr.Decl(out)
	}
	return t
}

// windowExecColumn resolves one logical WindowExpr into the executable
// column spec, over the Window node that owns it.
//
// It is the single place window arguments are read: the column out of the
// argument list, the offset/default/N that share it, the frame, and the
// output type. Both consumers go through it — the single-process pipeline
// (buildWindow) builds exec.WindowColumn directly, and walkStages copies the
// resolved values into the stage spec the DAG ships to workers. A worker has
// no catalog and no logical plan, so a second implementation there would be a
// second answer; the arguments are parsed once, here, where the types resolve
// (#345's shape, and #329/#333's).
// windowInputCol is the name a window function's ARGUMENT reaches the operator
// under. It is `cleanExpr`'s bare spelling everywhere except where dropping the
// qualifier would leave a name more than one arm of the window's input
// publishes, and there it is the qualified spelling the query wrote — see
// windowArgKeepsItsQualifier for why the strip is a coin toss in exactly that
// case and for the two paths it landed on opposite sides of.
func windowInputCol(node *logical.Node, we logical.WindowExpr) string {
	arg := strings.TrimSpace(we.InputColumn())
	if len(node.Children) == 1 && windowArgKeepsItsQualifier(arg, node.Children[0]) {
		return arg
	}
	return cleanExpr(arg)
}

func windowExecColumn(node *logical.Node, we logical.WindowExpr, keys map[string]windowKey) exec.WindowColumn {
	// resolveWindowKeys binds a qualified reference to the input column and
	// renames an expression to the column the pre-window projection computes
	// under; a term it left alone keeps its own spelling (#585).
	keyName := func(term string) string {
		if k, ok := keys[strings.TrimSpace(term)]; ok {
			return k.Name
		}
		return term
	}
	var orderKeys []exec.SortKey
	for _, ob := range we.OrderBy {
		order := exec.Ascending
		if ob.Desc {
			order = exec.Descending
		}
		orderKeys = append(orderKeys, exec.SortKey{
			Column:    keyName(ob.Column),
			Order:     order,
			NullsLast: resolveNullsLast(ob),
			// A WINDOW reads its keys by NAME off the input batch, and a name
			// stops being an address the moment the producer emits it twice.
			// `SELECT x.a AS b, SUM(x.b) AS a, RANK() OVER (ORDER BY SUM(x.b))
			// … GROUP BY x.a` re-spells the window's term to `a` — which the
			// aggregate below publishes for its KEY as well — and the rank
			// came back in the key's order beside a correct sum, on every arm
			// (#968). The CLASS travels on the term (`NamesAggregateOutput`,
			// set where respellOverAggregate rewrote it) and the POSITION is
			// read here, from the same `aggregateEmittedOutputNames` model the
			// projection and the sort key use.
			SlotPos: windowOrderKeySlot(node, keyName(ob.Column), ob.NamesAggregateOutput),
		})
	}
	partBy := make([]string, len(we.PartitionBy))
	for i, pb := range we.PartitionBy {
		partBy[i] = keyName(pb)
	}
	fn := strings.ToLower(we.Func)
	wc := exec.WindowColumn{
		Func: parseWindowFunc(we.Func),
		// InputColumn drops the offset/default/N that share the
		// argument string, so cleanExpr sees a column reference and
		// nothing else. Applied the other way round, a float default
		// (LAG(x, 1, 1.5)) looked like a qualified name and cleanExpr
		// returned "5" as the input column.
		InputCol:    windowInputCol(node, we),
		OutputCol:   we.OutputCol,
		OutputType:  windowSpecOutputType(node, we).ID,
		PartitionBy: partBy,
		OrderBy:     orderKeys,
	}
	// A ROW FIELD PATH argument is materialized like a key and read under the
	// synthetic name: `SUM(rw.f) OVER ()` reached exec as the bare `f`, found
	// no input vector, and answered NULL in every row (#603). The output type
	// follows the FIELD for the functions whose answer is one of their input's
	// values — windowSpecOutputType could not resolve it, because the name it
	// types is the field's, which is a column of nothing.
	if k, ok := keys[strings.TrimSpace(we.InputColumn())]; ok && k.Expr != nil {
		wc.InputCol = k.Name
		if windowValueFunc(fn) {
			wc.OutputType = k.Type
		} else if fn == "min" || fn == "max" {
			if out, vetted := exec.WindowMinMaxType(k.Type); vetted {
				wc.OutputType = out
			}
		}
	}
	if we.Frame != nil {
		wc.Frame = &exec.WindowFrameSpec{
			Mode:  we.Frame.Mode,
			Start: exec.WindowBound{Type: we.Frame.Start.Type, Offset: we.Frame.Start.Offset},
			End:   exec.WindowBound{Type: we.Frame.End.Type, Offset: we.Frame.End.Offset},
		}
	}
	// Parse the function-specific arguments out of the rest of the
	// argument string (WindowExpr.InputCol carries the whole list verbatim).
	if fn == "ntile" {
		if n, err := strconv.Atoi(strings.TrimSpace(we.InputCol)); err == nil {
			wc.NtileBuckets = n
		}
	} else if fn == "nth_value" {
		if parts := strings.SplitN(we.InputCol, ",", 2); len(parts) >= 2 {
			if n, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
				wc.NthValueN = n
			}
		}
	} else if fn == "lag" || fn == "lead" {
		parts := strings.SplitN(we.InputCol, ",", 3)
		if len(parts) >= 2 {
			if offset, err := strconv.Atoi(strings.TrimSpace(parts[1])); err == nil {
				wc.LagLeadOffset = offset
			}
		}
		if len(parts) >= 3 {
			defStr := strings.TrimSpace(parts[2])
			if v, err := strconv.ParseFloat(defStr, 64); err == nil {
				wc.LagLeadDefault = v
			} else {
				// A string default arrives as SQL source, quotes and
				// all; passing it through wrote 'none' — with the
				// quotes — into the result column.
				if len(defStr) >= 2 && strings.HasPrefix(defStr, "'") && strings.HasSuffix(defStr, "'") {
					defStr = strings.ReplaceAll(defStr[1:len(defStr)-1], "''", "'")
				}
				wc.LagLeadDefault = defStr
			}
		}
	}
	return wc
}
