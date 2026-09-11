// This file holds window declared output for the physical planner, governed by ADR-0024 and ADR-0026.
package physical

import (
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// windowOutputType declares input-independent rank/ratio functions and COUNT
// (int64 regardless of input). Other names use the float64 fallback.
// windowSpecOutputType resolves input-dependent value functions and MIN/MAX
// from their argument, and SUM/AVG through accumulator typing; DECIMAL matches
// the grouped result (#586, ADR-0012 item 9). Unresolved inputs keep fallback.
// Value functions must copy the input type (#345); MIN/MAX use
// exec.WindowMinMaxType for every supported type (#361, #569).
// See docs/internals/window-function-result-type-dispatch.md for the design.
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

// windowComputedArgDecl types the argument AST and checks int8-domain operands
// with nodeDeclaredType and aggInputIsWideInteger over the same declarations as
// grouped aggregates (#329, #333, #987; ADR-0024). Both answers are required:
// integer expressions compute in int64; SUM(int4-domain) is bigint, SUM(int8-domain)
// is numeric, and non-integers retain fallback. Bare columns, missing/undecided
// nodes and AST/InputCol spelling mismatches decline, never guess after respelling.
// windowSpecOutputType resolves input-dependent types in the owning window's
// schema; rebinding and unavailable parameter metadata bound lookup (#345).
// Undecidable arguments keep windowOutputType's fallback.
// See docs/internals/computed-window-argument-declarations.md for the design.
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
	decls := withSubqueryDecls(inputColDecls(node.Children[0]), node)
	if len(decls.types) == 0 {
		decls = withSubqueryDecls(emittedColDecls(node.Children[0]), node)
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

// windowBareArgWidth is aggIntegerInputWidth for the WINDOW spelling: the
// width exec.IntegerAccOutputType is asked about for a BARE argument column,
// read off the declaration the input publishes rather than off the INT64
// carrier every integer expression materializes in.
func windowBareArgWidth(decls colDecls, col string, carrier parquet.TypeID) parquet.TypeID {
	if carrier != parquet.TypeInt64 {
		return carrier
	}
	if w, ok := decls.colIntWidth(&plansql.ColRef{Column: col}); ok && w == intWidth4 {
		return parquet.TypeInt32
	}
	return carrier
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
	// Resolve through emittedColDecls so derived Projects and ROW field metadata
	// reach the same declaration used by aggregates and the wire (#529, #568,
	// #796; ADR-0026 §5). Parameterized types require their metadata.
	// Where colRefDeclaredType declines, keep fallback; runtime value-column
	// retyping carries (p,s)/element/field metadata from the input vector.
	// Zero-row results have no vector and depend solely on this declaration (#587);
	// projection callers cannot rely on window runtime correction.
	// See docs/internals/window-input-declaration-through-derived-plans.md for the design.
	inDecls := emittedColDecls(node.Children[0])
	t, conf := colRefDeclaredType(&plansql.ColRef{Column: col}, inDecls)
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
		if minMax {
			// MIN/MAX COPY a value, so the answer is the ARGUMENT's own type
			// whether the argument is a column or an expression — and the
			// expression's type is the same `windowComputedArgDecl` the
			// accumulating arm above reads. Without this the slot fell to
			// float8: `SUM(m)` over a derived `MIN(BITWISE_AND(id,3)) OVER ()`
			// declared OID 701 where PostgreSQL declares bigint, and the
			// int8-argument twin declared 701 where it declares numeric —
			// a declaration that is not even in the integer family, so the
			// width attribute could not be recorded for it at all (#1018
			// round 5 review, P1). exec.WindowMinMaxType is asked rather than
			// assumed, exactly as the decided-column arm below asks it, so
			// the planner and the operator cannot disagree about a type.
			if d, _, ok := windowComputedArgDecl(node, we); ok {
				if out, vetted := exec.WindowMinMaxType(d.ID); vetted {
					if out == parquet.TypeDecimal && d.DecKnown {
						return d
					}
					return expr.Decl(out)
				}
			}
		}
		return expr.Decl(windowOutputType(fn))
	}
	if sumAvg {
		// SUM/AVG accumulate rather than copy input: DECIMAL results match grouped
		// SUM DECIMAL(38,s) and AVG DECIMAL(38,min(s+4,38)) exactly
		// (#586, #475; ADR-0012 item 9, ADR-0024 item 2).
		// Integer results use exec.IntegerAccOutputType, shared by grouped declaration
		// and window runtime correction: SUM(int4) is bigint, SUM(int8) and AVG of
		// either are numeric (#987, #813; ADR-0012).
		// Every other input retains the name list's float64 fallback.
		// See docs/internals/window-accumulator-result-declarations.md for the design.
		if t.ID != parquet.TypeDecimal || !t.DecKnown {
			// t is a COLUMN's declaration here — a computed argument does
			// not resolve through colRefDeclaredType and is typed in the
			// undecided arm above, by windowComputedArgDecl.
			// The DECLARED width, not the INT64 carrier: a derived table's
			// `BITWISE_AND(id, 3) AS v` is an int4 column in an int64 box,
			// and `SUM(v) OVER ()` over it declares bigint in PostgreSQL.
			// The grouped spelling asks aggIntegerInputWidth for the same
			// fact, from the same map (#1018 round 5, B1).
			out, prec, scale, ok := exec.IntegerAccOutputType(fn == "avg",
				windowBareArgWidth(inDecls, col, t.ID))
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
