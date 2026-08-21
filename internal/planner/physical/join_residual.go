package physical

import (
	"log/slog"
	"math"
	"strconv"
	"strings"
	"sync"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// BuildJoinResidualFilter compiles an outer join's ON-clause residual — every
// conjunct that is not an equi-join key pair — into a predicate over the
// COMBINED row: the probe row plus one candidate build row (#358).
//
// An outer join's ON runs BEFORE the NULL-padding, so this residual cannot be
// a filter above the join (that deletes the preserved rows) and cannot be
// pushed into a preserved side's scan (that deletes the rows the join owes
// unmatched). The executor evaluates it per key-matched candidate; see
// exec.HashJoin.Residual for the unmatched semantics it feeds.
//
// BuildSemiAntiFilter is not reusable here: it only expresses
// `probeCol OP buildCol` and ignores NULLs, while a residual must take
// literals (`r.r_regionkey < 3`), arithmetic (`n.x = r.y + 3`) and SQL
// three-valued logic (a residual evaluating to NULL rejects the candidate,
// but NOT of it must not accept). This is a small AST interpreter instead:
// per-row and boxed, which is acceptable for a capability the planner
// previously refused outright — no existing plan shape gains this code path.
//
// Column resolution against the two sides is by name, decided lazily on the
// first evaluated pair and cached: a qualified name is looked up verbatim in
// the probe then the build schema (self-join chains carry qualified columns);
// a qualifier equal to buildAlias forces the build side; otherwise the bare
// name resolves probe-first. An unresolvable column makes every evaluation
// UNKNOWN (candidate rejected) and logs once — the planner ships JoinFilter
// columns through NeededColumns, so a miss here is a plan bug, not user error.
//
// Returns nil when the expression contains a shape the interpreter does not
// support; the caller must then refuse the plan loudly rather than drop the
// conjunct.
func BuildJoinResidualFilter(filter, buildAlias string) func(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool {
	expr := parseJoinCondExpr(filter)
	if expr == nil || !residualSupported(expr) {
		return nil
	}
	buildAlias = strings.ToLower(buildAlias)

	// Column bindings, resolved once against the first pair's schemas.
	type colBinding struct {
		fromBuild bool
		idx       int
	}
	bindings := map[*plansql.ColRef]*colBinding{}
	collectResidualColRefs(expr, func(c *plansql.ColRef) {
		bindings[c] = &colBinding{}
	})
	var resolveOnce sync.Once

	resolve := func(probe, build *batch.RecordBatch) {
		for c, b := range bindings {
			col := strings.ToLower(c.Column)
			if c.Table != "" {
				qual := strings.ToLower(c.Table) + "." + col
				if idx := probe.ColumnIndex(qual); idx >= 0 {
					b.fromBuild, b.idx = false, idx
					continue
				}
				if idx := build.ColumnIndex(qual); idx >= 0 {
					b.fromBuild, b.idx = true, idx
					continue
				}
				if strings.ToLower(c.Table) == buildAlias {
					b.fromBuild, b.idx = true, build.ColumnIndex(col)
					continue
				}
			}
			if idx := probe.ColumnIndex(col); idx >= 0 {
				b.fromBuild, b.idx = false, idx
				continue
			}
			b.fromBuild, b.idx = true, build.ColumnIndex(col)
			if b.idx < 0 {
				slog.Warn("join residual column resolves on neither side — every candidate will be rejected",
					"column", c.String(), "filter", filter)
			}
		}
	}

	var eval func(n plansql.Node, probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) resVal
	eval = func(n plansql.Node, probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) resVal {
		switch e := n.(type) {
		case *plansql.ParenNode:
			return eval(e.Inner, probe, probeRow, build, buildRow)
		case *plansql.ColRef:
			b := bindings[e]
			if b == nil || b.idx < 0 {
				return resNull
			}
			if b.fromBuild {
				return residualValue(build.Columns[b.idx], buildRow)
			}
			return residualValue(probe.Columns[b.idx], probeRow)
		case *plansql.Lit:
			switch e.Kind {
			case plansql.LitNull:
				return resNull
			case plansql.LitString:
				return resVal{kind: resStr, str: e.Value}
			case plansql.LitBool:
				return resVal{kind: resBool, b: strings.EqualFold(e.Value, "true")}
			default: // LitNumber
				if i, err := strconv.ParseInt(e.Value, 10, 64); err == nil {
					return resVal{kind: resNum, i: i, f: float64(i), isInt: true}
				}
				f, err := strconv.ParseFloat(e.Value, 64)
				if err != nil {
					return resNull
				}
				return resVal{kind: resNum, f: f}
			}
		case *plansql.UnaryOp:
			v := eval(e.Inner, probe, probeRow, build, buildRow)
			if v.kind != resNum {
				return resNull
			}
			if e.Op == "-" {
				return resVal{kind: resNum, i: -v.i, f: -v.f, isInt: v.isInt}
			}
			return v
		case *plansql.BinaryOp:
			return residualArith(
				eval(e.Left, probe, probeRow, build, buildRow),
				e.Op,
				eval(e.Right, probe, probeRow, build, buildRow))
		case *plansql.CmpExpr:
			return residualCompare(
				eval(e.Left, probe, probeRow, build, buildRow),
				e.Op,
				eval(e.Right, probe, probeRow, build, buildRow))
		case *plansql.IsExpr:
			v := eval(e.Left, probe, probeRow, build, buildRow)
			switch strings.ToLower(e.Check) {
			case "null":
				return resVal{kind: resBool, b: (v.kind == resNullKind) != e.Not}
			case "true":
				return resVal{kind: resBool, b: (v.kind == resBool && v.b) != e.Not}
			case "false":
				return resVal{kind: resBool, b: (v.kind == resBool && !v.b) != e.Not}
			}
			return resNull
		case *plansql.AndNode:
			l := eval(e.Left, probe, probeRow, build, buildRow)
			r := eval(e.Right, probe, probeRow, build, buildRow)
			return residual3VL(l, r, true)
		case *plansql.OrNode:
			l := eval(e.Left, probe, probeRow, build, buildRow)
			r := eval(e.Right, probe, probeRow, build, buildRow)
			return residual3VL(l, r, false)
		case *plansql.NotNode:
			v := eval(e.Inner, probe, probeRow, build, buildRow)
			if v.kind != resBool {
				return resNull
			}
			return resVal{kind: resBool, b: !v.b}
		}
		return resNull
	}

	return func(probe *batch.RecordBatch, probeRow int, build *batch.RecordBatch, buildRow int) bool {
		resolveOnce.Do(func() { resolve(probe, build) })
		v := eval(expr, probe, probeRow, build, buildRow)
		// SQL ON semantics: a residual that is FALSE or UNKNOWN rejects.
		return v.kind == resBool && v.b
	}
}

// residualSupported walks the AST and reports whether every node is one the
// interpreter evaluates. Anything else (function calls, CASE, IN, BETWEEN,
// LIKE, subqueries, casts) makes the caller refuse the plan — loud, exactly
// as before this capability existed — rather than mis-evaluate.
func residualSupported(n plansql.Node) bool {
	switch e := n.(type) {
	case *plansql.ParenNode:
		return residualSupported(e.Inner)
	case *plansql.ColRef, *plansql.Lit:
		return true
	case *plansql.UnaryOp:
		return residualSupported(e.Inner)
	case *plansql.BinaryOp:
		return residualSupported(e.Left) && residualSupported(e.Right)
	case *plansql.CmpExpr:
		return residualSupported(e.Left) && residualSupported(e.Right)
	case *plansql.IsExpr:
		return residualSupported(e.Left)
	case *plansql.AndNode:
		return residualSupported(e.Left) && residualSupported(e.Right)
	case *plansql.OrNode:
		return residualSupported(e.Left) && residualSupported(e.Right)
	case *plansql.NotNode:
		return residualSupported(e.Inner)
	}
	return false
}

// collectResidualColRefs visits every ColRef in the (already validated) AST.
func collectResidualColRefs(n plansql.Node, visit func(*plansql.ColRef)) {
	switch e := n.(type) {
	case *plansql.ParenNode:
		collectResidualColRefs(e.Inner, visit)
	case *plansql.ColRef:
		visit(e)
	case *plansql.UnaryOp:
		collectResidualColRefs(e.Inner, visit)
	case *plansql.BinaryOp:
		collectResidualColRefs(e.Left, visit)
		collectResidualColRefs(e.Right, visit)
	case *plansql.CmpExpr:
		collectResidualColRefs(e.Left, visit)
		collectResidualColRefs(e.Right, visit)
	case *plansql.IsExpr:
		collectResidualColRefs(e.Left, visit)
	case *plansql.AndNode:
		collectResidualColRefs(e.Left, visit)
		collectResidualColRefs(e.Right, visit)
	case *plansql.OrNode:
		collectResidualColRefs(e.Left, visit)
		collectResidualColRefs(e.Right, visit)
	case *plansql.NotNode:
		collectResidualColRefs(e.Inner, visit)
	}
}

// resVal is the interpreter's value: NULL, a number (int-ness tracked so
// integer division truncates the way the engine's `/` does, #369), a string,
// or a boolean.
type resVal struct {
	kind  byte
	i     int64
	f     float64
	isInt bool
	str   string
	b     bool
}

const (
	resNullKind byte = iota
	resNum
	resStr
	resBool
)

var resNull = resVal{kind: resNullKind}

// residualValue reads one cell as a resVal. GetValue already resolves views
// and NULL bitmaps; DATE boxes as its ISO string, which compares correctly
// against both another DATE and a date literal.
func residualValue(v *batch.Vector, row int) resVal {
	switch x := v.GetValue(row).(type) {
	case nil:
		return resNull
	case int64:
		return resVal{kind: resNum, i: x, f: float64(x), isInt: true}
	case int32:
		return resVal{kind: resNum, i: int64(x), f: float64(x), isInt: true}
	case float64:
		return resVal{kind: resNum, f: x}
	case float32:
		return resVal{kind: resNum, f: float64(x)}
	case bool:
		return resVal{kind: resBool, b: x}
	case string:
		// A decimal column boxes as its formatted string; residual arithmetic
		// and numeric comparison need the number back.
		if v.Type == batch.TypeDecimal {
			if f, err := strconv.ParseFloat(x, 64); err == nil {
				return resVal{kind: resNum, f: f}
			}
		}
		return resVal{kind: resStr, str: x}
	case []byte:
		return resVal{kind: resStr, str: string(x)}
	}
	return resNull
}

// residualArith evaluates l op r with NULL propagation. Integer inputs stay
// integer through + - * % and truncating / (PostgreSQL semantics, ADR-0012);
// any float operand promotes the result to float. || concatenates strings.
func residualArith(l resVal, op string, r resVal) resVal {
	if op == "||" {
		if l.kind == resStr && r.kind == resStr {
			return resVal{kind: resStr, str: l.str + r.str}
		}
		return resNull
	}
	if l.kind != resNum || r.kind != resNum {
		return resNull
	}
	if l.isInt && r.isInt {
		switch op {
		case "+":
			return resVal{kind: resNum, i: l.i + r.i, f: float64(l.i + r.i), isInt: true}
		case "-":
			return resVal{kind: resNum, i: l.i - r.i, f: float64(l.i - r.i), isInt: true}
		case "*":
			return resVal{kind: resNum, i: l.i * r.i, f: float64(l.i * r.i), isInt: true}
		case "/":
			if r.i == 0 {
				return resNull
			}
			q := l.i / r.i // Go truncates toward zero, same as PostgreSQL
			return resVal{kind: resNum, i: q, f: float64(q), isInt: true}
		case "%":
			if r.i == 0 {
				return resNull
			}
			m := l.i % r.i
			return resVal{kind: resNum, i: m, f: float64(m), isInt: true}
		}
		return resNull
	}
	switch op {
	case "+":
		return resVal{kind: resNum, f: l.f + r.f}
	case "-":
		return resVal{kind: resNum, f: l.f - r.f}
	case "*":
		return resVal{kind: resNum, f: l.f * r.f}
	case "/":
		if r.f == 0 {
			return resNull
		}
		return resVal{kind: resNum, f: l.f / r.f}
	case "%":
		if r.f == 0 {
			return resNull
		}
		return resVal{kind: resNum, f: math.Mod(l.f, r.f)}
	}
	return resNull
}

// residualCompare evaluates l op r under SQL comparison rules: NULL on either
// side is UNKNOWN, and a type mismatch is UNKNOWN rather than false so that a
// NOT above it stays UNKNOWN too.
func residualCompare(l resVal, op string, r resVal) resVal {
	if l.kind == resNullKind || r.kind == resNullKind || l.kind != r.kind {
		return resNull
	}
	var cmp int
	switch l.kind {
	case resNum:
		switch {
		case l.isInt && r.isInt:
			cmp = cmpOrdered(l.i, r.i)
		default:
			cmp = cmpOrdered(l.f, r.f)
		}
	case resStr:
		cmp = strings.Compare(l.str, r.str)
	case resBool:
		li, ri := 0, 0
		if l.b {
			li = 1
		}
		if r.b {
			ri = 1
		}
		cmp = cmpOrdered(li, ri)
	default:
		return resNull
	}
	var out bool
	switch op {
	case "=":
		out = cmp == 0
	case "!=", "<>":
		out = cmp != 0
	case "<":
		out = cmp < 0
	case "<=":
		out = cmp <= 0
	case ">":
		out = cmp > 0
	case ">=":
		out = cmp >= 0
	default:
		return resNull
	}
	return resVal{kind: resBool, b: out}
}

func cmpOrdered[T int | int64 | float64](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// residual3VL folds two operands under AND (isAnd) or OR with SQL
// three-valued logic: FALSE AND UNKNOWN is FALSE, TRUE OR UNKNOWN is TRUE,
// and the rest of the UNKNOWN row stays UNKNOWN.
func residual3VL(l, r resVal, isAnd bool) resVal {
	lb, lok := l.kind == resBool && l.b, l.kind == resBool
	rb, rok := r.kind == resBool && r.b, r.kind == resBool
	if isAnd {
		if (lok && !lb) || (rok && !rb) {
			return resVal{kind: resBool, b: false}
		}
		if lok && rok {
			return resVal{kind: resBool, b: true}
		}
		return resNull
	}
	if (lok && lb) || (rok && rb) {
		return resVal{kind: resBool, b: true}
	}
	if lok && rok {
		return resVal{kind: resBool, b: false}
	}
	return resNull
}
