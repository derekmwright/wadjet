// This file holds expression helpers for the physical planner, governed by ADR-0026 and ADR-0027.
package physical

import (
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
)

func parseSimplePredicate(raw string) exec.UnaryOperator {
	// Parse "column op value" patterns
	operators := []struct {
		sql string
		op  exec.CompareOp
	}{
		{">=", exec.OpGe},
		{"<=", exec.OpLe},
		{"!=", exec.OpNe},
		{">", exec.OpGt},
		{"<", exec.OpLt},
		{"=", exec.OpEq},
	}

	for _, o := range operators {
		parts := strings.SplitN(raw, o.sql, 2)
		if len(parts) == 2 {
			col := cleanExpr(strings.TrimSpace(parts[0]))
			valStr := strings.TrimSpace(parts[1])
			val := parseValue(valStr)
			return kernelOrNothing(col, o.op, val, numericLitText(valStr))
		}
	}

	// LIKE / NOT LIKE
	upper := strings.ToUpper(raw)
	if idx := strings.Index(upper, " NOT LIKE "); idx >= 0 {
		col := cleanExpr(strings.TrimSpace(raw[:idx]))
		pattern := strings.TrimSpace(raw[idx+len(" NOT LIKE "):])
		pattern = strings.Trim(pattern, "'")
		return exec.NewLikeFilter(col, pattern, true)
	}
	if idx := strings.Index(upper, " LIKE "); idx >= 0 {
		col := cleanExpr(strings.TrimSpace(raw[:idx]))
		pattern := strings.TrimSpace(raw[idx+len(" LIKE "):])
		pattern = strings.Trim(pattern, "'")
		return exec.NewLikeFilter(col, pattern, false)
	}

	// IS NULL / IS NOT NULL — vectorized null bitmap scan
	if strings.Contains(upper, "IS NOT NULL") {
		col := cleanExpr(strings.TrimSpace(raw[:strings.Index(upper, "IS NOT NULL")]))
		return exec.NewNullCheckFilter(col, false)
	}
	if strings.Contains(upper, "IS NULL") {
		col := cleanExpr(strings.TrimSpace(raw[:strings.Index(upper, "IS NULL")]))
		return exec.NewNullCheckFilter(col, true)
	}

	// BETWEEN: "col between X and Y" → col >= X AND col <= Y
	if idx := strings.Index(upper, " BETWEEN "); idx >= 0 {
		col := cleanExpr(strings.TrimSpace(raw[:idx]))
		rest := strings.TrimSpace(raw[idx+len(" BETWEEN "):])
		andIdx := strings.Index(strings.ToUpper(rest), " AND ")
		if andIdx >= 0 {
			loStr := strings.TrimSpace(rest[:andIdx])
			hiStr := strings.TrimSpace(rest[andIdx+len(" AND "):])
			lo, hi := parseValue(loStr), parseValue(hiStr)
			return exec.NewChainFilter([]exec.UnaryOperator{
				kernelOrNothing(col, exec.OpGe, lo, numericLitText(loStr)),
				kernelOrNothing(col, exec.OpLe, hi, numericLitText(hiStr)),
			})
		}
	}

	// IN: "col in (v1, v2, v3)" → vectorized set membership
	if idx := strings.Index(upper, " IN "); idx >= 0 {
		col := cleanExpr(strings.TrimSpace(raw[:idx]))
		rest := strings.TrimSpace(raw[idx+len(" IN "):])
		rest = strings.TrimPrefix(rest, "(")
		rest = strings.TrimSuffix(rest, ")")
		parts := strings.Split(rest, ",")
		values := make([]any, 0, len(parts))
		texts := make([]string, 0, len(parts))
		for _, part := range parts {
			part = strings.TrimSpace(part)
			values = append(values, parseValue(part))
			texts = append(texts, numericLitText(part))
		}
		if len(values) > 0 {
			return inFilterForList(col, values, texts, false)
		}
	}

	return nil
}

// numericLitText returns a raw predicate operand's text when it is a plain
// decimal number, and "" otherwise. It is what lets a DECIMAL comparison
// built from raw SQL text keep the digits the float64 box drops (#452);
// exponent forms are deliberately not included, so they keep the box.
func numericLitText(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	i := 0
	if s[0] == '+' || s[0] == '-' {
		i++
	}
	digits, dot := false, false
	for ; i < len(s); i++ {
		switch {
		case s[i] >= '0' && s[i] <= '9':
			digits = true
		case s[i] == '.' && !dot:
			dot = true
		default:
			return ""
		}
	}
	if !digits {
		return ""
	}
	return s
}

func parseValue(s string) any {
	s = strings.TrimSpace(s)
	// Remove quotes
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	// An UNQUOTED null is the NULL literal. Falling through returned the
	// four-character string "null", so `WHERE c = NULL` reaching this path
	// compared the column against that text instead of answering UNKNOWN;
	// the callers turn nil into a match-nothing operator (#450).
	if strings.EqualFold(s, "null") {
		return nil
	}
	// Try integer
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i
	}
	// Try float
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}

func parseCompareOp(op string) exec.CompareOp {
	switch op {
	case "=":
		return exec.OpEq
	case "!=", "<>":
		return exec.OpNe
	case "<":
		return exec.OpLt
	case "<=":
		return exec.OpLe
	case ">":
		return exec.OpGt
	case ">=":
		return exec.OpGe
	default:
		return exec.OpEq
	}
}

func parseAggFunc(s string) exec.AggFunc {
	switch strings.ToLower(s) {
	case "sum":
		return exec.AggSum
	case "count":
		return exec.AggCount
	case "min":
		return exec.AggMin
	case "max":
		return exec.AggMax
	case "avg":
		return exec.AggAvg
	case "string_agg":
		return exec.AggStringAgg
	case "bool_and", "every":
		return exec.AggBoolAnd
	case "bool_or":
		return exec.AggBoolOr
	case "stddev", "stddev_samp":
		return exec.AggStddev
	case "variance", "var_samp":
		return exec.AggVariance
	case "stddev_pop":
		return exec.AggStddevPop
	case "var_pop":
		return exec.AggVarPop
	case "approx_distinct":
		return exec.AggApproxDistinct
	case "corr":
		return exec.AggCorr
	case "covar_samp":
		return exec.AggCovarSamp
	case "covar_pop":
		return exec.AggCovarPop
	case "percentile_cont", "quantile_cont":
		return exec.AggPercentileCont
	case "percentile_disc", "quantile_disc":
		return exec.AggPercentileDisc
	case "mode":
		return exec.AggMode
	case "ohlcv":
		return exec.AggOhlcv
	case exec.OhlcvStateFunc:
		return exec.AggOhlcvState
	case exec.OhlcvStateMergeFunc:
		return exec.AggOhlcvStateMerge
	case "min_by":
		return exec.AggMinBy
	case "max_by":
		return exec.AggMaxBy
	case "median":
		return exec.AggMedian
	default:
		return exec.AggCount
	}
}

// parseWindowFunc maps a SQL window function name onto its operator constant.
// A name exec has no window form for still resolves to ROW_NUMBER — the zero
// value — but no plan reaches the operator with one, because refuseUnwindowable
// runs first at every site that builds a window operator.
func parseWindowFunc(s string) exec.WindowFunc {
	fn, _ := exec.ParseWindowFunc(s)
	return fn
}

// refuseUnwindowable fails the plan for a window expression whose function has
// no window form, with the SAME sentence the worker's fragment builder raises
// (exec.RefuseUnsupportedWindowFunc). Before it, such a plan reached
// exec.Window as ROW_NUMBER with a mis-typed output vector and PANICKED —
// 23 of the 28 known aggregates did, reported as "internal error in pipeline"
// with no SQLSTATE (#965's census; see exec.RefuseUnsupportedWindowFunc).
func refuseUnwindowable(exprs []logical.WindowExpr) error {
	for _, we := range exprs {
		if _, ok := exec.ParseWindowFunc(we.Func); !ok {
			return exec.RefuseUnsupportedWindowFunc(we.Func)
		}
	}
	return nil
}

// wrapExpr adapts an expr.Expr into an exec.Expression function.
func wrapExpr(e expr.Expr) exec.Expression {
	return func(b *batch.RecordBatch, row int) any {
		return e.Eval(b, row)
	}
}

// wrapPredicate adapts an expr.Expr into an exec.Predicate function. The
// typed protocol and its two-valued collapse are chosen once, in
// expr.FilterPredicate, so the row loop neither boxes nor re-dispatches.
func wrapPredicate(e expr.Expr) exec.Predicate {
	return expr.FilterPredicate(e)
}

// limitPushdownSafe reports whether a LIMIT may be applied independently by
// each task under it.
//
// It may when every node between the LIMIT and its scans passes rows through
// one at a time: Project and Filter qualify (a filtered task simply reaches n
// later, or never), and a scan is the base case. Anything that derives rows
// from more than one input row — join, aggregate, distinct, sort, window, set
// operation — does not: bounding its INPUT changes its OUTPUT, which would
// silently produce wrong answers rather than merely fewer rows.
//
// Multiple scans under a UNION ALL are fine: each bounds itself, and the
// coordinator trims the union to n.
func limitPushdownSafe(node *logical.Node) bool {
	if node == nil {
		return false
	}
	sawScan := false
	var walk func(n *logical.Node) bool
	walk = func(n *logical.Node) bool {
		if n == nil {
			return false
		}
		switch n.Type {
		case logical.NodeScan:
			// A table function's row count is not bounded by its input, but
			// stopping early still yields a prefix of what it would produce.
			sawScan = true
			return true
		case logical.NodeProject, logical.NodeFilter, logical.NodeLimit:
			// A nested LIMIT is at most as permissive as this one.
		default:
			return false
		}
		for _, c := range n.Children {
			if !walk(c) {
				return false
			}
		}
		return true
	}
	if !walk(node) {
		return false
	}
	return sawScan
}
