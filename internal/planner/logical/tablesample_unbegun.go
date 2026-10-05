// SPDX-License-Identifier: MIT

package logical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// dropUnbegunSamples takes the sampler off every TABLESAMPLE scan whose rows
// no answer can read: a scan below a Filter with a constant-false (or NULL)
// conjunct, or below a LIMIT 0 (#1411 review r1 B4).
//
// PostgreSQL checks a sample's percentage when the sample scan BEGINS — its
// first fetch — and its planner never begins these: a constant-false WHERE or
// HAVING folds to a one-time filter (or a dummy plan) that never runs the
// subtree, and LIMIT 0 never pulls its input. So `… TABLESAMPLE BERNOULLI
// (101) WHERE false` and `… LIMIT 0` answer no rows there, where this engine's
// pipeline pulled the scan's first batch and raised 2202H.
//
// The engine has no one-time filter: the subtree still runs, and every row it
// produces is discarded above it, so no answer depends on what the scan
// yields. What the sampler would do there is unobservable except for its
// range check, which PostgreSQL never makes — so the scan is planned without
// it. A scan whose rows CAN reach the answer keeps its sampler and its check:
// `WHERE id < 0` begins the scan on PostgreSQL too (2202H on both), as does a
// table with no rows.
//
// The argument's own coercion (22003, 42804, 22P02) is not this check: it
// happened when the scan was built, as PostgreSQL's parse analysis raises it
// whatever the plan.
func dropUnbegunSamples(n *Node) {
	if n == nil {
		return
	}
	if (n.Type == NodeFilter && filterIsConstantFalse(n)) || (n.Type == NodeLimit && n.LimitVal == 0) {
		clearSamples(n)
		return
	}
	for _, c := range n.Children {
		dropUnbegunSamples(c)
	}
}

func clearSamples(n *Node) {
	if n == nil {
		return
	}
	if n.Type == NodeScan && n.SampleMethod != "" {
		n.SampleMethod, n.SamplePercent, n.SampleNull, n.SampleClockArg = "", 0, false, nil
	}
	for _, c := range n.Children {
		clearSamples(c)
	}
}

// filterIsConstantFalse reports whether one of a Filter's conjuncts reads no
// row, calls nothing volatile and evaluates to false or NULL — a filter no
// row passes, whatever its input.
func filterIsConstantFalse(n *Node) bool {
	for _, p := range n.Predicates {
		node := p.ASTExpr
		if node == nil {
			continue
		}
		if !conjunctIsConstant(node) {
			continue
		}
		if v, ok := evalConstantFilter(node); ok {
			if b, isBool := v.(bool); v == nil || (isBool && !b) {
				return true
			}
		}
	}
	return false
}

// evalConstantFilter evaluates a constant conjunct once; ok=false when it
// does not compile or its evaluation raises (1/0 is the statement's answer
// when the filter runs, not this pass's).
//
// A conjunct that reads a CLOCK function is not decided here either: its value
// is the statement's clock (#1566), bound where the statement runs, and a
// plan-time read of the live clock could fold a filter to an answer the
// run-time evaluation of the same now() contradicts. It stays in the plan.
func evalConstantFilter(node plansql.Node) (v any, ok bool) {
	if readsClock(node) {
		return nil, false
	}
	c, err := expr.Compile(node)
	if err != nil {
		return nil, false
	}
	defer func() {
		if recover() != nil {
			v, ok = nil, false
		}
	}()
	v, err = evalConstant(c)
	return v, err == nil
}

// ConjunctIsConstant is conjunctIsConstant for the stage planner, which
// evaluates a conjunct carrying a deferred subquery failure once, as
// PostgreSQL's one-time filter does.
func ConjunctIsConstant(node plansql.Node) bool { return conjunctIsConstant(node) }

// conjunctIsConstant reports whether a conjunct reads no row and has one
// value however often it is evaluated: no column, no subquery, no aggregate
// or window, no placeholder a later stage substitutes, and nothing volatile
// (a filter over random() is evaluated per row, so one evaluation here says
// nothing about the rows).
func conjunctIsConstant(node plansql.Node) bool {
	constant := true
	plansql.RewriteExpr(node, func(x plansql.Node) (plansql.Node, bool) {
		switch v := x.(type) {
		case *plansql.ColRef, *plansql.StarNode, *plansql.SubqueryNode, *plansql.ExistsNode,
			*plansql.AnyAllExpr, *plansql.LiteralPlaceholder, *plansql.WindowFuncNode:
			constant = false
		case *plansql.FuncCallNode:
			if plansql.IsAggregate(v.Name) || volatileFuncs[strings.ToLower(v.Name)] {
				constant = false
			}
		}
		return nil, false
	})
	return constant
}

// readsClock reports whether node calls a SQL clock function (now(),
// CURRENT_TIMESTAMP, LOCALTIMESTAMP, CURRENT_DATE): one value per statement,
// which this layer has no statement to read (expr.StartStatement).
func readsClock(node plansql.Node) bool {
	found := false
	plansql.RewriteExpr(node, func(x plansql.Node) (plansql.Node, bool) {
		if f, ok := x.(*plansql.FuncCallNode); ok && expr.IsClockFunc(strings.ToLower(f.Name)) {
			found = true
		}
		return nil, false
	})
	return found
}
