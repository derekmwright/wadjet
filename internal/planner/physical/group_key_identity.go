package physical

import (
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// What an aggregate publishes its GROUP BY keys under, and how a consumer
// finds the one it means.
//
// One aggregate, two engines: the single-process pipeline materializes a
// derived key into a pre-aggregate projection column, and the stage DAG ships
// the key text to a worker that does the same. Both then have to answer the
// same question for every SELECT item, HAVING term and sort key above them —
// "which key IS this?" — and both used to answer it by comparing rendered
// text, each with its own idea of what counts as the same rendering. That is
// #720 (a HAVING that never matched), #723 (a SELECT item rebuilt as
// arithmetic over a column the aggregate does not emit, NULL on every row)
// and #725 (a delimited identifier shipped with its quotes).
//
// groupKeyOutputs is the single answer both paths read: one published NAME
// per key, and one plansql.ExprIdentity that resolves to it.

// groupKeyOut describes one GROUP BY key as the aggregate publishes it.
type groupKeyOut struct {
	// Name is the column the key's value is emitted under. For a bare
	// column of the aggregate's input that is the column itself; for a
	// derived key it is the key's canonical text, which is what the DAG's
	// worker names it and — since #723 — what the single-process
	// pre-aggregate projection names it too, so the two schemas agree.
	Name string
	// Identity is plansql.ExprIdentity of the key expression: the key a
	// consumer's own expression is looked up by.
	Identity string
	// Derived marks a key the aggregate's input does not already carry, so
	// one of the two paths has to materialize it.
	Derived bool
	// Literal marks a constant key the single-process path elides from the
	// key set and re-attaches afterwards; its Name is the synthetic one.
	Literal bool
}

// groupKeyOutputs describes an Aggregate node's GROUP BY keys. The rules are
// buildAggregate's own, stated once so aggregateOutputNames, the projection
// above the aggregate and the pre-aggregate projection cannot drift apart.
func groupKeyOutputs(agg *logical.Node) []groupKeyOut {
	if agg == nil || agg.Type != logical.NodeAggregate {
		return nil
	}
	var decls colDecls
	if len(agg.Children) == 1 {
		decls = inputColDecls(agg.Children[0])
	}
	haveExprs := len(agg.GroupByExprs) == len(agg.GroupBy)
	// A literal key is elided only when a non-literal key remains: GROUP BY
	// over literals alone must still emit zero rows on empty input, which
	// one retained key preserves.
	nonLit := 0
	if haveExprs {
		for _, e := range agg.GroupByExprs {
			if e == nil {
				nonLit++
				continue
			}
			if _, isLit := e.(*plansql.Lit); !isLit {
				nonLit++
			}
		}
	}
	out := make([]groupKeyOut, len(agg.GroupBy))
	for i, gb := range agg.GroupBy {
		k := groupKeyOut{Name: plansql.NormalizeIdentRef(strings.TrimSpace(gb))}
		var e plansql.Node
		if haveExprs {
			e = agg.GroupByExprs[i]
		}
		if e == nil {
			// No AST for this key: parse the recorded text so the identity
			// is still the canonical one. A text that does not parse keeps
			// its own spelling as its identity, which is what every site
			// compared before identities existed.
			if parsed, err := plansql.ParseExpression(gb); err == nil {
				k.Identity = plansql.ExprIdentity(parsed)
			} else {
				k.Identity = strings.ToLower(strings.TrimSpace(gb))
			}
			out[i] = k
			continue
		}
		k.Identity = plansql.ExprIdentity(e)
		if _, isLit := e.(*plansql.Lit); isLit && nonLit > 0 {
			k.Literal = true
			k.Name = syntheticGroupKeyName(i)
		} else if !isPlainGroupKey(e, decls) {
			k.Derived = true
		}
		out[i] = k
	}
	return out
}

// syntheticGroupKeyName is the name a LITERAL group key is re-attached under
// after being elided from the key set. It stays synthetic: the value is a
// constant, no consumer resolves it by expression, and its own text (`1`,
// `'x'`) makes a poor column name.
func syntheticGroupKeyName(i int) string {
	return "__gb_expr_" + strconv.Itoa(i)
}

// groupKeysByIdentity indexes a STAGE's published output names by the
// identity of the expression each one names.
//
// The stage has only the names — it is a serialized plan, not an AST — so
// each is parsed back to recover its identity. A name that does not parse
// as an expression is indexed under itself, which is what a text comparison
// gave before identities existed.
func groupKeysByIdentity(names map[string]string) map[string]string {
	if len(names) == 0 {
		return nil
	}
	m := make(map[string]string, len(names))
	for _, real := range names {
		id := strings.ToLower(real)
		if parsed, err := plansql.ParseExpression(real); err == nil {
			id = plansql.ExprIdentity(parsed)
		}
		if _, taken := m[id]; taken {
			continue
		}
		m[id] = real
	}
	return m
}

// groupKeyByIdentity indexes the DERIVED keys of an aggregate by identity, so
// a SELECT item, a HAVING term or a sort key spelled any way at all resolves
// to the one column the aggregate publishes it under.
//
// Bare-column keys are deliberately absent: their value is published under
// the input column's own name and every consumer already reads it there. A
// mapping for them would only re-route a resolution that works, and the one
// shape it would repair — a SELECT item that spells the column in a different
// CASE — is broken with no GROUP BY in sight (`SELECT G FROM t`), so it
// belongs to the identifier-folding defect and not to this one.
func groupKeyByIdentity(agg *logical.Node) map[string]string {
	keys := groupKeyOutputs(agg)
	if len(keys) == 0 {
		return nil
	}
	m := make(map[string]string, len(keys))
	for _, k := range keys {
		if !k.Derived && !k.Literal {
			continue
		}
		if k.Identity == "" {
			continue
		}
		if _, taken := m[k.Identity]; taken {
			continue // first wins; two keys of one identity are one column
		}
		m[k.Identity] = k.Name
	}
	return m
}
