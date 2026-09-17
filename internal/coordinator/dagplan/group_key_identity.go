// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

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

// aggregateUnderOutput finds the Aggregate the output projection reads, or nil
// when the plan's top is not a grouped query. Only the nodes that leave the
// aggregate's own columns visible are walked through: a Project, and the
// wrappers physical.AggScopePreservingWrapper names. A join or a set operation below
// the top means the SELECT list is written over something else, and the walk
// declines rather than guessing.
func aggregateUnderOutput(root *logical.Node) *logical.Node {
	for n := root; n != nil; {
		switch {
		case n.Type == logical.NodeAggregate:
			return n
		case n.Type == logical.NodeProject, localPlanFacts.AggScopePreservingWrapper(n.Type):
		default:
			return nil
		}
		if len(n.Children) != 1 {
			return nil
		}
		n = n.Children[0]
	}
	return nil
}
