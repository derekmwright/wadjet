// SPDX-License-Identifier: MIT

package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// ProjectsASet reports whether a Project node has a set-returning SELECT
// item — unnest, generate_subscripts or information_schema._pg_expandarray,
// or a field of the last. The local planner expands those with a ProjectSet
// (physical/set_returning.go); a planner that does not (the stage DAG) asks
// this to hand the statement to the local pipeline instead.
func ProjectsASet(n *Node) bool {
	if n == nil || n.Type != NodeProject {
		return false
	}
	for _, p := range n.Projections {
		fn, ok := plansql.Unparen(p.ASTExpr).(*plansql.FuncCallNode)
		if !ok {
			continue
		}
		name := strings.ToLower(fn.Name)
		if name == "row_field" && len(fn.Args) == 2 {
			if inner, ok := plansql.Unparen(fn.Args[0]).(*plansql.FuncCallNode); ok {
				name = strings.ToLower(inner.Name)
			}
		}
		if isSetReturningName(name) {
			return true
		}
	}
	return false
}

// isSetReturningName reports whether a call of this name returns a SET when
// it is a SELECT item — the names physical/set_returning.go expands, under
// the schema qualifiers PostgreSQL resolves them in.
func isSetReturningName(name string) bool {
	name = strings.ToLower(name)
	name = strings.TrimPrefix(strings.TrimPrefix(name, "pg_catalog."), "information_schema.")
	switch name {
	case "unnest", "generate_subscripts", "_pg_expandarray", "generate_series":
		return true
	}
	return false
}
