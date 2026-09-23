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
		name = strings.TrimPrefix(strings.TrimPrefix(name, "pg_catalog."), "information_schema.")
		switch name {
		case "unnest", "generate_subscripts", "_pg_expandarray":
			return true
		}
	}
	return false
}
