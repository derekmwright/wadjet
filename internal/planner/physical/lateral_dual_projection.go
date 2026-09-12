// This file holds the single-process lowering of a table-less LATERAL body,
// governed by ADR-0021.
package physical

import (
	"context"
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// buildTableLessLateralJoin builds the pipeline for a join whose right side is
// a LATERAL body with NO FROM clause: the OUTER subtree's pipeline, with the
// body's SELECT list computed on top of it (exec.LateralOuterProject).
//
// There is no join operator, because there is no join: the body yields exactly
// one row per outer row and its columns are functions of that row, so
// `left CROSS JOIN LATERAL (SELECT e(u) AS v)` IS `π(u.*, e(u) AS v)(u)`
// (#1033). The logical tree keeps the join node with the Dual under it — the
// declaration walks read the body's items there, and the stage DAG hands any
// plan containing a Dual to the in-process pipeline, which is what keeps every
// arm answering through this one lowering.
//
// The join's CONDITION is not consulted and must be empty: the logical
// lowering moved the body's WHERE and the written ON into the enclosing
// query's WHERE, which runs above this projection and is where an inner join's
// condition already applies. An outer join whose ON does not fold to true was
// refused there rather than reaching here.
func (p *Planner) buildTableLessLateralJoin(ctx context.Context, node *logical.Node) (
	exec.Source, []exec.UnaryOperator, exec.Sink, error) {

	cols, err := compileLateralDualItems(node.LateralDualItems, node.Children[0])
	if err != nil {
		return nil, nil, nil, err
	}
	source, ops, sink, err := p.buildPipeline(ctx, node.Children[0])
	if err != nil {
		return nil, nil, nil, err
	}
	if op := exec.NewLateralOuterProject(node.LateralDualAlias, cols); op != nil {
		ops = append(ops, op)
	}
	return source, ops, sink, nil
}

// compileLateralDualItems compiles each item of a table-less LATERAL body
// against the OUTER subtree's declared columns, through the engine's own
// expression compiler and the same declaration rule an ordinary SELECT item
// takes (inferProjectionDeclType). The declaration is not decoration: it is
// what the vector is allocated as, so an item typed by the BOOL zero value
// stored `true` for every value #1033 lost.
//
// An item that will not compile is an ERROR and never a dropped column: the
// value it stands for is the whole of what the issue lost, and a column
// silently missing from the projection is the same wrong answer in another
// shape.
func compileLateralDualItems(items []logical.Projection, outer *logical.Node) (
	[]exec.LateralOuterColumn, error) {

	decls := inputColDecls(outer)
	strictInt := strictIntArithCols(outer)
	out := make([]exec.LateralOuterColumn, 0, len(items))
	for _, item := range items {
		name := item.Alias
		if name == "" {
			name = item.Column
		}
		if name == "" || item.ASTExpr == nil {
			return nil, fmt.Errorf("a LATERAL subquery with no FROM clause has an item "+
				"this planner cannot compute: %q", item.Expr)
		}
		compiled, err := expr.CompileWithColumnTypes(item.ASTExpr, nil, decls.types, nil)
		if err != nil {
			return nil, fmt.Errorf("compiling LATERAL item %q: %w", item.Expr, err)
		}
		decl := lateralDualItemDecl(item, decls, strictInt)
		expr.StampArithMode(compiled, decl.ID == parquet.TypeInt64)
		out = append(out, exec.LateralOuterColumn{
			Name: name, Expr: compiled.Eval, Decl: decl,
		})
	}
	return out, nil
}

// lateralDualItemDecl is what ONE item of a table-less LATERAL body declares,
// typed against the OUTER subtree's columns.
//
// It is one function and not two because the vector the runtime allocates and
// the type the planner publishes are one answer: a column declared INT64 by
// the walk and built as a text vector by the operator is the same wrong answer
// #1033 was filed for, spelled the other way round.
//
// A BARE outer reference takes its column's own declaration rather than the
// withheld one inferProjectionDeclTypeConf gives a bare reference: that
// withholding exists because exec.Project types a direct COPY from the
// same-named input column, and a body publishes the column under ITS OWN
// alias, so there is no same-named input column to correct the fallback and
// `u.id AS v` declared STRING.
func lateralDualItemDecl(item logical.Projection, decls colDecls,
	strictInt map[string]bool) expr.DeclType {

	if item.ASTExpr == nil {
		return expr.Decl(parquet.TypeString)
	}
	if _, bare := item.ASTExpr.(*plansql.ColRef); bare {
		if t, c := nodeDeclaredType(item.ASTExpr, decls); c != expr.Undecided {
			return t
		}
	}
	return inferProjectionDeclType(item.ASTExpr, parquet.TypeString, strictInt, decls)
}

// lateralDualItemDecls is every item's declaration, keyed by the LOWER-CASED
// output name — what the join node PUBLISHES beyond its outer side, for the
// declaration walks that ask a node what its columns are.
//
// `inputColTypes` stops at a Project because a rename may bind a name to a
// different value; the body's Project over the Dual is exactly such a stop,
// and with the join's right side answering nothing the merged map was nil —
// so `SUM(l.v)` over the lateral declared float8 where PostgreSQL declares
// numeric, and a SECOND lateral reading the first one's column declared STRING.
func lateralDualItemDecls(join *logical.Node) map[string]expr.DeclType {
	if join == nil || len(join.LateralDualItems) == 0 || len(join.Children) != 2 {
		return nil
	}
	outer := join.Children[0]
	decls := inputColDecls(outer)
	strictInt := strictIntArithCols(outer)
	out := make(map[string]expr.DeclType, len(join.LateralDualItems))
	for _, item := range join.LateralDualItems {
		name := item.Alias
		if name == "" {
			name = item.Column
		}
		if name == "" {
			continue
		}
		name = strings.ToLower(strings.TrimSpace(name))
		// QUALIFIED where the outer row already carries the name, because that
		// is the column the operator emits: an item colliding with an outer
		// column is TWO columns, and this walk and the runtime have to describe
		// the same one (exec.LateralOuterProject).
		if _, collides := decls.types[name]; collides && join.LateralDualAlias != "" {
			name = strings.ToLower(join.LateralDualAlias) + "." + name
		}
		out[name] = lateralDualItemDecl(item, decls, strictInt)
	}
	return out
}
