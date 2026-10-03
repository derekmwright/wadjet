// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// windowDeclaredInput is the set of names a window stage's input carries
// when the producer below it emits the derived table's DECLARED columns —
// lower-cased bare name → the name on the stream — or nil when it does not.
type windowDeclaredInput map[string]string

// name returns the stream name a reference to a declared column reads, or "".
func (d windowDeclaredInput) name(ref string) string {
	if d == nil || ref == "" {
		return ""
	}
	return d[strings.ToLower(stripQualifier(ref))]
}

// materializeWindowDeclaredInput makes the producer below a window emit the
// declared columns of the derived table it reads, under their own names,
// when that table shadows a column of its own input
// (logical.WindowShadowedInput), and returns them.
//
// Every walk that resolves a name from above such a window stops at it, so
// every consumer above — the SELECT list, a filter, a join, an aggregate, a
// set operation, a window over the window — reads the derived table's column
// by its own name, with no table mapping one name to another. The shadowed
// source is not on the stream at all, as it is not in scope above the derived
// table. The window's own keys and argument read the same names.
//
// Each item is computed by the producer from its definition, composed down
// through the derived tables between it and the producer. A producer that
// cannot compute every item (not a scan or a join, a projection already
// attached, a column it does not carry) refuses the plan with
// ErrUnreachableGatherOutput, which routes the query to the coordinator-local
// pipeline: the walks above have stopped at the window either way.
func (p *StagePlanner) materializeWindowDeclaredInput(stages []Stage, window *logical.Node) windowDeclaredInput {
	top := logical.WindowShadowedInput(window)
	if top == nil {
		return nil
	}
	refuse := func(why string) windowDeclaredInput {
		if p.windowRouteErr == nil {
			p.windowRouteErr = fmt.Errorf("%w: a window over a derived table that computes a column under "+
				"the name of a column of its own input reads the table's declared columns, and %s",
				ErrUnreachableGatherOutput, why)
		}
		return nil
	}
	bottom := chainBottom(top)
	producer := windowAliasProducer(stages)
	switch {
	case bottom == nil || (bottom.Type != logical.NodeScan && bottom.Type != logical.NodeJoin):
		return refuse("the stage below it cannot compute them")
	case producer == nil || len(producer.ProjectExprs) > 0:
		return refuse("its producer already carries a projection")
	case (bottom.Type == logical.NodeScan) != (producer.Type == StageScan):
		return refuse("its producer is not the stage that computes the table's input")
	}
	decls := localPlanFacts.InputColDecls(bottom)
	strict := localPlanFacts.StrictIntArithCols(bottom)
	specs := make([]physical.ProjectExprSpec, 0, len(top.Projections))
	out := windowDeclaredInput{}
	for _, item := range top.Projections {
		name := strings.ToLower(stripQualifier(localPlanFacts.ProjectionOutputName(item)))
		def := projectionDefinition(item)
		if item.IsAgg || name == "" || def == nil {
			return refuse("an item has no definition the producer can compute")
		}
		composed, ok := composeThroughDerivedTables(def, top.Children[0], 0)
		if !ok || !producerEvaluates(producer, composed.String()) {
			return refuse(fmt.Sprintf("the producer does not carry what %q reads", name))
		}
		if _, dup := out[name]; dup {
			return refuse(fmt.Sprintf("it publishes %q twice", name))
		}
		d := localPlanFacts.DeclTypeParts(localPlanFacts.InferProjectionDeclType(composed, parquet.TypeString, strict, decls))
		specs = append(specs, physical.ProjectExprSpec{
			Expr: composed.String(), Name: name, Type: d.Type, TypeKnown: true,
			Fields: d.Fields, Precision: d.Precision, Scale: d.Scale, ElementType: d.ElementType,
		})
		out[name] = name
	}
	if len(specs) == 0 {
		return refuse("it publishes no column")
	}
	producer.ProjectExprs = specs
	return out
}

// chainBottom is the first node below a derived table that is not a Project,
// filter, sort or limit.
func chainBottom(n *logical.Node) *logical.Node {
	for n != nil {
		switch n.Type {
		case logical.NodeProject, logical.NodeFilter, logical.NodeSort, logical.NodeLimit:
			if len(n.Children) != 1 {
				return nil
			}
			n = n.Children[0]
		default:
			return n
		}
	}
	return nil
}

// projectionDefinition is what a SELECT-list item computes: its expression,
// or the column it renames.
func projectionDefinition(item logical.Projection) plansql.Node {
	switch {
	case item.ASTExpr != nil:
		return item.ASTExpr
	case item.Column != "":
		return &plansql.ColRef{Column: item.Column}
	}
	return nil
}

// composeThroughDerivedTables spells an expression over the relation below a
// chain of derived tables: every reference to a derived table's column is
// replaced by that column's definition, recursively, through Projects,
// filters, sorts and limits.
func composeThroughDerivedTables(ast plansql.Node, n *logical.Node, depth int) (plansql.Node, bool) {
	if depth > aggRespellDepth {
		return nil, false
	}
	for n != nil && (n.Type == logical.NodeFilter || n.Type == logical.NodeSort || n.Type == logical.NodeLimit) {
		if len(n.Children) != 1 {
			return nil, false
		}
		n = n.Children[0]
	}
	if n == nil || n.Type != logical.NodeProject || len(n.Children) != 1 {
		return ast, true
	}
	failed := false
	out, _, complete := localPlanFacts.RewriteColRefs(ast, func(ref *plansql.ColRef) (plansql.Node, bool) {
		written := ref.String()
		proj := localPlanFacts.ProjectionForName(n.Projections, written, localPlanFacts.DerivedScopeBareName(written, n))
		if proj == nil || proj.IsAgg {
			failed = true
			return nil, false
		}
		def := projectionDefinition(*proj)
		if def == nil {
			failed = true
			return nil, false
		}
		sub, ok := composeThroughDerivedTables(def, n.Children[0], depth+1)
		if !ok {
			failed = true
			return nil, false
		}
		if _, isRef := sub.(*plansql.ColRef); isRef {
			return sub, true
		}
		return &plansql.ParenNode{Inner: sub}, true
	})
	if failed || !complete {
		return nil, false
	}
	return out, true
}

// keyExprs spells every column reference in the window's key expressions
// that names a declared column by its stream name; without a declared input
// the specs take the respell path.
func (d windowDeclaredInput) keyExprs(specs []physical.ProjectExprSpec, otherwise func([]physical.ProjectExprSpec) []physical.ProjectExprSpec) []physical.ProjectExprSpec {
	if d == nil {
		return otherwise(specs)
	}
	for i := range specs {
		ast, err := plansql.ParseExpression(specs[i].Expr)
		if err != nil {
			continue
		}
		out, changed, complete := localPlanFacts.RewriteColRefs(ast, func(ref *plansql.ColRef) (plansql.Node, bool) {
			if n := d.name(ref.String()); n != "" {
				return &plansql.ColRef{Column: n}, true
			}
			return nil, false
		})
		if changed && complete {
			specs[i].Expr = out.String()
		}
	}
	return specs
}

// typedOverShadowedWindow spells an expression written over the declared
// columns of a window's shadowing derived table — the names a rewrite through
// the renames above the window stops at — in its definitions over the
// relation below the table, for its DECLARATION only; nil when the renames
// below child do not end at such a window.
func typedOverShadowedWindow(ast plansql.Node, child *logical.Node) (plansql.Node, *logical.Node) {
	n := child
	for n != nil && len(n.Children) == 1 {
		switch n.Type {
		case logical.NodeProject:
			for _, item := range n.Projections {
				if item.IsAgg || item.Column == "" {
					return nil, nil
				}
			}
		case logical.NodeFilter, logical.NodeSort, logical.NodeLimit, logical.NodeDistinct:
		case logical.NodeWindow:
			top := logical.WindowShadowedInput(n)
			if top == nil {
				break // a window over no such table forwards its input's names
			}
			typed, ok := composeThroughDerivedTables(ast, top, 0)
			if !ok {
				return nil, nil
			}
			return typed, chainBottom(top)
		default:
			return nil, nil
		}
		n = n.Children[0]
	}
	return nil, nil
}
