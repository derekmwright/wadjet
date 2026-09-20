// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A reference to a column a table function does not publish is 42703 — LOUDLY,
// naming the column — and never a NULL for every row (#1210).
//
// For a function whose SIGNATURE declares its columns the refusal is made at
// plan time, by the binder, exactly as it is over a base table
// (tableFuncDeclaredSchema). A function whose columns are its INPUT's — every
// file and database reader — has no plan-time column list this engine may ask
// for: the binder runs BEFORE the table-function capability is authorized on
// every door (auth.ValidateStatementColumns precedes auth.EnforcePlanPolicies,
// and on the coordinator door physical.AnnotateScanColumns does too), so
// reading the file there would open it for an identity that may not be allowed
// to — the property #943 and ADR-0034 hold. So the refusal for THOSE is made
// where the schema first exists: at the FIRST BATCH.
//
// The names checked are the ones the operators DIRECTLY ABOVE the relation ask
// of it. That position is what makes them certain: the batch the source
// publishes IS the relation, nothing has renamed or minted anything yet, and a
// reference that resolves to no column of it resolves to nothing at all. A
// consumer further up reads a schema some operator has already changed, and
// its names are not this relation's to answer for.
//
// The boundary, recorded on the differences page: a function that produces NO
// batch is never measured — the same boundary the column-alias list's 42P10
// has (#1184) and for the same reason.

// tableFuncSourceRelation reports the table-function Scan whose OWN OUTPUT is
// the input of n, or ok=false when it is not one. The walk descends only
// through operators that publish their input's schema unchanged, because those
// are the only ones that leave the relation's own batch in place.
//
// A function whose columns tableFuncDeclaredSchema knows is excluded: the
// binder already refused an unknown column at plan time, which is earlier and
// covers a call that produces no batch.
func tableFuncSourceRelation(n *logical.Node) (relName string, ok bool) {
	for n != nil {
		switch n.Type {
		case logical.NodeScan:
			if !n.IsTableFunc {
				return "", false
			}
			if _, declared := tableFuncDeclaredSchema(n.FuncName, n.FuncArgs, n.WithOrdinality); declared {
				return "", false
			}
			name := n.TableAlias
			if name == "" {
				name = n.FuncName
			}
			return name, true
		case logical.NodeFilter, logical.NodeLimit:
			if len(n.Children) != 1 {
				return "", false
			}
			n = n.Children[0]
		default:
			return "", false
		}
	}
	return "", false
}

// nodeInputColumnRefs enumerates the column references one logical node makes
// OF ITS INPUT, and ok=false when the node carries an expression whose
// references cannot be enumerated with certainty — a subquery, a window call,
// a star, or a node shape this walk does not know. Certainty is the whole
// contract: a refusal built from an incomplete enumeration refuses a column
// that is there (expr.FilterColumnRefs' own rule, #147).
func nodeInputColumnRefs(n *logical.Node) ([]string, bool) {
	var out []string
	add := func(name string) {
		name = strings.TrimSpace(name)
		// A DELIMITED identifier names ONE column, dots included: a Zeek
		// `"id.orig_h"` is the whole name and not alias.column, and it
		// reaches here with its quotes because that is how a reference is
		// SPELLED. Resolve it the way the scan's own read set does
		// (logical.sanitizeScanNeeds), or every quoted column over a table
		// function is refused as missing.
		if strings.Contains(name, `"`) {
			if qual, col, ok := plansql.SplitIdentRef(name); ok {
				if qual == "" {
					name = col
				} else {
					name = qual + "." + col
				}
			}
		}
		// A planner-minted slot is not a column of any relation, and the
		// operators that read one publish it themselves.
		if name == "" || name == "*" || strings.HasPrefix(name, "__") {
			return
		}
		out = append(out, name)
	}
	addExpr := func(e plansql.Node) bool {
		if e == nil {
			return true
		}
		refs, ok := expr.FilterColumnRefs(e)
		if !ok {
			return false
		}
		for _, r := range refs {
			add(r)
		}
		return true
	}
	// A TERM IS A TERM, NOT A COLUMN NAME, wherever the plan carries one as
	// TEXT. `GROUP BY a + 1` reaches the logical node as the string "a + 1",
	// `GROUP BY UPPER(b)` as "upper(b)", and an ordinal or a select alias
	// arrives already resolved to the select item's text — so asking the
	// relation for a column of that name refuses a column that IS there.
	// Eight base-right spellings over a file reader were newly 42703 for
	// exactly that (round-1 review, B3, whose repair this generalizes).
	//
	// Every such field goes through here: the text is PARSED and the
	// references it actually makes are taken. A term that will not parse
	// cannot be enumerated with certainty, and the rule for that is this
	// file's rule everywhere — no check at all, never a guess.
	addTerm := func(text string) bool {
		text = strings.TrimSpace(text)
		if text == "" || text == "*" || strings.HasPrefix(text, "__") {
			return true
		}
		node, err := plansql.ParseExpression(text)
		if err != nil || node == nil {
			return false
		}
		return addExpr(node)
	}
	switch n.Type {
	case logical.NodeFilter:
		for _, pred := range n.Predicates {
			if pred.ASTExpr != nil {
				if !addExpr(pred.ASTExpr) {
					return nil, false
				}
				continue
			}
			if pred.Raw != "" {
				// A predicate carried as TEXT: its references cannot be
				// enumerated with certainty from here.
				return nil, false
			}
			if !addTerm(pred.Column) {
				return nil, false
			}
		}
	case logical.NodeProject:
		for _, proj := range n.Projections {
			if proj.SlotSource != "" {
				continue
			}
			if proj.ASTExpr != nil {
				if !addExpr(proj.ASTExpr) {
					return nil, false
				}
				continue
			}
			if proj.IsAgg {
				return nil, false
			}
			if !addTerm(proj.Column) {
				return nil, false
			}
		}
	case logical.NodeSort:
		for _, ob := range n.OrderBy {
			if ob.Position != 0 {
				return nil, false
			}
			if !addTerm(ob.Column) {
				return nil, false
			}
		}
	case logical.NodeAggregate:
		if len(n.GroupingSets) > 0 || len(n.GroupingCalls) > 0 {
			return nil, false
		}
		for _, gb := range n.GroupBy {
			if !addTerm(gb) {
				return nil, false
			}
		}
		for _, e := range n.GroupByExprs {
			if !addExpr(e) {
				return nil, false
			}
		}
		for _, a := range n.AggExprs {
			if a.InputExpr != nil {
				if !addExpr(a.InputExpr) {
					return nil, false
				}
				continue
			}
			if !addTerm(a.InputCol) {
				return nil, false
			}
		}
	default:
		return nil, false
	}
	return out, true
}

// withRequiredColumns wraps a table function's source so that, at its FIRST
// BATCH, a name the consumer above asks of it and the relation does not
// publish is 42703 with the column named and the relation's own columns
// listed — PostgreSQL's class and its diagnosis, where this engine answered
// NULL for every row.
func withRequiredColumns(src exec.Source, refs []string, relName string) exec.Source {
	if len(refs) == 0 {
		return src
	}
	return &requiredColumnsSource{src: src, refs: refs, relName: relName}
}

type requiredColumnsSource struct {
	src     exec.Source
	refs    []string
	relName string
	checked bool
}

func (s *requiredColumnsSource) Init(ctx context.Context) error { return s.src.Init(ctx) }

func (s *requiredColumnsSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	b, err := s.src.Next(ctx)
	if err != nil || b == nil || s.checked {
		return b, err
	}
	s.checked = true
	for _, ref := range s.refs {
		if idx, _ := expr.ResolveColumnRef(b, ref); idx < 0 {
			return nil, sqlerr.New("42703",
				"column %q does not exist: the table function %q publishes %s",
				ref, s.relName, strings.Join(publishedNames(b.Schema), ", "))
		}
	}
	return b, nil
}

func (s *requiredColumnsSource) Close() error { return s.src.Close() }

func publishedNames(schema []parquet.Column) []string {
	out := make([]string, len(schema))
	for i, c := range schema {
		out[i] = c.Name
	}
	return out
}

// guardTableFuncColumns wraps src with the first-batch refusal when consumer's
// INPUT is a dynamic-schema table function's own output. It is a no-op
// everywhere else, including over a function whose columns the binder already
// held the statement to at plan time.
func (p *Planner) guardTableFuncColumns(consumer, input *logical.Node, src exec.Source) exec.Source {
	relName, ok := tableFuncSourceRelation(input)
	if !ok {
		return src
	}
	refs, ok := nodeInputColumnRefs(consumer)
	if !ok {
		return src
	}
	return withRequiredColumns(src, refs, relName)
}
