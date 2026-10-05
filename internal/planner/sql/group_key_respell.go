// SPDX-License-Identifier: MIT

package sql

import "strings"

// A SELECT ITEM THAT IS A GROUP BY KEY IS SPELLED AS THE KEY (#1524,
// ADR-0026 §1a).
//
// PostgreSQL matches a select-list, HAVING or ORDER BY expression to a GROUP
// BY key by comparing the PARSED, RESOLVED trees: in `SELECT t.i + 1 … FROM
// ss_t t GROUP BY i + 1` both `t.i` and `i` resolve to the one relation's
// column `i`, so the item IS the key and is published with the key's value
// and declared type. ExprIdentity erases parentheses, identifier case and
// whitespace, and nothing else — it cannot erase a qualifier, because whether
// `t.x` and `x` are one column is a question about the block's SCOPE, not
// about either spelling.
//
// The block's scope is known here. In a block whose FROM is ONE relation, a
// qualifier that names that relation is spelling; one that names anything else
// (an OUTER relation a correlated subquery reads) is not. BlockIdentity is
// ExprIdentity with exactly that qualifier erased, and it is the ONE rule.
//
// It is applied ONCE, to the block, before anything matches a consumer to a
// key: every select item, HAVING, ORDER BY and window term that is a key under
// BlockIdentity but spelled differently from it is RE-SPELLED AS THE KEY, AST
// and text alike. Every consumer downstream — the 42803 check, the projection
// over the aggregate and its declared type, the HAVING and window respelling,
// the sort-key resolution, the DISTINCT lowering, the stage DAG's dispatch —
// then reads one spelling, which is the spelling it has always answered for
// the statement whose item and key are written alike. The alternative, teaching
// each of those consumers the block's scope, is the per-site matching ADR-0026
// retired.

// BlockOwnQualifier is the qualifier a reference in this block may carry to
// name the block's ONE relation — its alias, or its name when it has none —
// and "" when the FROM clause is not exactly one relation. Over a join, a bare
// name resolves to whichever relation provides it, which takes the relations'
// column sets to decide; that is not this rule's to guess.
func BlockOwnQualifier(info *SelectInfo) string {
	if info == nil || len(info.Tables) != 1 || len(info.Joins) != 0 {
		return ""
	}
	t := info.Tables[0]
	if t.Alias != "" {
		return t.Alias
	}
	if t.IsFunction || strings.ContainsAny(t.Name, " ()") {
		return ""
	}
	return t.Name
}

// BlockIdentity is ExprIdentity with the block's OWN qualifier erased from
// every column reference — the resolved identity in a single-relation block.
// With own == "" it is ExprIdentity exactly.
func BlockIdentity(n Node, own string) string {
	if own == "" {
		return ExprIdentity(n)
	}
	return ExprIdentity(stripOwnQualifier(n, own))
}

// stripOwnQualifier rebuilds n with the qualifier `own` removed from every
// column reference that carries it, and every other qualifier kept. It does
// not enter an aggregate call (RewriteExpr's rule): inside one the reference
// is read over the input rows, and an aggregate is never a key.
func stripOwnQualifier(n Node, own string) Node {
	return RewriteExpr(n, func(x Node) (Node, bool) {
		ref, ok := x.(*ColRef)
		if !ok {
			return nil, false
		}
		if ref.Table == own {
			c := *ref
			c.Table = ""
			return &c, true
		}
		return ref, true
	})
}

// cloneExpr deep-copies an expression through RewriteExpr's own rebuild, with
// the two leaf kinds it shares by pointer copied too, so a re-spelled item
// never aliases its key's nodes.
func cloneExpr(n Node) Node {
	return RewriteExpr(n, func(x Node) (Node, bool) {
		switch e := x.(type) {
		case *ColRef:
			c := *e
			return &c, true
		case *Lit:
			c := *e
			return &c, true
		}
		return nil, false
	})
}

// RespellGroupKeyTerms re-spells, in a single-relation grouped block, every
// select item, HAVING, ORDER BY and window term that IS a GROUP BY key under
// BlockIdentity but is spelled differently from it, as that key. It never
// changes a key, never enters an aggregate call or a subquery, and leaves a
// block that is not one relation untouched. It is idempotent.
//
// A bare name in HAVING or ORDER BY that is a SELECT alias is left alone: it
// names the output column there, not the input column the key reads.
func RespellGroupKeyTerms(info *SelectInfo) {
	if info == nil || info.Union != nil || len(info.GroupByExprs) == 0 {
		return
	}
	own := BlockOwnQualifier(info)
	if own == "" {
		return
	}
	type key struct {
		resolved, spelled string
		ast               Node
	}
	var keys []key
	for i, e := range info.GroupByExprs {
		if e == nil {
			continue
		}
		if i < len(info.GroupBySubqueryOrigin) && info.GroupBySubqueryOrigin[i] != "" {
			continue // matched AS WRITTEN (SelectColumn.UnfoldedFrom)
		}
		u := Unparen(e)
		if _, isLit := u.(*Lit); isLit {
			continue // an ordinal or a constant names no expression
		}
		r := BlockIdentity(u, own)
		if r == "" {
			continue
		}
		keys = append(keys, key{resolved: r, spelled: ExprIdentity(u), ast: u})
	}
	if len(keys) == 0 {
		return
	}
	aliases := map[string]bool{}
	for _, c := range info.Columns {
		if a := strings.ToLower(strings.TrimSpace(c.Alias)); a != "" {
			aliases[a] = true
		}
	}
	// respell answers the re-spelled node and whether anything changed. A
	// window call's arguments, PARTITION BY and ORDER BY are read over the
	// GROUPED rows, so they are re-spelled too; RewriteExpr does not enter
	// one on its own.
	var respell func(n Node, aliasesVisible bool) (Node, bool)
	respell = func(n Node, aliasesVisible bool) (Node, bool) {
		if n == nil {
			return n, false
		}
		changed := false
		out := RewriteExpr(n, func(x Node) (Node, bool) {
			if ref, ok := x.(*ColRef); ok && aliasesVisible && ref.Table == "" && aliases[strings.ToLower(ref.Column)] {
				return ref, true
			}
			if w, ok := x.(*WindowFuncNode); ok {
				nw, wChanged := respellWindow(w, func(e Node) (Node, bool) { return respell(e, false) })
				if wChanged {
					changed = true
				}
				return nw, true
			}
			r := BlockIdentity(x, own)
			for _, k := range keys {
				if r != k.resolved {
					continue
				}
				if ExprIdentity(x) == k.spelled {
					return x, true // already the key's spelling
				}
				changed = true
				repl := cloneExpr(k.ast)
				if isInfixNode(repl) {
					repl = &ParenNode{Inner: repl}
				}
				return repl, true
			}
			return nil, false
		})
		if !changed {
			return n, false
		}
		return Unparen(out), true
	}
	respellText := func(s string, aliasesVisible bool) string {
		if strings.TrimSpace(s) == "" {
			return s
		}
		parsed, err := ParseExpression(s)
		if err != nil || parsed == nil {
			return s
		}
		if out, ok := respell(parsed, aliasesVisible); ok {
			return out.String()
		}
		return s
	}
	for i := range info.Columns {
		c := &info.Columns[i]
		if c.Star || c.IsAgg || c.UnfoldedFrom != "" {
			continue
		}
		if c.IsWindow {
			w, ok := c.ASTExpr.(*WindowFuncNode)
			if !ok || c.WindowSpec == nil {
				continue
			}
			nw, changed := respellWindow(w, func(e Node) (Node, bool) { return respell(e, false) })
			if !changed {
				continue
			}
			ws := windowSpecFromNode(nw, c.WindowSpec.Alias)
			ws.Frame = c.WindowSpec.Frame
			c.ASTExpr, c.WindowSpec, c.Expr = nw, ws, nw.String()
			continue
		}
		out, ok := respell(c.ASTExpr, false)
		if !ok {
			continue
		}
		c.ASTExpr = out
		c.Expr = out.String()
		if ref, isRef := out.(*ColRef); isRef {
			c.ColumnRef, c.TableRef = ref.Column, ref.Table
		}
	}
	if info.HavingUnfoldedFrom == "" {
		if out, ok := respell(info.HavingExpr, true); ok {
			info.HavingExpr = out
			info.Having = out.String()
		}
	}
	for i := range info.OrderBy {
		ob := &info.OrderBy[i]
		// An ORDINAL term carries the item's own text, and is the item.
		if ob.UnfoldedFrom != "" {
			continue
		}
		if ob.Expr != nil {
			if out, ok := respell(ob.Expr, true); ok {
				ob.Expr = out
				ob.Column = out.String()
			}
			continue
		}
		ob.Column = respellText(ob.Column, true)
	}
}

// respellWindow rebuilds a window call with each of its arguments, PARTITION
// BY and ORDER BY terms passed through rs, and reports whether any changed.
func respellWindow(w *WindowFuncNode, rs func(Node) (Node, bool)) (*WindowFuncNode, bool) {
	if w == nil || w.Func == nil {
		return w, false
	}
	changed := false
	f := *w.Func
	f.Args = make([]Node, len(w.Func.Args))
	for i, a := range w.Func.Args {
		na, ok := rs(a)
		changed = changed || ok
		f.Args[i] = na
	}
	nw := &WindowFuncNode{Func: &f, Frame: w.Frame,
		PartitionBy: make([]Node, len(w.PartitionBy)), OrderBy: make([]WindowOrderBy, len(w.OrderBy))}
	for i, p := range w.PartitionBy {
		np, ok := rs(p)
		changed = changed || ok
		nw.PartitionBy[i] = np
	}
	for i, o := range w.OrderBy {
		ne, ok := rs(o.Expr)
		changed = changed || ok
		nw.OrderBy[i] = WindowOrderBy{Expr: ne, Desc: o.Desc, NullsFirst: o.NullsFirst}
	}
	if !changed {
		return w, false
	}
	return nw, true
}
