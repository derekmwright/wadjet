// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// typeByteaLiterals gives an UNKNOWN-typed (quoted) literal that a construct
// resolves to bytea its bytea type IN THE PLAN, as `CAST('…' AS bytes)` — the
// one reading of a text as bytea, byteain (kernel.ByteaIn), which the CAST
// already applies on every arm (#1590).
//
// PostgreSQL resolves the common type of COALESCE, NULLIF, GREATEST, LEAST and
// a CASE's results (select_common_type), of `bytea || unknown`,
// `position(unknown in bytea)` and a comparison (operator / function
// resolution) and of a set operation's column, and coerces an unknown literal
// among them to that type with the type's input function. When the type is
// bytea the literal names the bytes byteain reads from it: `COALESCE(b,
// '\x6869')` is the two bytes `hi`. Nothing did that here: the literal
// reached the BYTES vector, the concatenation and the set operation's result
// as its own SPELLING, so the same literal was `hi` beside a column
// comparison (kernel.ByteaConstText) and the six bytes `\x6869` beside
// COALESCE.
//
// Scope, deliberately: bytea only, and only where every operand that is not
// an unknown literal is DECLARED bytea. A construct holding a text operand
// beside a bytea one is PostgreSQL's 42804 / text resolution, not this rule;
// an operand the declaration walk cannot type leaves the construct exactly as
// it was. A literal byteain refuses is CAST all the same, so the CAST's own
// 22023 / 22P02 is raised where the value is read; the plan-time refusal is
// the binder's (refuseByteaLiterals).
//
// It runs from AnnotateScanColumns beside typeMemberLiterals, for the same
// reason: every entry calls it, the optimizer calls it again after its
// rewrites, and a typed literal is a CAST no rule here matches again, so a
// second run changes nothing.
func (p *Planner) typeByteaLiterals(root *logical.Node) {
	if p == nil || root == nil {
		return
	}
	w := newDeclWalk()
	// node returns the GROUP BY keys an Aggregate at or below n typed. A
	// select item, HAVING or ORDER BY term above the Aggregate names a key in
	// one of two ways, and both have to follow it or the term stops naming
	// its key: by the key's own subtree (`SELECT COALESCE(b, 'hi') … GROUP BY
	// COALESCE(b, 'hi')`), typed here under the Aggregate's declarations; or
	// by a column reference to the slot the key is published under, whose
	// name is the key's TEXT exactly as GroupBy holds it (the logical
	// builder's computedGroupKeyRefs; a HAVING term is lowered to one),
	// renamed here to the typed key's. The key's text itself (GroupBy) is
	// re-spelled too, because the stage DAG computes a key no select item
	// spells from that text. They travel up the single-input chain only.
	var node func(n *logical.Node) *byteaGroupKeys
	node = func(n *logical.Node) *byteaGroupKeys {
		if n == nil {
			return nil
		}
		var keys *byteaGroupKeys
		for _, c := range n.Children {
			k := node(c)
			if len(n.Children) == 1 {
				keys = k
			}
		}
		if isSetOpNode(n) {
			typeByteaSetOpArms(n)
		}
		var decls ColDecls
		ready := false
		in := func() ColDecls {
			if !ready {
				src := n
				if len(n.Children) == 1 && n.Type != logical.NodeScan {
					src = n.Children[0]
				}
				decls = withSubqueryDecls(w.inputColDecls(src), n)
				ready = true
			}
			return decls
		}
		typeIn := func(e plansql.Node) bool {
			if e == nil {
				return false
			}
			changed := keys.respell(e)
			if !hasQuotedLiteral(e) {
				return changed
			}
			return typeByteaLiteralsIn(e, in()) || changed
		}
		for _, preds := range [][]logical.Predicate{n.Predicates, n.ScanPredicates} {
			for i := range preds {
				// The DAG reads a filter back from its TEXT (Raw), so a typed
				// literal is written into both, as typeMemberLiterals does.
				if typeIn(preds[i].ASTExpr) {
					preds[i].Raw = preds[i].ASTExpr.String()
				}
			}
		}
		for i := range n.Projections {
			// The stage DAG computes a select item from its TEXT (Expr), as
			// it reads a filter from Raw, so a typed item is written into
			// both. The item's output NAME is not its text (an unaliased
			// item is named by its node: `coalesce`, `?column?`).
			if typeIn(n.Projections[i].ASTExpr) && n.Projections[i].Expr != "" {
				n.Projections[i].Expr = n.Projections[i].ASTExpr.String()
			}
		}
		// A join's condition and keys are TEXT the join planners parse
		// (parseJoinKeys, the stage DAG's key lists), so a typed construct
		// there is written back as text — or `a.x = COALESCE(kb, '\x6869')`
		// over a derived `COALESCE(b, '\x6869') AS x` compared a typed value
		// with a spelled one.
		if n.Type == logical.NodeJoin {
			jd := joinSideDecls(w, n)
			typeText := func(s string) string {
				if s == "" || !strings.Contains(s, "'") {
					return s
				}
				e := parseJoinCondExpr(s)
				if e == nil || !typeByteaLiteralsIn(e, jd) {
					return s
				}
				return e.String()
			}
			n.JoinCond = typeText(n.JoinCond)
			n.JoinFilter = typeText(n.JoinFilter)
			for i := range n.LeftKeys {
				n.LeftKeys[i] = typeText(n.LeftKeys[i])
			}
			for i := range n.RightKeys {
				n.RightKeys[i] = typeText(n.RightKeys[i])
			}
		}
		// A window's PARTITION BY and ORDER BY terms are text the window
		// operator computes (or, over an Aggregate, reads as the key's
		// slot), with the parsed term beside it; both follow.
		for i := range n.WindowExprs {
			we := &n.WindowExprs[i]
			for j := range we.PartitionBy {
				var term plansql.Node
				if j < len(we.PartitionByExprs) {
					term = we.PartitionByExprs[j]
				}
				if term == nil {
					term = parseJoinCondExpr(we.PartitionBy[j])
				}
				if term != nil && typeIn(term) {
					we.PartitionBy[j] = term.String()
					if j < len(we.PartitionByExprs) {
						we.PartitionByExprs[j] = term
					}
				}
			}
			for j := range we.OrderBy {
				var term plansql.Node
				if j < len(we.OrderByExprs) {
					term = we.OrderByExprs[j]
				}
				if term == nil {
					term = parseJoinCondExpr(we.OrderBy[j].Column)
				}
				if term != nil && typeIn(term) {
					we.OrderBy[j].Column = term.String()
					if j < len(we.OrderByExprs) {
						we.OrderByExprs[j] = term
					}
				}
			}
		}
		for i := range n.AggExprs {
			for _, e := range []plansql.Node{n.AggExprs[i].InputExpr, n.AggExprs[i].InputExpr2, n.AggExprs[i].InputExpr3} {
				typeIn(e)
			}
		}
		if n.Type == logical.NodeAggregate {
			keys = nil
			for i, g := range n.GroupByExprs {
				if g == nil || !hasQuotedLiteral(g) {
					continue
				}
				before := g.String()
				if !typeByteaLiteralsIn(g, in()) {
					continue
				}
				if keys == nil {
					keys = &byteaGroupKeys{decls: map[string]ColDecls{}, names: map[string]string{}}
				}
				keys.decls[before] = in()
				if i < len(n.GroupBy) && strings.EqualFold(cleanExpr(n.GroupBy[i]), cleanExpr(before)) {
					oldText := n.GroupBy[i]
					old := strings.ToLower(oldText)
					n.GroupBy[i] = g.String()
					// The new slot name is the typed text AS WRITTEN, the name
					// the Aggregate publishes the key and its distribution
					// under: the stage DAG compares a window's PARTITION BY
					// with that distribution byte for byte (dagplan keysEqual),
					// and `cast(… as bytes)` against `cast(… as BYTES)` refused
					// the plan's exchange consistency. The old name stays the
					// folded lookup key, as a reference is matched folded.
					keys.names[old] = n.GroupBy[i]
					// A grouping set and a GROUPING(...) argument name a
					// key by the same TEXT (aggregate_plan.go's keyIndex),
					// so each follows the key it names: a set left on the
					// old spelling matched no key and became empty, which
					// turned ROLLUP (b || '\x41') into two grand totals,
					// and GROUPING(...) on it a 42803.
					respellKeyText(n.GroupingSets, oldText, n.GroupBy[i])
					for c := range n.GroupingCalls {
						for a, arg := range n.GroupingCalls[c].Args {
							if sameKeyText(arg, oldText) {
								n.GroupingCalls[c].Args[a] = n.GroupBy[i]
							}
						}
					}
				}
			}
		} else if len(n.Children) != 1 {
			keys = nil
		}
		return keys
	}
	node(root)
}

// joinSideDecls is what a join's condition can name: each side's emitted
// declarations, merged, a name the two sides type differently dropped (the
// rule inputColTypes applies to two scans). inputColTypes itself stops at a
// derived table's Project, which is the side a join over a derived
// `COALESCE(b, '\x6869') AS x` has.
func joinSideDecls(w *declWalk, n *logical.Node) ColDecls {
	merged := map[string]parquet.TypeID{}
	drop := map[string]bool{}
	for _, c := range n.Children {
		for k, t := range w.childDecls(c).Types {
			if prev, dup := merged[k]; dup && prev != t {
				drop[k] = true
			}
			merged[k] = t
		}
	}
	for k := range drop {
		delete(merged, k)
	}
	return ColDecls{Types: merged, walk: w}
}

// sameKeyText is the comparison aggregate_plan.go's keyIndex makes between a
// key's text and a grouping set's or GROUPING argument's: trimmed, folded.
func sameKeyText(a, b string) bool {
	return strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b))
}

// respellKeyText renames every grouping-set entry spelling old to new.
func respellKeyText(sets [][]string, old, new string) {
	for s := range sets {
		for c, col := range sets[s] {
			if sameKeyText(col, old) {
				sets[s][c] = new
			}
		}
	}
}

// byteaGroupKeys is what an Aggregate's typed GROUP BY keys hand the terms
// above it: each key's subtree spelling before the CAST with the
// declarations it was typed under, and each key's published slot name, old
// to new.
type byteaGroupKeys struct {
	decls map[string]ColDecls
	names map[string]string
}

// respell types, in place, every subtree of e that spells a typed key, and
// renames every unqualified column reference to a typed key's slot.
func (k *byteaGroupKeys) respell(e plansql.Node) bool {
	if k == nil {
		return false
	}
	changed := false
	var walk func(plansql.Node)
	walk = func(n plansql.Node) {
		if n == nil {
			return
		}
		if ref, ok := n.(*plansql.ColRef); ok {
			if ref.Table == "" {
				if to, ok := k.names[strings.ToLower(ref.Column)]; ok {
					ref.Column = to
					changed = true
				}
			}
			return
		}
		if d, ok := k.decls[n.String()]; ok {
			changed = typeByteaLiteralsIn(n, d) || changed
			return
		}
		for _, c := range exprOperands(n) {
			walk(c)
		}
	}
	walk(e)
	return changed
}

// hasQuotedLiteral is the cheap gate: an expression with no quoted literal is
// left without a declaration walk.
func hasQuotedLiteral(e plansql.Node) bool {
	found := false
	var walk func(plansql.Node)
	walk = func(n plansql.Node) {
		if n == nil || found {
			return
		}
		if lit, ok := n.(*plansql.Lit); ok && lit.Kind == plansql.LitString {
			found = true
			return
		}
		for _, c := range exprOperands(n) {
			walk(c)
		}
	}
	walk(e)
	return found
}

// typeByteaLiteralsIn types, in place, every unknown literal in e that a
// construct resolves to bytea, and reports whether it changed anything.
// Children first: an inner construct typed bytea is then a bytea operand of
// the one around it, as PostgreSQL analyses inside-out.
func typeByteaLiteralsIn(e plansql.Node, decls ColDecls) bool {
	changed := false
	var walk func(plansql.Node)
	walk = func(n plansql.Node) {
		if n == nil {
			return
		}
		for _, c := range exprOperands(n) {
			walk(c)
		}
		switch v := n.(type) {
		case *plansql.FuncCallNode:
			switch strings.ToLower(v.Name) {
			case "coalesce", "ifnull", "greatest", "least", "nullif":
				if byteaCommon(v.Args, decls) {
					changed = castByteaLiterals(v.Args) || changed
				}
			case "position", "strpos":
				if len(v.Args) == 2 && byteaCommon(v.Args, decls) {
					changed = castByteaLiterals(v.Args) || changed
				}
			}
		case *plansql.CaseNode:
			results := make([]plansql.Node, 0, len(v.Whens)+1)
			for _, w := range v.Whens {
				results = append(results, w.Result)
			}
			if v.Else != nil {
				results = append(results, v.Else)
			}
			if byteaCommon(results, decls) && castByteaLiterals(results) {
				for i := range v.Whens {
					v.Whens[i].Result = results[i]
				}
				if v.Else != nil {
					v.Else = results[len(results)-1]
				}
				changed = true
			}
			// A simple CASE compares its subject with each WHEN operand
			// (PostgreSQL resolves `subject = operand` per arm). A bytea
			// subject that is an EXPRESSION — COALESCE(b, '\x41') once typed
			// above, GREATEST(…), CAST(b AS BYTES) — met a quoted WHEN
			// operand as its spelling; a bare column subject is already
			// compared through byteain (the comparison kernels, #582) and is
			// left as it is, as byteaCompared leaves a column comparison.
			if v.Subject != nil {
				ops := []plansql.Node{v.Subject}
				for _, w := range v.Whens {
					ops = append(ops, w.Cond)
				}
				if byteaCompared(ops, decls) && castByteaLiterals(ops) {
					v.Subject = ops[0]
					for i := range v.Whens {
						v.Whens[i].Cond = ops[i+1]
					}
					changed = true
				}
			}
		case *plansql.CmpExpr:
			pair := []plansql.Node{v.Left, v.Right}
			if byteaCompared(pair, decls) && castByteaLiterals(pair) {
				v.Left, v.Right = pair[0], pair[1]
				changed = true
			}
		case *plansql.InExpr:
			ops := append([]plansql.Node{v.Left}, v.Values...)
			if byteaCompared(ops, decls) && castByteaLiterals(ops) {
				v.Left = ops[0]
				copy(v.Values, ops[1:])
				changed = true
			}
		case *plansql.BetweenExpr:
			ops := []plansql.Node{v.Left, v.Low, v.High}
			if byteaCompared(ops, decls) && castByteaLiterals(ops) {
				v.Left, v.Low, v.High = ops[0], ops[1], ops[2]
				changed = true
			}
		case *plansql.BinaryOp:
			if v.Op == "||" {
				pair := []plansql.Node{v.Left, v.Right}
				if byteaCommon(pair, decls) && castByteaLiterals(pair) {
					v.Left, v.Right = pair[0], pair[1]
					changed = true
				}
			}
		}
	}
	walk(e)
	return changed
}

// byteaCommon reports that the operands resolve to bytea: at least one is
// DECLARED bytea, at least one is an unknown quoted literal, and every other
// one is bytea or unknown (a quoted literal or a NULL).
func byteaCommon(ops []plansql.Node, decls ColDecls) bool {
	bytea, lit := false, false
	for _, o := range ops {
		if o == nil {
			return false
		}
		if l, ok := plansql.Unparen(o).(*plansql.Lit); ok {
			switch l.Kind {
			case plansql.LitString:
				lit = true
				continue
			case plansql.LitNull:
				continue
			}
			return false
		}
		d, c := nodeDeclaredType(o, decls)
		if c != expr.Decided || d.ID != parquet.TypeBytes {
			return false
		}
		bytea = true
	}
	return bytea && lit
}

// byteaCompared is byteaCommon for a COMPARISON (=, <, IS DISTINCT FROM, IN,
// BETWEEN), restricted to one whose bytea operands are not all bare
// columns. A literal compared with a bytea COLUMN is already read by byteain
// in the comparison kernels (kernel.ByteaConstText, #582) and the scan's
// pushdown and pruning read that shape, so it is left exactly as it is; a
// bytea EXPRESSION (`COALESCE(b, …) = '\x6869'`, a typed constant) was
// compared with the literal's spelling.
func byteaCompared(ops []plansql.Node, decls ColDecls) bool {
	if !byteaCommon(ops, decls) {
		return false
	}
	for _, o := range ops {
		u := plansql.Unparen(o)
		if l, ok := u.(*plansql.Lit); ok && (l.Kind == plansql.LitString || l.Kind == plansql.LitNull) {
			continue
		}
		if _, ok := u.(*plansql.ColRef); !ok {
			return true
		}
	}
	return false
}

// castByteaLiterals replaces each quoted literal operand with its CAST to
// BYTES, in the slice, and reports whether it replaced one.
func castByteaLiterals(ops []plansql.Node) bool {
	changed := false
	for i, o := range ops {
		if l, ok := plansql.Unparen(o).(*plansql.Lit); ok && l.Kind == plansql.LitString {
			ops[i] = byteaLiteralCast(l)
			changed = true
		}
	}
	return changed
}

// byteaLiteralCast is the literal's CAST to bytea spelled as the parser
// spells a written `CAST('…' AS BYTES)`: type name lower-cased. The typed
// text becomes a key the stage DAG compares byte for byte (an Aggregate's
// distribution, a window's PARTITION BY, an exchange's keys), and a planner
// walk that folds a name to lower case on the way (a derived table's alias
// resolved to its source) then still meets the same text. Spelled `BYTES`,
// `GROUP BY COALESCE(b, '\x6869')` under a window over the derived table
// refused its exchange consistency where the written CAST runs.
func byteaLiteralCast(l *plansql.Lit) plansql.Node {
	return &plansql.CastNode{Inner: l, TypeName: "bytes"}
}

// typeByteaSetOpArms types a set operation's unknown-literal select items
// when the column's other arms are all declared bytea (PostgreSQL's
// select_common_type over the arms, the literal taking the result's type).
// A VALUES list is the same rule over its rows.
func typeByteaSetOpArms(n *logical.Node) {
	outNames := setOpOutputNames(n.Children[0])
	if len(outNames) == 0 {
		return
	}
	type arm struct {
		proj *logical.Node
		plan SetOpArmPlan
	}
	arms := make([]arm, 0, len(n.Children))
	for _, child := range n.Children {
		plan, err := setOpArmProjection(child, outNames)
		if err != nil {
			return
		}
		var proj *logical.Node
		if !isSetOpNode(setOpUnwrap(child)) {
			proj = findOutputProjectionNode(child)
			if proj != nil && len(proj.Projections) != len(outNames) {
				proj = nil
			}
		}
		arms = append(arms, arm{proj: proj, plan: plan})
	}
	for col := range outNames {
		bytea, lit := false, false
		for _, a := range arms {
			if a.proj != nil {
				if l, ok := plansql.Unparen(a.proj.Projections[col].ASTExpr).(*plansql.Lit); ok {
					if l.Kind == plansql.LitString {
						lit = true
						continue
					}
					if l.Kind == plansql.LitNull {
						continue
					}
				}
			}
			if col >= len(a.plan.Types) || !a.plan.Types[col].Known || a.plan.Types[col].Typ != parquet.TypeBytes {
				bytea, lit = false, false
				break
			}
			bytea = true
		}
		if !bytea || !lit {
			continue
		}
		for _, a := range arms {
			if a.proj == nil {
				continue
			}
			pr := &a.proj.Projections[col]
			if l, ok := plansql.Unparen(pr.ASTExpr).(*plansql.Lit); ok && l.Kind == plansql.LitString {
				pr.ASTExpr = byteaLiteralCast(l)
			}
		}
	}
}
