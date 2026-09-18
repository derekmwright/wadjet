// SPDX-License-Identifier: MIT

package sql

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A BARE REFERENCE TO A `JOIN … USING` MERGED COLUMN IN A SORT OR WINDOW KEY
// BINDS THE MERGE, NOT THE LEFT ARM.
//
// `USING (c)` merges the joined column into ONE output column. For an INNER or
// LEFT join that column's VALUE is the left arm's `c` — the side that is never
// NULL-extended — so a key that resolves to the left arm's column is already
// the merged value and nothing here has to move. For a RIGHT join it is the
// RIGHT arm's, and for a FULL join it is `COALESCE(left.c, right.c)`, because
// either side may be the NULL-extended one.
//
// `ORDER BY c` is resolved and planned BELOW the projection that states the
// merge, against the join's own stream, where the only column of that name is
// the LEFT arm's. So `psb FULL JOIN psa USING (id) ORDER BY id` sorted by
// `psb.id`, whose NULL is exactly the row whose merged value came from the
// other side: rows 2, 3, 1 for PostgreSQL 17.11's 1, 2, 3, and under
// `LIMIT 1` a different ROW, and under `OFFSET 1` a different ROW SET
// (measured by the round-1 review, B1).
//
// The binding is fixed HERE, at the one place the clause is read and the two
// sides' names are known without a catalog: the key is rewritten onto the
// merged EXPRESSION. What the engine can then do with that expression is not
// this pass's question — a RIGHT join's merged key is a plain qualified column
// and sorts on every arm; a FULL join's is computed, which a bare `SELECT *`
// over more than one relation cannot carry (order_by_keys.go's own bound) and
// which a named select list carries fine.
//
// Only RIGHT and FULL are rewritten. Rewriting an INNER or LEFT key would
// change the rendering of every existing USING query for no change in meaning.

// bindMergedUsingKeys rewrites every bare sort or window key naming a column
// merged by a RIGHT or FULL `JOIN … USING` onto that merge's expression.
//
// It applies to the narrow shape whose merge this parser can state without a
// catalog: ONE FROM item carrying ONE join. A chain puts more than one
// relation on the left and a comma list more than one item, and which of them
// a bare name belongs to is a catalog question — those keep the binding they
// had, which is the same conservative bound `parseJoinUsing` already draws.
func bindMergedUsingKeys(info *SelectInfo) error {
	if info == nil || len(info.Tables) != 1 || len(info.Joins) != 1 {
		return nil
	}
	ji := info.Joins[0]
	if len(ji.Using) == 0 {
		return nil
	}
	kind := strings.ToLower(strings.TrimSpace(ji.Type))
	right := kind == "right join"
	full := kind == "full outer join" || kind == "full join"
	if !right && !full {
		return nil
	}
	leftQual := info.Tables[0].Alias
	if leftQual == "" {
		leftQual = info.Tables[0].Name
	}
	rightQual := ji.RightAlias
	if rightQual == "" {
		rightQual = ji.RightTable
	}
	if leftQual == "" || rightQual == "" {
		return nil
	}

	merged := make(map[string]bool, len(ji.Using))
	for _, c := range ji.Using {
		merged[strings.ToLower(c)] = true
	}
	// AN OUTPUT ALIAS OF THE SAME NAME. PostgreSQL resolves a bare ORDER BY
	// term against the OUTPUT list before the input relation, so `SELECT b AS
	// id … ORDER BY id` names the select item and not the merge. This pass
	// does not arbitrate that; where the two could collide it does nothing and
	// the binding is the one the statement had.
	for _, col := range info.Columns {
		if col.Alias != "" && merged[strings.ToLower(col.Alias)] {
			return nil
		}
	}

	mergedExpr := func(col string) Node {
		if right {
			return &ColRef{Table: rightQual, Column: col}
		}
		return &FuncCallNode{Name: "coalesce", Args: []Node{
			&ColRef{Table: leftQual, Column: col},
			&ColRef{Table: rightQual, Column: col},
		}}
	}

	// ORDER BY. The term carries both a TEXT and an AST and the two have to
	// stay one thing, so the text is re-derived from the rewritten tree.
	for i := range info.OrderBy {
		if info.OrderBy[i].Expr == nil {
			continue
		}
		before := info.OrderBy[i].Expr.String()
		rewritten := RewriteExpr(info.OrderBy[i].Expr, func(n Node) (Node, bool) {
			cr, ok := n.(*ColRef)
			if !ok || cr.Table != "" || !merged[strings.ToLower(cr.Column)] {
				return nil, false
			}
			return mergedExpr(cr.Column), true
		})
		if rewritten == nil || rewritten.String() == before {
			continue
		}
		info.OrderBy[i].Expr = rewritten
		info.OrderBy[i].Column = rewritten.String()
	}

	// WINDOW items live in TWO places that have to agree: the `WindowSpec`
	// the planner reads, and the `WindowFuncNode` that spec is DERIVED from.
	// Rewriting only the spec is thrown away — `unfoldFromlessScalars` rebuilds
	// every window item's spec from its node one call later
	// (fromless_scalar.go), which is how a RIGHT join's window key went back to
	// the left arm and answered wrong VALUES on all five arms for a shape this
	// pass claimed to bind (review round 2, B1-r2). Both are rewritten here,
	// and parser.go runs this pass again after the rebuild.
	//
	// A window's ARGUMENT carries the same bare reference — `SUM(id) OVER (…)`
	// read the left arm's column and summed NULLs where PostgreSQL sums the
	// merged key — and an argument is an EXPRESSION, so it takes the merged
	// expression for a FULL join as readily as for a RIGHT one.
	//
	// A window KEY is not: its slot in `WindowSpec` holds a column NAME, so a
	// RIGHT join's merged key (a plain qualified column) is written there and a
	// FULL join's (a COALESCE) is REFUSED rather than silently bound to the
	// left arm.
	for i := range info.Columns {
		ws := info.Columns[i].WindowSpec
		if ws == nil {
			continue
		}
		if full {
			for _, key := range ws.PartitionBy {
				if name, ok := mergedWindowKey(key, merged); ok {
					return refuseFullMergedWindowKey(name, "PARTITION BY", leftQual, rightQual)
				}
			}
			for _, key := range ws.OrderBy {
				if name, ok := mergedWindowKey(key.Column, merged); ok {
					return refuseFullMergedWindowKey(name, "ORDER BY", leftQual, rightQual)
				}
			}
		} else {
			for j, key := range ws.PartitionBy {
				if name, ok := mergedWindowKey(key, merged); ok {
					ws.PartitionBy[j] = rightQual + "." + name
				}
			}
			for j, key := range ws.OrderBy {
				if name, ok := mergedWindowKey(key.Column, merged); ok {
					ws.OrderBy[j].Column = rightQual + "." + name
				}
			}
		}
		wfn, ok := info.Columns[i].ASTExpr.(*WindowFuncNode)
		if !ok {
			continue
		}
		bind := func(n Node) Node {
			if n == nil {
				return nil
			}
			return RewriteExpr(n, func(x Node) (Node, bool) {
				cr, isCol := x.(*ColRef)
				if !isCol || cr.Table != "" || !merged[strings.ToLower(cr.Column)] {
					return nil, false
				}
				return mergedExpr(cr.Column), true
			})
		}
		if wfn.Func != nil {
			for j := range wfn.Func.Args {
				wfn.Func.Args[j] = bind(wfn.Func.Args[j])
			}
		}
		if !full {
			for j := range wfn.PartitionBy {
				wfn.PartitionBy[j] = bind(wfn.PartitionBy[j])
			}
			for j := range wfn.OrderBy {
				wfn.OrderBy[j].Expr = bind(wfn.OrderBy[j].Expr)
			}
		}
	}
	return nil
}

// mergedWindowKey reports whether a window key is a BARE reference to one of
// the merged columns, and which one. A qualified key names a side and is left
// alone; anything computed is not a bare reference.
func mergedWindowKey(key string, merged map[string]bool) (string, bool) {
	k := strings.ToLower(strings.TrimSpace(key))
	if k == "" || strings.ContainsAny(k, ".( ") {
		return "", false
	}
	if !merged[k] {
		return "", false
	}
	return k, true
}

// refuseFullMergedWindowKey is the one shape this pass cannot rewrite: a
// window key naming a FULL join's merged column, whose value is a COALESCE and
// whose key slot holds a column NAME. Refused rather than left bound to the
// left arm, which is a value the merged column does not have on the rows the
// left arm did not supply.
func refuseFullMergedWindowKey(name, clause, leftQual, rightQual string) error {
	return sqlerr.New("0A000",
		"a window %s key naming %q, which a FULL JOIN ... USING merges, is not supported: "+
			"the merged value is COALESCE(%s.%s, %s.%s) and a window key here is a column "+
			"name. Write the expression, or write the join condition with ON",
		clause, name, leftQual, name, rightQual, name)
}
