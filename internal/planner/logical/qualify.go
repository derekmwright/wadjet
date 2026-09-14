package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// QUALIFY IS A FILTER OVER THE WINDOW'S OUTPUT, AND IT IS DUCKDB'S CLAUSE.
//
// PostgreSQL has no `QUALIFY`, so it cannot be the oracle for it; DuckDB
// 1.1.3 — where Snowflake's and BigQuery's clause has a second, checkable
// implementation — is, and ADR-0012 records that. Everything this file
// encodes was measured there over the `lat_ord`/`lat_item` rows:
//
//	SELECT u.id, ROW_NUMBER() OVER (ORDER BY u.id) AS rn FROM lat_ord u QUALIFY rn = 1
//	  -> one row. The parser has accepted this since the clause was added and
//	     nothing consumed `SelectInfo.Qualify`, so every row came back: a
//	     PLAUSIBLE SUPERSET, which is the one wrong answer a client cannot
//	     detect. #1076.
//
// Four facts decide the lowering, and each is a cell of
// `coordinator.TestArcL1QualifyAnswersDuckDBOnEveryArm`:
//
//  1. It filters AFTER window evaluation and BEFORE the projection, so it may
//     name a column the SELECT list does not publish
//     (`QUALIFY ROW_NUMBER() OVER (PARTITION BY i.order_id ORDER BY i.amount) = 1`
//     over `SELECT i.product`). A rewrite into a derived table would have to
//     invent that list; a Filter between the Window and the Project does not.
//  2. A window call written INSIDE the clause is evaluated like any other —
//     it takes a slot of the same family and the predicate reads the slot.
//     The Window operator therefore exists for a block whose SELECT list has
//     no window at all.
//  3. A bare name binds the INPUT relation's column where one exists and a
//     SELECT-list ALIAS otherwise. That order is DuckDB's, measured:
//     `SELECT i.amount AS id FROM lat_item i QUALIFY … AND id > 60` answers
//     ZERO rows — `id` is `lat_item.id`, not the alias over `amount`. The
//     alias arm is what makes `QUALIFY rn = 1` work at all, and it reaches a
//     computed alias (`ROW_NUMBER() OVER (…) * 10 AS rn`, `i.amount*2 AS d`)
//     as well as a bare window one.
//  4. A `QUALIFY` with NO window function anywhere — neither in the SELECT
//     list nor in the clause — is an ERROR, not a `WHERE` in disguise:
//     DuckDB's binder says "at least one window function must appear in the
//     SELECT column or QUALIFY clause". Answering it as a filter would make
//     wadjet accept a statement DuckDB rejects, which is the same class of
//     divergence as ignoring it.
//
// What it does NOT do is move the aggregate machinery: a window call inside
// the clause hoists its aggregate ARGUMENTS through the same
// `reuseOrAddAggregate` path a SELECT-list window's do, and the predicate
// itself is re-spelled over an aggregate exactly as `HAVING` is. Both are
// done by the caller, where those maps live.

// qualifyPlan is what a `QUALIFY` clause contributes to one block's plan: the
// window calls written inside it, which the Window operator has to evaluate
// even though nothing projects them, and the predicate with those calls
// replaced by the slots that hold them.
type qualifyPlan struct {
	windows []WindowExpr
	pred    plansql.Node
}

// qualifyWindows lifts every window call in the clause into its own output
// slot, continuing the block's shared counter so a QUALIFY window and a
// SELECT-list window can never land on the same slot.
func qualifyWindows(info *plansql.SelectInfo, winCounter *int) qualifyPlan {
	if info == nil || info.QualifyExpr == nil {
		return qualifyPlan{}
	}
	out := qualifyPlan{pred: info.QualifyExpr}
	wfns := plansql.FindAllWindowFuncs(info.QualifyExpr)
	if len(wfns) == 0 {
		return out
	}
	replacements := make(map[*plansql.WindowFuncNode]string, len(wfns))
	for _, wfn := range wfns {
		if wfn.Func == nil {
			continue
		}
		slot := plansql.SlotName(plansql.SlotWindowOutput, *winCounter)
		*winCounter++
		out.windows = append(out.windows, windowExprFromNode(wfn, slot))
		replacements[wfn] = slot
	}
	if len(replacements) > 0 {
		out.pred = plansql.ReplaceWindowFuncs(out.pred, replacements)
	}
	return out
}

// qualifyWindowTerms is `windowSpecTerms` for the clause: the expressions a
// QUALIFY window will EVALUATE, so an aggregate written in one is hoisted
// into the block's aggregate list like a SELECT-list window's is (#737's
// rule, one clause further along). `QUALIFY ROW_NUMBER() OVER (ORDER BY
// SUM(i.amount) DESC) = 1` over a `GROUP BY` is the shape that needs it.
func qualifyWindowTerms(info *plansql.SelectInfo) []plansql.Node {
	if info == nil || info.QualifyExpr == nil {
		return nil
	}
	var out []plansql.Node
	for _, wfn := range plansql.FindAllWindowFuncs(info.QualifyExpr) {
		if wfn.Func != nil {
			for _, a := range wfn.Func.Args {
				out = append(out, a)
			}
		}
		for _, pb := range wfn.PartitionBy {
			out = append(out, pb)
		}
		for _, ob := range wfn.OrderBy {
			if ob.Expr != nil {
				out = append(out, ob.Expr)
			}
		}
	}
	return out
}

// resolveQualifyNames spells the predicate against what the WINDOW operator
// below the filter actually emits.
//
// The order is fact 3 above: a name the input relation carries is left alone,
// and only a name it does not carry is looked up in the SELECT list — as the
// slot holding its window output, as the expression a nested window was
// rewritten into, or as its own expression. A qualified name (`x.y`) is never
// a SELECT-list alias and is left alone.
func resolveQualifyNames(pred plansql.Node, cols []plansql.SelectColumn,
	bareWin map[int]string, nested map[int]plansql.Node, input *Node) plansql.Node {
	if pred == nil {
		return nil
	}
	emitted := map[string]bool{}
	for _, c := range emittedColumns(input) {
		emitted[strings.ToLower(stripQualifier(c.name))] = true
	}
	return plansql.RewriteExpr(pred, func(n plansql.Node) (plansql.Node, bool) {
		ref, ok := n.(*plansql.ColRef)
		if !ok || ref.Table != "" || ref.Column == "" {
			return nil, false
		}
		name := strings.ToLower(ref.Column)
		if emitted[name] {
			return nil, false
		}
		for i, c := range cols {
			if !strings.EqualFold(plansql.OutputColumnName(c), ref.Column) {
				continue
			}
			if slot, hit := bareWin[i]; hit && slot != "" {
				return &plansql.ColRef{Column: slot}, true
			}
			if rewritten, hit := nested[i]; hit && rewritten != nil {
				return rewritten, true
			}
			if c.ASTExpr != nil {
				return c.ASTExpr, true
			}
			return nil, false
		}
		return nil, false
	})
}

// refuseQualifyWithoutAWindow is fact 4: DuckDB's binder rejects a QUALIFY
// that no window function reaches, and so does this.
//
// `42601` because the statement is malformed rather than unimplemented — the
// clause IS implemented, and this spelling of it has no meaning in the engine
// that defines it.
func refuseQualifyWithoutAWindow(info *plansql.SelectInfo, windowed bool) error {
	if info == nil || info.QualifyExpr == nil || windowed {
		return nil
	}
	return sqlerr.New("42601", "QUALIFY requires a window function: at least one "+
		"must appear in the SELECT list or in the QUALIFY clause itself, because "+
		"QUALIFY filters the rows a window produced. With no window the clause "+
		"says what WHERE says — write it there")
}
