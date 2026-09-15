package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A LIFTED CORRELATED PREDICATE IS EVALUATED OVER THE BODY'S OUTPUT, SO THE
// BODY PUBLISHES WHAT IT NAMES.
//
// The decorrelation lifts a correlated WHERE predicate out of the body and
// evaluates it at or above the JOIN, over the body's OUTPUT rows. For an
// EQUALITY the key loop already guarantees the inner column is there. A
// predicate that is NOT an equality took none of that path, so where the body
// RENAMES the column or does not select it, the condition named nothing:
//
//	SELECT s.m FROM lat_ord o LEFT JOIN LATERAL (
//	  SELECT i.amount AS m FROM lat_item i WHERE i.amount < o.total) s ON true
//	-- PostgreSQL 17.11: 9 rows   single / spilled: 3 NULL-padded rows
//	                              the three DAG arms: PostgreSQL's 9
//
// **The DAG was right by MECHANISM, not by accident**, which is what the
// round-2 review established and what this repair follows: its stage plan
// evaluates the predicate where the inner column still exists —
// `AG/liftedScaleRenamed` lands on PostgreSQL's 177 500 rows over 40×5 000 with
// the column published under NO name at all. An earlier version of this file
// REFUSED the shape uniformly, which turned four right answers into `0A000`.
//
// The repair gives the single-process path the same evaluation point: the
// inner columns the predicate names are materialized into hidden slots of the
// body's own projection and the predicate is respelled to them. Two placements
// exist and the difference is measured, not guessed:
//
//   - the column is published under its OWN name and the predicate is NOT
//     respelled. A first attempt minted `__key_N` slots and respelled to them:
//     the two single-process arms became right and the three DAG arms went
//     from PostgreSQL's nine rows to three NULL-padded ones, because the
//     minted name is one the DAG's evaluation point does not carry. What the
//     single path lacked was not a NAME but the COLUMN.
//   - the column is EMITTED by the join and hidden from a STAR only
//     (`Node.StarLiftedRefCols`). `HiddenJoinCols` is both properties at once,
//     and a predicate the physical planner routes to a FILTER ABOVE the join —
//     where a non-equi residual goes when an equality beside it keys the join
//     — reads the join's OUTPUT: dropping answered ZERO rows there.
//
// An AGGREGATED body is still refused, and for a reason the projection cannot
// answer: publishing `i.amount` beside `SUM(i.amount)` needs it in the GROUP
// BY, which changes what the aggregate computes.
//
// publishLiftedRefs materializes every inner column a LIFTED correlated
// predicate names, so the predicate can be EVALUATED wherever the planner puts
// it, and respells the predicate to those slots.
//
// It returns the slots and whether they may be DROPPED at the join. They may
// when every correlated part is a non-equality: the whole correlation is then
// the join's own `ON`, evaluated over the pair before the output mapping runs.
// They may NOT when an equality rides along — the physical planner keys on the
// equality and routes the non-equi residual to a FILTER ABOVE the join, which
// reads the join's OUTPUT, and a dropped slot answers zero rows there. Those
// slots are hidden from a STAR instead (`Node.StarLiftedRefCols`).
//
// An AGGREGATED body is still refused: there is no projection to publish the
// column in at all, and publishing it would put it in the GROUP BY.
func publishLiftedRefs(info *plansql.SelectInfo, correlatedParts []string,
	leftAliases map[string]bool, aggregates bool,
	alloc *plansql.SlotAllocator, injected *[]plansql.SelectColumn) (slots []string, droppable bool, err error) {
	droppable = true
	for _, cp := range correlatedParts {
		if extractInnerColumn(cp, leftAliases) != "" {
			droppable = false // an equality keys the join; the rest routes above it
		}
	}
	for i, cp := range correlatedParts {
		if extractInnerColumn(cp, leftAliases) != "" {
			continue
		}
		node, perr := plansql.ParseExpression(cp)
		if perr != nil || node == nil {
			continue
		}
		repl := map[string]string{}
		var mint []string
		walkExprNodes(node, func(x plansql.Node) {
			ref, ok := x.(*plansql.ColRef)
			if !ok || ref.Column == "" || leftAliases[strings.ToLower(ref.Table)] {
				return
			}
			k := strings.ToLower(ref.Column)
			if _, dup := repl[k]; dup {
				return
			}
			// Published under its OWN name (or through a star) needs no slot:
			// the body already emits the column the predicate names. An ALIAS
			// is not the same thing — the alias is the only name the
			// projection emits — so a renamed column takes the full mint.
			if _, renamed := lateralPublishedKeyName(info.Columns, ref.Column); !renamed &&
				lateralSelectsColumn(info.Columns, ref.Column) {
				return
			}
			repl[k] = ""
			mint = append(mint, ref.Column)
		})
		if len(mint) == 0 {
			continue
		}
		if aggregates {
			return nil, false, sqlerr.New("0A000",
				"LATERAL body AGGREGATES and its correlated predicate %s is not an "+
					"equality: the predicate is evaluated over the body's OUTPUT and an "+
					"aggregated body publishes no column to evaluate it against — "+
					"publishing one would put it in the GROUP BY and change what the "+
					"aggregate computes. PostgreSQL evaluates the body per outer row, "+
					"which this engine does not do for this shape. Aggregate in the "+
					"ENCLOSING query over an unaggregated lateral instead",
				sqlerr.Quote(strings.TrimSpace(cp)))
		}
		// PUBLISHED UNDER ITS OWN NAME, and the predicate is NOT respelled.
		// The two paths resolve it differently and only the source name is
		// spelled the same in both: the DAG's stage plan reads the column off
		// a stream that carries the scan's own names (which is why it answers
		// this shape today), and respelling to a minted slot took that away —
		// measured, `B/orig` went from PostgreSQL's nine rows on the three DAG
		// arms to three NULL-padded ones. What the single-process path lacked
		// was not a NAME but the COLUMN, which its projection had dropped.
		for _, col := range mint {
			item, ok := lateralKeySelectItem(col)
			if !ok {
				continue
			}
			bare := lateralBareKeyName(col)
			if bare == "" {
				bare = strings.TrimSpace(col)
			}
			item.Alias = bare
			*injected = append(*injected, item)
			slots = append(slots, bare)
		}
		_ = i
	}
	return slots, droppable, nil
}
