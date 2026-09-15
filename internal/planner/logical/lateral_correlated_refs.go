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
	leftAliases map[string]bool, aggregates bool, outer *plansql.SelectInfo, left *Node,
	injected *[]plansql.SelectColumn) (slots []string, err error) {
	// THE COLUMN IS PUBLISHED, SO IT IS ONLY MATERIALIZED WHERE PUBLISHING IT
	// CHANGES NOTHING ELSE. Four shapes decline, and each returns the query to
	// the disposition it had before this repair existed — never to a new wrong
	// answer (round-4 review, B1r / B2r / B3r / P1r):
	//
	//   - the body carries DISTINCT or GROUP BY. Both are computed over the
	//     projection, so a widened projection is a different key: a DISTINCT
	//     body answered nine rows for PostgreSQL's seven, silently, on every
	//     arm.
	//   - one of the body's OWN output aliases already publishes that name.
	//     `SELECT i.id AS amount` beside a predicate naming `i.amount` gave
	//     the join two columns called `amount`, and `s.amount` bound the
	//     injected one on the three DAG arms — a cell RIGHT on five arms
	//     before.
	//   - the ENCLOSING relation publishes that name. The lifted predicate
	//     then reads the outer column instead of the body's, which is the
	//     `i.id < o.id` shape.
	//   - the enclosing query writes a STAR over this join. A bare `SELECT *`
	//     publishes the join's stream, and the materialized column is in it —
	//     on all nine doors, in `RowDescription`. The QUALIFIED star reads the
	//     body's own list and is already clean (`Node.StarLiftedRefCols`).
	//
	// The first three are read off the body and the enclosing query; the
	// fourth off the enclosing SELECT list. None of them can be decided after
	// the plan is built, which is why they are conditions on the injection and
	// not a repair above it.
	//
	// EVERY ONE OF THEM IS TESTED AFTER THE AGGREGATED REFUSAL, inside the
	// loop — the star test sets a flag rather than returning, for exactly that
	// reason. A decline on an aggregated body is a silent wrong answer, so
	// while the refusal and the declines look alike (neither materializes
	// anything), they are not interchangeable: round 4 left the star test as a
	// return above the loop and one statement got two dispositions decided by
	// the ENCLOSING SELECT list — a named list refused, `SELECT *` answered
	// three NULLs for PostgreSQL's `350 | 350 | NULL` on all five arms. That
	// ordering is no longer a comment: `TestNoLiftedRefDeclineSitsBeforeThe`
	// `AggregatedRefusal` reads it off this function's source, and
	// `TestTheAggregatedRefusalPrecedesEveryLiftedRefDecline` crosses every
	// trigger above with an aggregated body.
	if info == nil {
		return nil, nil
	}
	enclosingStar := false
	if outer != nil {
		for _, c := range outer.Columns {
			if c.Star {
				enclosingStar = true
			}
		}
	}
	outerNames := map[string]bool{}
	for _, e := range emittedColumns(left) {
		outerNames[strings.ToLower(stripQualifier(e.name))] = true
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
		contested := false
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
			// A name the ENCLOSING relation carries, or one the body's own
			// list already publishes as an ALIAS over some other value, is a
			// name this pass may not add a second column under.
			bare := strings.ToLower(lateralBareKeyName(ref.Column))
			if bare == "" {
				bare = strings.ToLower(strings.TrimSpace(ref.Column))
			}
			if outerNames[bare] || lateralAliasPublishes(info.Columns, bare) {
				contested = true
			}
			repl[k] = ""
			mint = append(mint, ref.Column)
		})
		if len(mint) == 0 {
			continue
		}
		// THE AGGREGATED REFUSAL COMES FIRST, and two tests hold it there. It
		// is a statement about the BODY — there is no projection to publish
		// the column in — and not about what publishing it would disturb, so
		// a shape that would also decline below must still be refused rather
		// than silently answered.
		if aggregates {
			return nil, sqlerr.New("0A000",
				"LATERAL body AGGREGATES and its correlated predicate %s is not an "+
					"equality: the predicate is evaluated over the body's OUTPUT and an "+
					"aggregated body publishes no column to evaluate it against — "+
					"publishing one would put it in the GROUP BY and change what the "+
					"aggregate computes. PostgreSQL evaluates the body per outer row, "+
					"which this engine does not do for this shape. Aggregate in the "+
					"ENCLOSING query over an unaggregated lateral instead",
				sqlerr.Quote(strings.TrimSpace(cp)))
		}
		// A CONTESTED NAME, a DISTINCT the widened projection would change, or
		// an enclosing STAR whose published list it would enter, DECLINES: the
		// query keeps the disposition it had before this repair existed, which
		// is never a new wrong answer. (A GROUPED body needs no arm here — it
		// aggregates, so the refusal above has already fired.)
		if contested || info.Distinct || enclosingStar {
			return nil, nil
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
	return slots, nil
}

// lateralAliasPublishes reports whether one of the body's own output items
// publishes `bare` as its ALIAS — a name the injection may not take.
func lateralAliasPublishes(cols []plansql.SelectColumn, bare string) bool {
	for _, c := range cols {
		if c.Alias != "" && strings.EqualFold(c.Alias, bare) {
			return true
		}
	}
	return false
}
