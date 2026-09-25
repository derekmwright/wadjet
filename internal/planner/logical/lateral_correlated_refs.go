// SPDX-License-Identifier: MIT

package logical

import (
	"errors"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// publishLiftedRefs materializes inner columns used by lifted non-equality
// predicates under their existing names. A pure non-equality join may drop
// these columns after evaluating ON; with an equality key, the residual may
// run above the join, so the columns remain in its output and are hidden only
// from stars. Aggregated bodies refuse because publishing a column would
// change their grouping. See ADR-0021 §1s.
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
	// A BARE star only. A QUALIFIED star (`s.*`) reads the lateral's own
	// published list, from which `Node.StarLiftedRefCols` hides the slot, so
	// the materialization is clean under it (ADR-0021 §1q); reading it as an
	// enclosing star refused — and, before arc LT, declined to NULL pads —
	// a shape the DAG arms answered right (`R3/liftedStar`).
	enclosingStar := false
	if outer != nil {
		for _, c := range outer.Columns {
			if c.Star && c.TableRef == "" {
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
		// an enclosing STAR whose published list it would enter, is REFUSED
		// (arc LT; #1131, #1130). Each used to DECLINE to the disposition it
		// had before the materialization existed — and that disposition was a
		// plausible wrong row set, not a right one: the predicate read a
		// column the body dropped (`rows=0`, or a NULL pad per outer row), or
		// an alias holding another value, on every arm, and the L1 fixture
		// agreed with PostgreSQL by coincidence in one of them (`aliasCollides`,
		// where `i.id < 150` and `i.amount < 150` select the same rows). The
		// rule is ADR-0021 §1s's: a body with no equality key is not
		// key-partitionable, PostgreSQL evaluates it per outer row, and a
		// relation-valued body has no per-row runner here yet — so it is loud.
		// (A GROUPED body needs no arm here — it aggregates, so the refusal
		// above has already fired.)
		// A BARE enclosing star DECLINES rather than refuses (round-2
		// review, B5): the DAG evaluates the lifted predicate at the join
		// off the scan's own stream and answered PostgreSQL's rows on both
		// fixtures, so the refusal is the SINGLE-PROCESS pipeline's alone
		// (Node.LiftedRefDeclinedUnderStar, RefuseDeclinedLiftedRefs).
		if enclosingStar && !contested && !info.Distinct {
			return nil, errLiftedRefDeclinedUnderStar
		}
		if contested || info.Distinct {
			why := "the column it names is also published by the enclosing relation or by the body's own alias list, so the join could not tell the two apart"
			if info.Distinct && !contested {
				why = "the body carries DISTINCT, and materializing the column would change the DISTINCT key"
			}
			return nil, sqlerr.New("0A000",
				"LATERAL body's correlated predicate %s is not an equality on an inner column: "+
					"it is evaluated over the body's OUTPUT, which would have to publish the column it "+
					"names, and here it cannot — %s. PostgreSQL evaluates the body per outer row, which "+
					"this engine does not do for this shape. Correlate on an equality, name the columns "+
					"instead of a star, or restate the predicate in the enclosing WHERE over the "+
					"lateral's output",
				sqlerr.Quote(strings.TrimSpace(cp)), why)
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
			// Two lifted parts reading one column publish it ONCE: the WHERE
			// split is on the AST, so `(q.qv >= o.total AND q.qv <= o.total +
			// 20)` is two parts, and a second `qv` item made the body's list
			// name a column twice — `s.*` could not be enumerated (arc JP
			// round 4).
			if lateralAliasPublishes(*injected, lateralBareKeyNameOr(col)) {
				continue
			}
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

// errLiftedRefDeclinedUnderStar is publishLiftedRefs' private signal that the
// materialization declined under a bare enclosing star; buildLateralSubquery
// turns it into Node.LiftedRefDeclinedUnderStar.
var errLiftedRefDeclinedUnderStar = errors.New("lifted predicate declined under an enclosing star")

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

// qualifyLiftedRefsByLateralAlias respells, in a correlated predicate that is not
// the lateral's equality key, every INNER column reference the body publishes
// under its own name to the lateral's alias (`i.id` → `s.id`) — the one name
// that reads the body's column over the join's output whatever the enclosing
// relation publishes (the builder cannot ask: scan columns are annotated
// later). Anything else is returned unchanged.
func qualifyLiftedRefsByLateralAlias(cp string, leftAliases map[string]bool, cols []plansql.SelectColumn,
	rightAlias string) string {
	if rightAlias == "" || extractInnerColumn(cp, leftAliases) != "" {
		return cp
	}
	node, err := plansql.ParseExpression(cp)
	if err != nil || node == nil {
		return cp
	}
	changed := false
	out := plansql.RewriteExpr(node, func(n plansql.Node) (plansql.Node, bool) {
		ref, ok := n.(*plansql.ColRef)
		if !ok || ref.Column == "" || leftAliases[strings.ToLower(ref.Table)] {
			return nil, false
		}
		if _, renamed := lateralPublishedKeyName(cols, ref.Column); renamed ||
			!lateralSelectsColumn(cols, ref.Column) {
			return nil, false
		}
		changed = true
		return &plansql.ColRef{Table: rightAlias, Column: ref.Column}, true
	})
	if !changed {
		return cp
	}
	return out.String()
}

// lateralBareKeyNameOr is lateralBareKeyName, or the trimmed column when it
// has no bare name.
func lateralBareKeyNameOr(col string) string {
	if bare := lateralBareKeyName(col); bare != "" {
		return bare
	}
	return strings.TrimSpace(col)
}
