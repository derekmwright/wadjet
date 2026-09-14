package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A LIFTED CORRELATED PREDICATE READS THE BODY'S OUTPUT, AND A COLUMN THE
// BODY DOES NOT PUBLISH UNDER ITS OWN NAME IS REFUSED RATHER THAN ANSWERED.
//
// The decorrelation lifts a correlated WHERE predicate out of the body and
// evaluates it ABOVE the join, over the body's OUTPUT rows. For an EQUALITY
// the key loop guarantees the inner column is there: it is selected, minted
// into a hidden slot where the list would collide, and the predicate
// respelled to the name the body publishes.
//
// A predicate that is NOT an equality takes none of that path —
// `extractInnerColumn` answers "" for one — so it is lifted spelled against
// the body's INPUT. Where the body publishes that column under its own name
// the spelling still resolves and the answer is PostgreSQL's; where the body
// RENAMES it or does not select it, the condition names nothing:
//
//	SELECT s.m FROM lat_ord o JOIN LATERAL (
//	  SELECT i.amount AS m FROM lat_item i WHERE i.amount < o.total) s ON true
//	-- PostgreSQL 17.11: 8 rows   single / spilled: ZERO rows, silently
//	                              the three DAG arms: a loud join failure
//
//	SELECT i.amount FROM … (the same body publishing it under its own name)
//	-- 8 rows on all five arms, which is what localises the loss to the
//	   PROJECTION and not to the comparison
//
// **The disposition is `0A000`, and the two alternatives were measured.**
// Respelling the predicate to the body's published ALIAS makes the
// single-process arms answer PostgreSQL's rows — and makes the three DAG arms
// pad every outer row with NULL for the `LEFT JOIN LATERAL` spelling, which
// was RIGHT there before, because the stage that publishes a decorrelated
// lateral's output on the DAG emits the source column where the
// single-process projection emits the alias. Minting a hidden slot instead
// fixes neither: the slot is dropped at the join (`Node.HiddenJoinCols`,
// ADR-0026 §3c) and the lifted predicate runs ABOVE that drop, so both
// single-process arms answer zero rows again. Each is a right answer on some
// arms traded for a wrong one on others.
//
// Closing it is the DAG-identity layer #1028 names — one spelling both paths
// publish for a decorrelated lateral's output — together with a lifted
// predicate evaluated AT the join rather than above it. Until then the shape
// is loud on all five arms.
//
// **An AGGREGATED body has the same refusal for a second reason** and it is
// worth stating separately: there is no projection to publish the column in at
// all. `SELECT SUM(i.amount) … WHERE i.amount < o.total` would need
// `i.amount` in the GROUP BY, which changes what the aggregate computes.
// PostgreSQL evaluates the body per outer row and aggregates the rows that
// outer row admits — 350, 350, NULL over these three orders — where this
// engine answered three NULLs on every arm, silently.
func refuseUnpublishedLiftedRefs(info *plansql.SelectInfo, correlatedParts []string,
	leftAliases map[string]bool, aggregates bool) error {
	for _, cp := range correlatedParts {
		if extractInnerColumn(cp, leftAliases) != "" {
			continue // an equality the key loop publishes and respells
		}
		node, err := plansql.ParseExpression(cp)
		if err != nil || node == nil {
			continue
		}
		missing := ""
		walkExprNodes(node, func(x plansql.Node) {
			ref, ok := x.(*plansql.ColRef)
			if missing != "" || !ok || ref.Column == "" {
				return
			}
			if leftAliases[strings.ToLower(ref.Table)] {
				return
			}
			// Published under its OWN name, or through a star, is what the
			// lifted spelling needs. An ALIAS is not the same thing: the
			// alias is the single-process projection's name for the value and
			// the DAG's stage emits the source column, so the two paths
			// disagree about which spelling resolves.
			if _, renamed := lateralPublishedKeyName(info.Columns, ref.Column); renamed {
				missing = ref.Column
				return
			}
			if !lateralSelectsColumn(info.Columns, ref.Column) {
				missing = ref.Column
			}
		})
		if missing == "" {
			continue
		}
		why := "the body does not publish it under that name, so the lifted " +
			"predicate names nothing"
		if aggregates {
			why = "the body AGGREGATES, so there is no projection to publish it in — " +
				"publishing it would put it in the GROUP BY and change what the " +
				"aggregate computes"
		}
		return sqlerr.New("0A000",
			"LATERAL body's correlated predicate %s is not an equality and reads %s: "+
				"the correlation is lowered into a JOIN and the predicate is evaluated "+
				"over the body's OUTPUT, and %s. PostgreSQL evaluates the body per "+
				"outer row, which this engine does not do for this shape. SELECT %s in "+
				"the body under its own name, or write the comparison in the ENCLOSING "+
				"query over the lateral's output",
			sqlerr.Quote(strings.TrimSpace(cp)), sqlerr.Quote(missing), why,
			sqlerr.Quote(missing))
	}
	return nil
}
