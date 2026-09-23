// SPDX-License-Identifier: MIT

package logical

import (
	"strconv"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// THE EXISTS REWRITE KEEPS ONLY A BODY IT REPRODUCES — ADR-0021 §1s, #1238,
// arc DC's N2.
//
// `tryDecorrelateExists` builds the body's FROM and its WHERE as a semi (anti)
// join's build side and nothing else. PostgreSQL evaluates the whole body once
// per outer row, so every clause that can change WHETHER the body yields a row
// decides the answer, and a clause the build side does not carry was silently
// dropped:
//
//	EXISTS (SELECT 1 FROM lt_i i WHERE i.k = o.k LIMIT 0)      -- PG: no row, ever
//	EXISTS (SELECT MAX(i.v) FROM lt_i i WHERE i.k = o.k
//	        GROUP BY i.tag HAVING MAX(i.v) > 35)               -- PG: only where a group survives
//	EXISTS (SELECT MAX(i.v) FROM lt_i i WHERE i.k = o.k)       -- PG: EVERY outer row (an
//	                                                           --   ungrouped aggregate is one row)
//
// answered the plain `EXISTS (SELECT 1 … WHERE i.k = o.k)` on all five arms.
//
// The rule is key-partitionability (ADR-0021 §1s): the semi join is exact when
// the body's result restricted to one key equals the body evaluated for that
// key, and a bound, a grouping, a HAVING, an ungrouped aggregate, a QUALIFY or
// a set operation is a breaker the build side does not partition by the key.
// Such a body DECLINES here, and the per-row rerun — which executes the text
// as written — answers it. That is the same disposition the IN rewrite has
// had for a bound since #482 (`tryDecorrelateInSubquery`).
//
// What existence is INVARIANT under is stripped before the check rather than
// declined: `LIMIT n` with n >= 1 and no OFFSET cannot change whether there is
// a row, and `EXISTS (… LIMIT 1)` is a spelling people write on purpose. A
// DISTINCT and a SELECT-list window are invariant too and were never carried,
// so they need no strip. A `LIMIT 0` removes every row and declines like any
// other bound; the rerun answers it as PostgreSQL does.
//
// The rerun's cost is linear in the outer rows — one body run per row, 10.9 ms
// per row over a 1 000 000-row inner relation when this was measured — which
// is what a decline here costs and what the seam table's cells record.
func existsBodyIsReproduced(info *plansql.SelectInfo) bool {
	if info == nil {
		return false
	}
	if info.Union != nil {
		return false
	}
	if strings.TrimSpace(info.Offset) != "" {
		return false
	}
	if lim := strings.TrimSpace(info.Limit); lim != "" && !existsInvariantLimit(lim) {
		return false
	}
	if len(info.GroupBy) > 0 || info.HavingExpr != nil || strings.TrimSpace(info.Having) != "" {
		return false
	}
	if info.QualifyExpr != nil || strings.TrimSpace(info.Qualify) != "" {
		return false
	}
	for _, c := range info.Columns {
		if c.IsAgg {
			return false
		}
		if c.ASTExpr != nil && len(plansql.FindAllAggregates(c.ASTExpr)) > 0 {
			return false
		}
	}
	return true
}

// existsInvariantLimit reports whether a LIMIT text is a positive integer
// constant — the one bound that cannot change whether a body yields a row.
// `LIMIT ALL` is not parseable here at all, and anything this cannot read as
// an integer is assumed to bind.
func existsInvariantLimit(text string) bool {
	n, err := strconv.ParseInt(strings.TrimSpace(text), 10, 64)
	return err == nil && n >= 1
}
