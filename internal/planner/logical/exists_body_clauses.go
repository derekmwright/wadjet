// SPDX-License-Identifier: MIT

package logical

import (
	"strconv"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// existsBodyIsReproduced permits only clauses the semi/anti join preserves.
// A bound, grouping, HAVING, ungrouped aggregate, QUALIFY or set operation
// declines to per-outer-row execution. A positive literal LIMIT without OFFSET
// is removed because it cannot change existence; LIMIT 0 is not removed.
// DISTINCT and a SELECT-list window do not change existence. The fallback
// runs once per outer row; see ADR-0021 §1s.
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
