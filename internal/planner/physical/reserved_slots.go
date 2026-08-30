package physical

import (
	"sort"
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The planner materializes values into HIDDEN SLOTS: a window function's
// output (`__win_N`), a materialized window key (`__winkey_N`), an ORDER BY
// term the SELECT list does not carry (`__sortkey_N`), a computed GROUP BY key
// (`__gb_expr_N`), an aggregate's derived argument (`__agg_expr_N`), a scalar
// subquery's answer (`__scalar_N`), AVG's decomposed pair, STDDEV's and CORR's
// partial-state tuples, and several more. Every consumer of a slot reads it BY
// NAME off a batch.
//
// The invariant those consumers need is that the slot names something the
// planner put there. It does not hold on its own, because the names are
// ordinary identifiers a user can write:
//
//	SELECT id, SUM(a) OVER () AS w FROM (SELECT id, a, b AS __win_0 FROM t) x
//	-- PostgreSQL 52.99 on every row; wadjet answered t.b, silently, on every
//	-- execution path, because the window's slot and the user's column were
//	-- the same name and the projection resolved by name.
//
// That is #694 re-created under the slot's own name, and it is not specific to
// windows: `s AS __winkey_0` made a window over an expression answer NULL, and
// every other family has the same shape. Naming a slot after something the
// grammar cannot produce would close it structurally, but the SQL grammar can
// produce ANY string as a delimited identifier, so there is no such name.
//
// The namespace is therefore RESERVED, and reserving it means refusing to
// answer a query that spells one. That is a divergence from PostgreSQL — which
// has no reserved column namespace and answers these queries — and it is the
// right trade: the alternative is not answering them either, it is answering
// them WRONGLY. The refusal names the collision, so a user who genuinely has
// such a column knows to alias it.
//
// A name is checked where it enters the query's namespace: a base table's
// declared columns, a derived table's or CTE's output names, and a SELECT
// alias. A bare REFERENCE to a slot name needs no check of its own — no source
// provides it, so it is already 42703.
var reservedSlotPrefixes = []string{
	"__agg_",            // nested-aggregate rewrite, and __agg_expr_N
	"__avg_count",       // AVG's decomposed COUNT leg
	"__avg_sum",         // AVG's decomposed SUM leg
	"__covar_state",     // CORR/COVAR partial-state tuple
	"__default__",       // the partition sentinel
	"__gb_expr_",        // computed GROUP BY key
	"__having_",         // materialized HAVING term
	"__precomp_agg_",    // pre-computed aggregate substitution
	"__row_loc",         // row-locator sentinel
	"__rowcount_only__", // the count-only read sentinel
	"__scalar_",         // scalar-subquery answer
	"__setop_",          // set-operation arm counters
	"__sortkey_",        // materialized ORDER BY term
	"__subsume_f",       // subsumed-filter marker
	"__tl_",             // two-level distinct rewrite
	"__var_state",       // STDDEV/VARIANCE partial-state triple
	"__win_",            // window function output slot
	"__winkey_",         // materialized window PARTITION BY / ORDER BY / argument
}

// reservedSlotFamily returns the slot prefix name collides with, or "".
//
// The comparison is case-insensitive because column resolution is: a user who
// writes `__WIN_0` reaches the same slot every consumer looks up.
func reservedSlotFamily(name string) string {
	lower := strings.ToLower(strings.TrimSpace(name))
	if !strings.HasPrefix(lower, "__") {
		return "" // the overwhelmingly common case, answered without a scan
	}
	for _, p := range reservedSlotPrefixes {
		if strings.HasPrefix(lower, p) {
			return p
		}
	}
	return ""
}

// refuseReservedSlotName returns the 42601 refusal for a user-visible name that
// collides with a hidden slot. where names the site, so the message says which
// of the three entry points saw it.
func refuseReservedSlotName(name, where string) error {
	family := reservedSlotFamily(name)
	if family == "" {
		return nil
	}
	return sqlerr.New("42601",
		"%s %q is in the reserved column namespace %q*, which the planner "+
			"materializes its own values into (a window's output, a materialized sort "+
			"or group key, an aggregate's derived argument); a query that spells one "+
			"cannot be answered correctly, so it is refused — alias the column to "+
			"something outside %q*", where, name, family, family)
}

// refuseReservedSlotNames refuses the first colliding name in names.
func refuseReservedSlotNames(names []string, where string) error {
	sorted := append([]string(nil), names...)
	sort.Strings(sorted)
	for _, n := range sorted {
		if err := refuseReservedSlotName(n, where); err != nil {
			return err
		}
	}
	return nil
}
