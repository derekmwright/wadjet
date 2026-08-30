package physical

import (
	"fmt"
	"sort"
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Hidden slots, and why the namespace they live in is RESERVED.
//
// The planner materializes its own values into hidden slots: a window
// function's output (`__win_N`), a materialized window key (`__winkey_N`), an
// ORDER BY term the SELECT list does not carry (`__sortkey_N`), a computed
// GROUP BY key (`__gb_expr_N`), an aggregate's derived argument
// (`__agg_expr_N`), a scalar subquery's answer (`__scalar_N`), AVG's
// decomposed pair, STDDEV's and CORR's partial-state tuples, and several more.
// Every consumer of a slot reads it BY NAME off a batch.
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
// a computed GROUP BY key published under its own text collides with an input
// column of that name the same way. Naming a slot after something the grammar
// cannot produce would close it structurally, but the SQL grammar can produce
// ANY string as a delimited identifier, so there is no such name.
//
// The namespace is therefore RESERVED, and reserving it means refusing to
// answer a query that spells one. That is a divergence from PostgreSQL — which
// has no reserved column namespace and answers these queries — and it is the
// right trade: the alternative is not answering them either, it is answering
// them WRONGLY. The refusal names the collision, so a user who genuinely has
// such a column knows to alias it.
//
// This file is deliberately self-contained: the prefix table, the name
// constructor, the family test and the refusal, with no dependency on anything
// else the planner has grown. A pass that mints a new slot family adds one row
// to reservedSlotPrefixes and mints its names through SlotName, and both the
// reservation and the refusal follow.

// SlotFamily names one kind of hidden slot. The value is the name prefix, so a
// family constant and its reservation cannot drift apart.
type SlotFamily string

// The slot families the planner mints. Every one is reserved.
const (
	SlotWindowOutput SlotFamily = "__win_"       // a window function's output
	SlotWindowKey    SlotFamily = "__winkey_"    // a materialized PARTITION BY / ORDER BY / argument
	SlotSortKey      SlotFamily = "__sortkey_"   // a materialized ORDER BY term
	SlotGroupKey     SlotFamily = "__gb_expr_"   // a computed GROUP BY key
	SlotAggInput     SlotFamily = "__agg_expr_"  // an aggregate's derived argument
	SlotNestedAgg    SlotFamily = "__agg_"       // the nested-aggregate rewrite
	SlotScalar       SlotFamily = "__scalar_"    // a scalar subquery's answer
	SlotHaving       SlotFamily = "__having_"    // a materialized HAVING term
	SlotTwoLevel     SlotFamily = "__tl_"        // the two-level distinct rewrite
	SlotSetOpCount   SlotFamily = "__setop_"     // set-operation arm counters
	SlotAvgSum       SlotFamily = "__avg_sum"    // AVG's decomposed SUM leg
	SlotAvgCount     SlotFamily = "__avg_count"  // AVG's decomposed COUNT leg
	SlotVarState     SlotFamily = "__var_state"  // STDDEV/VARIANCE partial state
	SlotCovarState   SlotFamily = "__covar_stat" // CORR/COVAR partial state
)

// SlotName is the Nth slot of a family: `SlotName(SlotWindowOutput, 0)` is
// `__win_0`.
//
// Minting a slot through this rather than through a local fmt.Sprintf is what
// keeps the reservation and the names in one place — a family whose names are
// built somewhere else is a family the refusal below does not really cover.
func SlotName(family SlotFamily, n int) string {
	return fmt.Sprintf("%s%d", string(family), n)
}

// reservedSlotPrefixes is the reservation. It is a superset of the families
// above: several slots are minted with a suffix rather than an index
// (`__row_loc`, `__rowcount_only__`, `__default__`, `__precomp_agg_`,
// `__subsume_f`), and they are reserved on the same grounds.
var reservedSlotPrefixes = []string{
	"__agg_",
	"__avg_count",
	"__avg_sum",
	"__covar_state",
	"__default__",
	"__gb_expr_",
	"__having_",
	"__precomp_agg_",
	"__row_loc",
	"__rowcount_only__",
	"__scalar_",
	"__setop_",
	"__sortkey_",
	"__subsume_f",
	"__tl_",
	"__var_state",
	"__win_",
	"__winkey_",
}

// ReservedSlotFamily returns the slot prefix name collides with, or "".
//
// The comparison is case-insensitive because column resolution is: a user who
// writes `__WIN_0` reaches the same slot every consumer looks up.
func ReservedSlotFamily(name string) string {
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

// RefuseReservedSlotName returns the 42601 refusal for a user-visible name
// that collides with a hidden slot, or nil when it does not. where names the
// site, so the message says which entry point saw it.
//
// Call it where a name ENTERS the query's namespace — a base table's declared
// columns, a derived table's or a CTE's output names, a SELECT alias. A bare
// REFERENCE to a slot name needs no check of its own: no source provides it,
// so it is already 42703.
func RefuseReservedSlotName(name, where string) error {
	family := ReservedSlotFamily(name)
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

// RefuseReservedSlotNames refuses the first colliding name in names, in sorted
// order so the message does not depend on map iteration.
func RefuseReservedSlotNames(names []string, where string) error {
	sorted := append([]string(nil), names...)
	sort.Strings(sorted)
	for _, n := range sorted {
		if err := RefuseReservedSlotName(n, where); err != nil {
			return err
		}
	}
	return nil
}

// Package-local spellings, so callers inside physical read naturally.
func refuseReservedSlotName(name, where string) error { return RefuseReservedSlotName(name, where) }

func refuseReservedSlotNames(names []string, where string) error {
	return RefuseReservedSlotNames(names, where)
}
