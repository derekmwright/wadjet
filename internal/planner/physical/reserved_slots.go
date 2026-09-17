// SPDX-License-Identifier: MIT

package physical

import (
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// The reserved slot namespace lives in the PARSER package, not here.
//
// It has to: the names are minted in three packages — the logical builder
// (`__win_N`), the logical optimizer (`__tl_N`) and this one (`__winkey_N`,
// `__sortkey_N`, `__gb_expr_N`, `__agg_expr_N`, `__scalar_N`) — and
// `internal/planner/logical` cannot import `internal/planner/physical`,
// because the dependency runs the other way. A table of reserved prefixes that
// half the minting sites cannot reach is a table that drifts, which is exactly
// what happened: `SlotCovarState` read "__covar_stat" while the reservation
// and `worker/var_fold.go` used "__covar_state", latent only because nothing
// called the constructor.
//
// `internal/planner/sql/reserved_slots.go` is therefore the one copy, and it
// is what a caller outside this package should import. These aliases exist so
// the code already written against them keeps reading naturally.
type slotFamily = plansql.SlotFamily

const (
	slotWindowOutput = plansql.SlotWindowOutput
	SlotWindowKey    = plansql.SlotWindowKey
	slotSortKey      = plansql.SlotSortKey
	SlotGroupKey     = plansql.SlotGroupKey
	slotAggInput     = plansql.SlotAggInput
	slotNestedAgg    = plansql.SlotNestedAgg
	slotScalar       = plansql.SlotScalar
	slotHaving       = plansql.SlotHaving
	slotTwoLevel     = plansql.SlotTwoLevel
	slotSetOpCount   = plansql.SlotSetOpCount
	slotAvgSum       = plansql.SlotAvgSum
	slotAvgCount     = plansql.SlotAvgCount
	slotVarState     = plansql.SlotVarState
	slotCovarState   = plansql.SlotCovarState

	slotPreComputedAgg = plansql.SlotPreComputedAgg
	SlotSubsumeFlag    = plansql.SlotSubsumeFlag
	slotRowLocator     = plansql.SlotRowLocator
	slotRowCountOnly   = plansql.SlotRowCountOnly
	slotDefaultPart    = plansql.SlotDefaultPart
)

// SlotName mints the Nth slot of a family.
func SlotName(family slotFamily, n int) string { return plansql.SlotName(family, n) }

// ReservedSlotFamily returns the slot prefix name collides with, or "".
func reservedSlotFamily(name string) string { return plansql.ReservedSlotFamily(name) }

// RefuseReservedSlotName is the 42939 refusal for a name a user is CREATING.
func checkReservedSlotName(name, where string) error {
	return plansql.RefuseReservedSlotName(name, where)
}

// RefuseReservedSlotNames refuses the first colliding name in names.
func RefuseReservedSlotNames(names []string, where string) error {
	return plansql.RefuseReservedSlotNames(names, where)
}

func refuseReservedSlotName(name, where string) error { return checkReservedSlotName(name, where) }

func refuseReservedSlotNames(names []string, where string) error {
	return RefuseReservedSlotNames(names, where)
}
