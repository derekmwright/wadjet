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
type SlotFamily = plansql.SlotFamily

const (
	SlotWindowOutput = plansql.SlotWindowOutput
	SlotWindowKey    = plansql.SlotWindowKey
	SlotSortKey      = plansql.SlotSortKey
	SlotGroupKey     = plansql.SlotGroupKey
	SlotAggInput     = plansql.SlotAggInput
	SlotNestedAgg    = plansql.SlotNestedAgg
	SlotScalar       = plansql.SlotScalar
	SlotHaving       = plansql.SlotHaving
	SlotTwoLevel     = plansql.SlotTwoLevel
	SlotSetOpCount   = plansql.SlotSetOpCount
	SlotAvgSum       = plansql.SlotAvgSum
	SlotAvgCount     = plansql.SlotAvgCount
	SlotVarState     = plansql.SlotVarState
	SlotCovarState   = plansql.SlotCovarState

	SlotPreComputedAgg = plansql.SlotPreComputedAgg
	SlotSubsumeFlag    = plansql.SlotSubsumeFlag
	SlotRowLocator     = plansql.SlotRowLocator
	SlotRowCountOnly   = plansql.SlotRowCountOnly
	SlotDefaultPart    = plansql.SlotDefaultPart
)

// SlotName mints the Nth slot of a family.
func SlotName(family SlotFamily, n int) string { return plansql.SlotName(family, n) }

// ReservedSlotFamily returns the slot prefix name collides with, or "".
func ReservedSlotFamily(name string) string { return plansql.ReservedSlotFamily(name) }

// RefuseReservedSlotName is the 42939 refusal for a name a user is CREATING.
func RefuseReservedSlotName(name, where string) error {
	return plansql.RefuseReservedSlotName(name, where)
}

// RefuseReservedSlotNames refuses the first colliding name in names.
func RefuseReservedSlotNames(names []string, where string) error {
	return plansql.RefuseReservedSlotNames(names, where)
}

func refuseReservedSlotName(name, where string) error { return RefuseReservedSlotName(name, where) }

func refuseReservedSlotNames(names []string, where string) error {
	return RefuseReservedSlotNames(names, where)
}
