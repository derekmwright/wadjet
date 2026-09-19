// SPDX-License-Identifier: MIT

package logical

// Options is the planner configuration of ONE database instance — the value
// `wadjet.Config`, the `wadjetd` flags and `coordinator.Config` resolve into
// and hand to `OptimizeWith` at every entry that optimizes a plan.
//
// It is a value, not a package variable, because configuration is a property
// of the instance that was asked for it: two `wadjet.DB`s open in one process
// hold different ones, and closing a DB takes its settings with it. The zero
// value is the shipped default — every field is off — so an entry that
// declares nothing plans the default regime rather than whatever some other
// instance last stored (#1223).
//
// Process-wide toggles that remain package variables are the `optswitch`
// kill switches, which are env-set for a whole process by design.
type Options struct {
	// BushyJoinReorder lets the cost-based join reorder emit BUSHY plans —
	// joins of two composite intermediates, e.g. pre-joining a snowflake
	// dimension chain before it meets the fact stream — when strictly
	// cheaper than every left-deep order (docs/design/bushy-join-cbo.md
	// §3.2). Cost ties keep the left-deep shape. Off by default: the regime
	// is correctness-proven but its logical-layer cost model mispredicts
	// distributed exchange reality, which is why Phase C left it opt-in.
	BushyJoinReorder bool
}
