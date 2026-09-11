package sql

import (
	"fmt"
	"sort"
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Hidden slots must identify planner-produced values, never colliding user
// columns (#694). Every consumer reads by name; delimited SQL can spell any
// string, so an unusual prefix alone cannot guarantee isolation.
// Refuse newly minted user names in reserved families; stored columns remain
// readable and require planner slot renumbering through a seeded allocator.
// Keep each family in reservedSlotPrefixes and mint through SlotAllocator;
// SlotName renders a known index only. New families must extend the shared
// constructor/family/refusal definition together.
// See docs/internals/sql-hidden-slot-namespace.md for the design.

// SlotFamily names one kind of hidden slot. The value is the name prefix, so a
// family constant and its reservation cannot drift apart.
type SlotFamily string

// The slot families the planner mints. Every one is reserved.
const (
	SlotWindowOutput SlotFamily = "__win_"        // a window function's output
	SlotWindowKey    SlotFamily = "__winkey_"     // a materialized PARTITION BY / ORDER BY / argument
	SlotSortKey      SlotFamily = "__sortkey_"    // a materialized ORDER BY term
	SlotGroupKey     SlotFamily = "__gb_expr_"    // a computed GROUP BY key
	SlotAggInput     SlotFamily = "__agg_expr_"   // an aggregate's derived argument
	SlotNestedAgg    SlotFamily = "__agg_"        // the nested-aggregate rewrite
	SlotScalar       SlotFamily = "__scalar_"     // a scalar subquery's answer
	SlotHaving       SlotFamily = "__having_"     // a materialized HAVING term
	SlotTwoLevel     SlotFamily = "__tl_"         // the two-level distinct rewrite
	SlotSetOpCount   SlotFamily = "__setop_"      // set-operation arm counters
	SlotAvgSum       SlotFamily = "__avg_sum"     // AVG's decomposed SUM leg
	SlotAvgCount     SlotFamily = "__avg_count"   // AVG's decomposed COUNT leg
	SlotVarState     SlotFamily = "__var_state"   // STDDEV/VARIANCE partial state
	SlotCovarState   SlotFamily = "__covar_state" // CORR/COVAR partial state
	SlotOhlcvState   SlotFamily = "__ohlcv_state" // OHLCV's partial bar state (ADR-0035)
	SlotGrouping     SlotFamily = "__grouping_"   // a GROUPING(...) bitmask
	// SlotCorrKey is a DECORRELATED LATERAL's correlation key: a value the
	// enclosing query never named, which the lowering has to publish so the
	// join it manufactures has a column to key on. Publishing it under the
	// SOURCE COLUMN's name is how `SELECT MAX(t.id) AS g … WHERE t.g = d.k`
	// answered the KEY (#956) and `SELECT amount AS order_id … WHERE
	// order_id = o.id` answered nothing (#767). ADR-0026 §3a.
	SlotCorrKey SlotFamily = "__key_" // a decorrelated correlation key
	// The SUFFIX-minted families: their names carry a discriminator rather than
	// a bare index, so they are rendered with fmt.Sprintf against the constant
	// rather than through SlotName. They are reserved on the same grounds.
	SlotPreComputedAgg SlotFamily = "__precomp_agg_" // pre-computed aggregate substitution
	SlotSubsumeFlag    SlotFamily = "__subsume_f"    // subsumed-filter marker
	SlotRowLocator     SlotFamily = "__row_loc"      // row-locator sentinel
	SlotRowCountOnly   SlotFamily = "__rowcount_only__"
	SlotDefaultPart    SlotFamily = "__default__"
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

// allSlotFamilies is every family constant above, so the coverage gate can
// walk them. A family added without a row here fails that gate.
// suffixMintedFamilies are the families whose names carry a discriminator
// rather than a bare index, so SlotName is not their producer. They are named
// here so "no SlotName producer" cannot quietly mean "forgotten".
var suffixMintedFamilies = []SlotFamily{
	SlotPreComputedAgg, SlotSubsumeFlag, SlotRowLocator,
	SlotRowCountOnly, SlotDefaultPart,
}

var allSlotFamilies = []SlotFamily{
	SlotWindowOutput, SlotWindowKey, SlotSortKey, SlotGroupKey, SlotAggInput,
	SlotNestedAgg, SlotScalar, SlotHaving, SlotTwoLevel, SlotSetOpCount,
	SlotAvgSum, SlotAvgCount, SlotVarState, SlotCovarState, SlotOhlcvState,
	SlotGrouping, SlotCorrKey,
}

// reservedSlotPrefixes is the reservation. It is a superset of the families
// above: several slots are minted with a suffix rather than an index
// (`__row_loc`, `__rowcount_only__`, `__default__`, `__precomp_agg_`,
// `__subsume_f`), and they are reserved on the same grounds.
var reservedSlotPrefixes = []string{
	"__agg_",
	"__agg_expr_",
	"__avg_count",
	"__avg_sum",
	"__covar_state",
	"__default__",
	"__gb_expr_",
	"__grouping_",
	"__having_",
	"__key_",
	"__ohlcv_state",
	"__precomp_agg_",
	"__row_loc",
	"__rowcount_only__",
	"__scalar_",
	"__setop_",
	"__sortkey_",
	"__subsume_f",
	"__tl_", // also covers __tl_avgsum_N / __tl_avgcnt_N / __tl_<func>_N
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
	// LONGEST match wins. Several prefixes nest — `__agg_expr_` extends
	// `__agg_`, `__win_` is not `__winkey_` but `__tl_` prefixes
	// `__tl_avgsum_` — and reporting the shorter one names a family the
	// caller did not mint. The refusal is the same either way; the MESSAGE is
	// what would have been wrong, and the coverage gate reads it.
	best := ""
	for _, p := range reservedSlotPrefixes {
		if strings.HasPrefix(lower, p) && len(p) > len(best) {
			best = p
		}
	}
	return best
}

// RefuseReservedSlotName returns the 42939 (reserved_name) refusal for a name
// a user is CREATING that collides with a hidden slot, or nil when it does not.
// where names the site, so the message says which door saw it.
//
// Call it where a name is MINTED BY THE USER: a SELECT alias, a derived table's
// or a CTE's output names, and the DDL and ingest doors (CREATE TABLE, the
// CreateTable API, an Ingester's schema, an INSERT column list that creates a
// column).
//
// Do NOT call it on a table's STORED columns at read time. Reading is not
// minting: the column already exists, some binary wrote it, and refusing it
// makes the table unreadable by every query — including the `SELECT *` that
// would show the user what is in it — while CREATE TABLE, the Go API and
// INSERT all still succeeded, so the trap closed behind the user with DROP
// TABLE the only exit. A stored collision is handled by renumbering the
// PLANNER's slot instead (renameCollidingSlots), so the two coexist.
func RefuseReservedSlotName(name, where string) error {
	family := ReservedSlotFamily(name)
	if family == "" {
		return nil
	}
	return sqlerr.New("42939",
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

// SlotAllocator issues fresh hidden names for ONE query scope, excluding both
// seeded in-scope names AND all slots it has already issued.
// Never hand window #2 window #1's slot, or collapse two group keys.
// Use it whenever obtaining a slot; SlotName only renders known indices/tests.
// Separate family cursors must not renumber unrelated families.
// Not concurrency-safe: one planning goroutine owns the scope and allocator.
// See docs/internals/sql-hidden-slot-allocation.md for the design.
type SlotAllocator struct {
	// taken holds the SEEDED names (a table's stored columns, an input
	// schema, the names already emitted in scope) and every slot this
	// allocator has ISSUED. Both halves matter, and forgetting the second is
	// the bug above.
	taken map[string]bool
	// next is the search cursor per family, so allocating in one family does
	// not renumber another.
	next map[SlotFamily]int
	// issued is what this allocator handed out, in order, for callers that
	// need to report or assert on it.
	issued []string
}

// NewSlotAllocator returns an allocator for a query scope, seeded with the
// names that already exist in it. Seeding is case-insensitive, because column
// resolution is.
func NewSlotAllocator(inScope ...string) *SlotAllocator {
	a := &SlotAllocator{taken: make(map[string]bool, len(inScope)+8), next: map[SlotFamily]int{}}
	a.Seed(inScope...)
	return a
}

// Seed adds more names to the scope. Safe to call after allocation has begun —
// a name seeded late is excluded from every LATER allocation, and the names
// already issued stay issued.
func (a *SlotAllocator) Seed(names ...string) {
	for _, n := range names {
		if n = strings.TrimSpace(n); n != "" {
			a.taken[strings.ToLower(n)] = true
		}
	}
}

// slotSearchBound is the most candidates one Next call will try. By the
// pigeonhole principle a free index exists within len(taken)+1 distinct
// candidates, since at most len(taken) of them can be taken; the constant is a
// generous ceiling and a guard against a corrupted map.
const slotSearchBound = 1 << 20

// Next returns the next unused slot of a family and records it as used.
//
// It excludes BOTH the seeded names and every slot this allocator has already
// issued, which is the whole of its contract.
//
// ok is false only when the family is EXHAUSTED — no free index below the
// search bound — which needs a scope holding a million names of one family and
// has no known SQL. A caller that gets false must leave the plan as it was
// rather than invent a name: an allocator that cannot allocate is a reason to
// decline an optimization, never a reason to reuse a slot.
//
// Terminates: the family's cursor advances by one per candidate and never
// rewinds, `taken` grows by at most one per successful call, and the loop is
// bounded.
func (a *SlotAllocator) Next(family SlotFamily) (string, bool) {
	for tried := 0; tried < slotSearchBound; tried++ {
		name := SlotName(family, a.next[family])
		a.next[family]++
		if !a.taken[strings.ToLower(name)] {
			a.taken[strings.ToLower(name)] = true
			a.issued = append(a.issued, name)
			return name, true
		}
	}
	return "", false
}

// Issued lists the slots this allocator handed out, in order.
func (a *SlotAllocator) Issued() []string {
	return append([]string(nil), a.issued...)
}
