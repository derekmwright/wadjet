package scan

import (
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/optswitch"
)

// FLAG-MASK PUSHDOWN (#966) — extends the Level-3 scan filter to the three
// TCP-flag predicates and to the `BITWISE_AND(col, m) <op> k` spelling they
// generalize.
//
// A flags column is the shape the dictionary path was built for: a TCP flags
// field has at most 512 distinct values and in real traffic a handful, so a
// row group's dictionary is a few entries long and its index stream is deeply
// run-structured. Evaluating `flags & 18 = 18` once per DICTIONARY ENTRY and
// applying the answer per RLE RUN is the difference between a few compares per
// row group and one per row — and the values never materialize at all when the
// column is filter-only.
//
// It rides the existing seams rather than adding a path: an Op constant, an arm
// in evalPredOnDict, an arm in andPlainPage, and a recognizer in the planner.
// That is exactly what OpLike added (like_filter.go), and it is why the run
// path, the null handling, the bitmap and the filter-only column elision all
// apply here without a line of new machinery.
//
// WHAT IS *NOT* DONE HERE, deliberately: a flag predicate prunes nothing
// BEFORE it is evaluated. A min/max bound cannot prove anything about a BIT —
// `min=0, max=511` admits every mask, and even `min=2, max=16` admits a row
// holding 18. Nor does it join the dictionary-PROBE prune (dict_prune.go),
// which answers "is this exact value absent" and says nothing about a mask.
// A prune that cannot be proven is a dropped row, so there is none.
//
// It is NOT the claim that a row group is never skipped, and the two were
// conflated in the docs until #966 round 2 B2. Once the predicate HAS been
// evaluated over a group and matched nothing, EvalRowGroupPreds answers
// FilterNone and the group is dropped whole — `scanFilterSkipped` moves, and
// on the three-group dictionary fixture it moves by two. A prune decides
// without reading the values; a skip decides after reading them.
// TestAFlagPredicateDoesNoPruningBeforeItIsEvaluated holds both halves:
// StatsPruned +0 while ScanFilterSkipped is free to move.

// RowPred.Op values for flag-mask predicates. Value carries the folded mask as
// an int64.
const (
	OpFlagsAll  = "flags_all"  // (v & mask) == mask
	OpFlagsAny  = "flags_any"  // (v & mask) != 0
	OpFlagsNone = "flags_none" // (v & mask) == 0
)

// FlagDictPushdown gates flag-mask evaluation inside the scan. The planner
// checks it when building the RowPred, so with the switch off no flag conjunct
// is ever pushed and the exec filter evaluates every one of them — which is
// what makes it a real kill switch rather than a slower path to the same code.
// Kill switch: WADJET_FLAG_DICT_PUSHDOWN=0.
var FlagDictPushdown = optswitch.Register("flag-dict-pushdown", "WADJET_FLAG_DICT_PUSHDOWN",
	"TCP-flag / bitmask predicate evaluation inside the scan filter, once per dictionary entry")

// Engagement counters. A pushdown proves it ran by counting, never by rows:
// a filter that silently stopped being pushed answers identically and costs
// the thing the pushdown exists to save.
var (
	flagDictEntries atomic.Int64 // dictionary entries a flag mask was evaluated on
	flagDictMasks   atomic.Int64 // dictionary masks built (one per column chunk)
	flagPlainValues atomic.Int64 // values a flag mask was evaluated on, plain pages
)

// FlagPushdownStatsSnapshot returns (dictionary entries evaluated, dictionary
// masks built, plain-page values evaluated).
func FlagPushdownStatsSnapshot() (entries, masks, plain int64) {
	return flagDictEntries.Load(), flagDictMasks.Load(), flagPlainValues.Load()
}

// ResetFlagPushdownStats zeroes the counters. Test-only: the counters are
// process-global and a gate that asserts engagement has to start from a known
// point.
func ResetFlagPushdownStats() {
	flagDictEntries.Store(0)
	flagDictMasks.Store(0)
	flagPlainValues.Store(0)
}

func isFlagOp(op string) bool {
	return op == OpFlagsAll || op == OpFlagsAny || op == OpFlagsNone
}

// flagMatch is the mask test. It is the same three lines expr.TCPFlagsMatch
// holds, deliberately not imported: internal/engine/scan does not depend on
// the expression engine, and the two are held to each other by
// TestTheScanAndTheExpressionAgreeOnEveryMask rather than by a shared symbol
// across that boundary.
func flagMatch(v, mask int64, op string) bool {
	switch op {
	case OpFlagsAll:
		return v&mask == mask
	case OpFlagsAny:
		return v&mask != 0
	default:
		return v&mask == 0
	}
}
