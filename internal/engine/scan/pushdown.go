package scan

import (
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/optswitch"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// StatsPrune gates static-predicate min/max (zonemap) row-group pruning at
// every consumption site: the planner's rgUnit build, readBatchDirect, and
// the Scanner decode path. Dynamic-filter pruning (join-build ranges,
// blooms) is separately gated. Kill switch: WADJET_STATS_PRUNE=0.
var StatsPrune = optswitch.Register("stats-prune", "WADJET_STATS_PRUNE",
	"min/max zonemap row-group pruning from static scan predicates")

// StatsPredicate evaluates whether a row group can be skipped based on column stats.
type StatsPredicate struct {
	Column string
	Op     exec.CompareOp
	Value  any
}

// CanPruneRowGroup returns true if the row group can be skipped based on min/max stats.
func CanPruneRowGroup(pred StatsPredicate, stats pqt.RowGroupStats) bool {
	colStats, ok := stats.Columns[pred.Column]
	if !ok || !colStats.HasStats {
		return false // no stats available, can't prune
	}

	// If all values are null in this column, only non-null predicates can prune
	if colStats.NullCount == stats.NumRows {
		switch pred.Op {
		case exec.OpIsNull:
			return false // all nulls, don't prune
		case exec.OpIsNotNull:
			return true // all nulls, prune
		default:
			return true // comparison against all-null column, prune
		}
	}

	if pred.Op == exec.OpIsNull || pred.Op == exec.OpIsNotNull {
		return false // can't prune IS NULL/IS NOT NULL with min/max stats alone
	}

	if colStats.MinValue == nil || colStats.MaxValue == nil {
		return false
	}

	min := colStats.MinValue
	max := colStats.MaxValue

	switch pred.Op {
	case exec.OpEq:
		// Prune if value < min or value > max
		if c, ok := compareValuesOK(pred.Value, min); ok && c < 0 {
			return true
		}
		if c, ok := compareValuesOK(pred.Value, max); ok && c > 0 {
			return true
		}
		return false
	case exec.OpLt:
		// Looking for rows where col < value. Prune if min >= value
		c, ok := compareValuesOK(min, pred.Value)
		return ok && c >= 0
	case exec.OpLe:
		// Looking for rows where col <= value. Prune if min > value
		c, ok := compareValuesOK(min, pred.Value)
		return ok && c > 0
	case exec.OpGt:
		// Looking for rows where col > value. Prune if max <= value
		c, ok := compareValuesOK(max, pred.Value)
		return ok && c <= 0
	case exec.OpGe:
		// Looking for rows where col >= value. Prune if max < value
		c, ok := compareValuesOK(max, pred.Value)
		return ok && c < 0
	case exec.OpNe:
		// Can only prune if min == max == value (all rows have the excluded value)
		cm, okm := compareValuesOK(min, max)
		cv, okv := compareValuesOK(min, pred.Value)
		return okm && okv && cm == 0 && cv == 0
	default:
		return false
	}
}

// CompareValues compares two typed values (int32, int64, float32, float64, string).
// Returns -1, 0, or 1. Exported for use by dynamic filter row-group pruning.
func CompareValues(a, b any) int {
	return compareValues(a, b)
}

func compareValues(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	c, _ := compareValuesOK(a, b)
	return c
}

// compareValuesOK compares two typed values and reports whether the
// comparison was MEANINGFUL. A cross-type comparison that cannot be
// coerced exactly (string vs numeric, non-numeric types) returns
// ok=false — callers deciding to PRUNE must treat that as "unknown, keep
// the row group". The old compareValues silently coerced mismatches to 0
// (string → int64 0), which made `dateCol <= '2013-07-31'` compare
// day-number stats against zero and prune every row group the moment
// string date literals reached the stats layer.
func compareValuesOK(a, b any) (int, bool) {
	switch av := a.(type) {
	case int64:
		bv, ok := toInt64OK(b)
		if !ok {
			return 0, false
		}
		if av < bv {
			return -1, true
		}
		if av > bv {
			return 1, true
		}
		return 0, true
	case int32:
		return compareValuesOK(int64(av), b)
	case float64:
		bv, ok := toFloat64OK(b)
		if !ok {
			return 0, false
		}
		if av < bv {
			return -1, true
		}
		if av > bv {
			return 1, true
		}
		return 0, true
	case float32:
		return compareValuesOK(float64(av), b)
	case string:
		bv, ok := b.(string)
		if !ok {
			return 0, false
		}
		if av < bv {
			return -1, true
		}
		if av > bv {
			return 1, true
		}
		return 0, true
	default:
		return 0, false
	}
}

func toInt64OK(v any) (int64, bool) {
	switch tv := v.(type) {
	case int64:
		return tv, true
	case int32:
		return int64(tv), true
	case int:
		return int64(tv), true
	case float64:
		return int64(tv), tv == float64(int64(tv))
	default:
		return 0, false
	}
}

func toFloat64OK(v any) (float64, bool) {
	switch tv := v.(type) {
	case float64:
		return tv, true
	case float32:
		return float64(tv), true
	case int64:
		return float64(tv), true
	case int32:
		return float64(tv), true
	case int:
		return float64(tv), true
	default:
		return 0, false
	}
}

func toInt64(v any) int64 {
	switch tv := v.(type) {
	case int64:
		return tv
	case int32:
		return int64(tv)
	case int:
		return int64(tv)
	case float64:
		return int64(tv)
	default:
		return 0
	}
}

func toFloat64(v any) float64 {
	switch tv := v.(type) {
	case float64:
		return tv
	case float32:
		return float64(tv)
	case int64:
		return float64(tv)
	case int:
		return float64(tv)
	default:
		return 0
	}
}

// CanRangePruneRowGroup returns true when a row group can be skipped based
// on dynamic min/max ranges supplied by an upstream hash-join build (or a
// distributed dynamic filter). A row group prunes when its column range has
// no overlap with ANY of the supplied range filters.
//
// Behavior matches the in-process planner's canRangePruneRowGroup
// (planner/physical/util.go) — moved here so the worker fragment-runner
// scan path can apply the same logic without a circular import.
func CanRangePruneRowGroup(ranges []exec.DynamicRange, stats pqt.RowGroupStats) bool {
	for _, r := range ranges {
		colStats, ok := stats.Columns[r.Column]
		if !ok || !colStats.HasStats {
			continue
		}
		if colStats.MinValue == nil || colStats.MaxValue == nil {
			continue
		}
		if colStats.NullCount == stats.NumRows {
			continue
		}
		// No overlap: row group max < build min, or row group min > build max
		if CompareValues(colStats.MaxValue, r.MinValue) < 0 {
			return true
		}
		if CompareValues(colStats.MinValue, r.MaxValue) > 0 {
			return true
		}
	}
	return false
}

// CanBloomPruneRowGroup returns true when every integer value in the row
// group's min..max range is absent from the bloom — i.e., the row group
// cannot contain any rows matching the build side. Only applicable for
// single-column integer keys with a small (≤1024) value range; larger
// ranges return false (no pruning) to keep the check O(small).
func CanBloomPruneRowGroup(bf *exec.BloomScanFilter, stats pqt.RowGroupStats) bool {
	if bf == nil || !bf.UseIntKey {
		return false
	}
	colStats, ok := stats.Columns[bf.Column]
	if !ok || !colStats.HasStats {
		return false
	}
	if colStats.MinValue == nil || colStats.MaxValue == nil {
		return false
	}
	if colStats.NullCount == stats.NumRows {
		return false
	}
	minVal := toInt64(colStats.MinValue)
	maxVal := toInt64(colStats.MaxValue)
	if minVal == 0 && maxVal == 0 {
		switch colStats.MinValue.(type) {
		case int64, int32, int:
			// Genuine zero range — fall through.
		default:
			return false
		}
	}
	const maxRangeSize = 1024
	rangeSize := maxVal - minVal + 1
	if rangeSize <= 0 || rangeSize > maxRangeSize {
		return false
	}
	for v := minVal; v <= maxVal; v++ {
		if exec.BloomContains(bf.Bloom, bf.BloomMask, exec.BloomHashInt(v)) {
			return false
		}
	}
	return true
}
