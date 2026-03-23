package scan

import (
	"github.com/citc-tech/wadjet/internal/engine/exec"
	pqt "github.com/citc-tech/wadjet/internal/storage/parquet"
)

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
		return compareValues(pred.Value, min) < 0 || compareValues(pred.Value, max) > 0
	case exec.OpLt:
		// Looking for rows where col < value. Prune if min >= value
		return compareValues(min, pred.Value) >= 0
	case exec.OpLe:
		// Looking for rows where col <= value. Prune if min > value
		return compareValues(min, pred.Value) > 0
	case exec.OpGt:
		// Looking for rows where col > value. Prune if max <= value
		return compareValues(max, pred.Value) <= 0
	case exec.OpGe:
		// Looking for rows where col >= value. Prune if max < value
		return compareValues(max, pred.Value) < 0
	case exec.OpNe:
		// Can only prune if min == max == value (all rows have the excluded value)
		return compareValues(min, max) == 0 && compareValues(min, pred.Value) == 0
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

	switch av := a.(type) {
	case int64:
		bv := toInt64(b)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case int32:
		return compareValues(int64(av), b)
	case float64:
		bv := toFloat64(b)
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	case float32:
		return compareValues(float64(av), b)
	case string:
		bv, ok := b.(string)
		if !ok {
			return 0
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
		return 0
	default:
		return 0
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
