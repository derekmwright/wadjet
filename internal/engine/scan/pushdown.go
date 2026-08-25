package scan

import (
	"math"

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
//
// Value must already be in the STATS domain — the representation
// parquet.RowGroupStats decodes the footer's bounds into, which for several
// types is not the representation a SQL literal arrives in. This layer
// compares two `any` values by their Go kind and cannot tell an unscaled
// DECIMAL bound from a scaled literal, or sixteen raw address bytes from an
// address in text; both pairs land in the same kind and get compared as if
// they agreed, which is #442. kernel.StatsDomainValue is the conversion, and
// the producer WITHHOLDS a predicate it cannot convert.
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

	// A FLOAT column's statistics say nothing about NaN. The parquet format
	// keeps NaN out of min/max by specification, and wadjet's own writer never
	// lets one win a `<`/`>` either — so a row group whose bounds are [1, 5]
	// may still hold a NaN. PostgreSQL orders NaN ABOVE every value (ADR-0012
	// item 8), which the filter kernels now do too, so `> c`, `>= c` and
	// `<> c` are TRUE for that invisible row and pruning on the bounds would
	// delete rows the filter keeps — a prune reading the predicate differently
	// from the filter (ADR-0018). `=`, `<` and `<=` need no exception: NaN
	// satisfies none of them against a finite constant, so the bounds decide
	// those correctly whether or not a NaN is hiding.
	if isFloatBound(min) || isFloatBound(max) {
		switch pred.Op {
		case exec.OpGt, exec.OpGe, exec.OpNe:
			return false
		}
	}

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

// isFloatBound reports whether a statistics bound came from a FLOAT column.
// The bound's Go kind is the only signal available here — CanPruneRowGroup is
// handed a decoded bound and a literal, not the column's declared type.
func isFloatBound(v any) bool {
	switch v.(type) {
	case float32, float64:
		return true
	}
	return false
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
	if ai, ok := toInt64OK(a); ok {
		if bi, ok := toInt64OK(b); ok {
			return cmpOrdered(ai, bi), true
		}
		if bf, ok := toFloat64OK(b); ok {
			return cmpInt64Float64(ai, bf)
		}
		return 0, false
	}
	if af, ok := toFloat64OK(a); ok {
		if bf, ok := toFloat64OK(b); ok {
			if math.IsNaN(af) || math.IsNaN(bf) {
				return 0, false
			}
			return cmpOrdered(af, bf), true
		}
		if bi, ok := toInt64OK(b); ok {
			c, ok := cmpInt64Float64(bi, af)
			return -c, ok
		}
		return 0, false
	}
	// A CIDR bound RowGroupStats has CONFIRMED is in inet order boxes as
	// pqt.CidrInetBound, a distinct type from plain string, specifically so
	// this comparison cannot mix it with an ordinary string (#523): a type
	// assertion to string fails on it (the dynamic type is CidrInetBound,
	// not string), so without this arm both sides would fall through to
	// "not comparable" — safe, but it would also refuse the case that must
	// work, two CONFIRMED bounds. A row group whose file could not confirm
	// the order keeps a plain string for MinValue/MaxValue, which still
	// correctly refuses against a CidrInetBound literal here.
	if ab, ok := a.(pqt.CidrInetBound); ok {
		bb, ok := b.(pqt.CidrInetBound)
		if !ok {
			return 0, false
		}
		return cmpOrdered(string(ab), string(bb)), true
	}
	if _, ok := b.(pqt.CidrInetBound); ok {
		return 0, false
	}
	as, aok := a.(string)
	bs, bok := b.(string)
	if aok && bok {
		return cmpOrdered(as, bs), true
	}
	return 0, false
}

func cmpOrdered[T int64 | float64 | string](a, b T) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// cmpInt64Float64 orders an integer against a float EXACTLY, at every
// magnitude, in one direction only — so that compareValuesOK gives the same
// answer whichever operand it is handed first.
//
// It used to not. The int64 arm coerced through toInt64OK, which REFUSES a
// non-integral float; the float64 arm coerced through toFloat64OK, which
// accepts an int64 without complaint. So (stat, literal) and
// (literal, stat) could disagree about whether the pair was even comparable —
// and CanPruneRowGroup passes them in the two different orders, OpEq one way
// and the five ordered operators the other. The DECIMAL half of #442 was
// invisible on `<`, `<=`, `>` and `>=` for exactly that reason: they were
// saved by a refusal, not by being right, and an INTEGRAL literal against the
// same wrong domain pruned them too.
func cmpInt64Float64(i int64, f float64) (int, bool) {
	switch {
	case math.IsNaN(f):
		return 0, false
	case math.IsInf(f, 1):
		return -1, true
	case math.IsInf(f, -1):
		return 1, true
	}
	// 2^63 is exactly representable; anything at or past it is outside int64.
	const twoPow63 = float64(1) * (1 << 62) * 2
	if f >= twoPow63 {
		return -1, true
	}
	if f < -twoPow63 {
		return 1, true
	}
	t := math.Trunc(f)
	if ti := int64(t); i != ti {
		return cmpOrdered(i, ti), true
	}
	switch frac := f - t; {
	case frac > 0:
		return -1, true
	case frac < 0:
		return 1, true
	}
	return 0, true
}

// toInt64OK and toFloat64OK now report only what a value IS, never what it
// could be coerced to: the pairing is decided by compareValuesOK, which is the
// only place that can make the choice symmetric.
func toInt64OK(v any) (int64, bool) {
	switch tv := v.(type) {
	case int64:
		return tv, true
	case int32:
		return int64(tv), true
	case int:
		return int64(tv), true
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
