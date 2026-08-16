package scan

import (
	"math"
	"sync/atomic"

	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Dictionary-probe row-group pruning (level 2.5 of the pushdown ladder):
// an equality predicate over a PURE dictionary-encoded column chunk can be
// answered against the dictionary alone — if the constant is not among
// the dictionary entries, no row in the row group can match, and the
// whole group's decode is skipped. This is the precise tool where min/max
// zonemaps are blind: a point filter on a high-cardinality column
// (ClickBench Q21 shape, `UserID = <const>` — random IDs span every
// zonemap) pays full column decode today; the dictionary answers it in
// microseconds.
//
// Soundness rests on ColumnPageReader.DictionaryIfPure: chunks with ANY
// non-dictionary data page (writer fallback) are never pruned. Equality
// never matches NULL, so null presence is irrelevant. Predicate values
// compare in the FILE's physical domain with conservative conversion —
// anything lossy declines to prune.

// DictPruneStats counters (wlog / test engagement markers).
var (
	dictPrunedRowGroups atomic.Int64
	dictProbeMisses     atomic.Int64 // probes that could not prune (impure chunk, type mismatch, value present)
)

// DictPruneStatsSnapshot returns (pruned row groups, non-pruning probes).
func DictPruneStatsSnapshot() (int64, int64) {
	return dictPrunedRowGroups.Load(), dictProbeMisses.Load()
}

// EqProbe is one equality conjunct to test against a row group.
type EqProbe struct {
	ColName string
	Value   any
}

// CanDictPruneRowGroup reports whether ANY of the equality conjuncts is
// provably unsatisfiable in this row group via a pure-dictionary probe.
// A true return means the row group cannot produce a matching row.
func CanDictPruneRowGroup(fr *pqt.FileReader, rgIdx int, probes []EqProbe) bool {
	if len(probes) == 0 {
		return false
	}
	leaves := fr.Leaves()
	for _, p := range probes {
		colIdx := -1
		for i, leaf := range leaves {
			if leaf.Name == p.ColName {
				colIdx = i
				break
			}
		}
		if colIdx < 0 {
			continue
		}
		if dictProbeAbsent(fr, rgIdx, colIdx, p.Value) {
			dictPrunedRowGroups.Add(1)
			return true
		}
		dictProbeMisses.Add(1)
	}
	return false
}

// dictProbeAbsent reports that the value is PROVABLY absent from the
// column's row-group chunk. Any uncertainty returns false.
func dictProbeAbsent(fr *pqt.FileReader, rgIdx, colIdx int, val any) bool {
	pr := fr.ColumnPages(rgIdx, colIdx)
	if pr == nil {
		return false
	}
	defer pr.Close()
	dict, pure, err := pr.DictionaryIfPure()
	if err != nil || !pure || dict == nil {
		return false
	}
	d := dict.Data
	switch d.PhysType() {
	case pqt.PhysicalInt64:
		want, ok := toInt64Exact(val)
		if !ok {
			return false
		}
		for _, v := range d.Int64() {
			if v == want {
				return false
			}
		}
		return true
	case pqt.PhysicalInt32:
		want, ok := toInt64Exact(val)
		if !ok || want < math.MinInt32 || want > math.MaxInt32 {
			// Out-of-range int32 equality can never match any int32 value,
			// but leave that conclusion to the planner — prune only on a
			// clean probe.
			return ok && (want < math.MinInt32 || want > math.MaxInt32)
		}
		w32 := int32(want)
		for _, v := range d.Int32() {
			if v == w32 {
				return false
			}
		}
		return true
	case pqt.PhysicalByteArray:
		var want string
		switch s := val.(type) {
		case string:
			want = s
		case []byte:
			want = string(s)
		default:
			return false
		}
		data, offs := d.ByteArray()
		for i := 0; i+1 < len(offs); i++ {
			if string(data[offs[i]:offs[i+1]]) == want {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// toInt64Exact converts predicate literals to int64 only when the
// conversion is exact.
func toInt64Exact(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	case float64:
		if x == math.Trunc(x) && x >= -9.2e18 && x <= 9.2e18 {
			return int64(x), true
		}
		return 0, false
	default:
		return 0, false
	}
}
