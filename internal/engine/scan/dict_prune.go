package scan

import (
	"math"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/optswitch"
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

// DictPrune gates dictionary-probe row-group pruning. The planner checks
// it when collecting equality conjuncts into EqProbes (plan.go), so with
// the switch off no probe is ever built. Kill switch: WADJET_DICT_PRUNE=0.
var DictPrune = optswitch.Register("dict-prune", "WADJET_DICT_PRUNE",
	"dictionary-probe row-group pruning for equality predicates on pure-dictionary chunks")

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
//
// For a DECIMAL column, Value is the unscaled carrier at the CATALOG's scale
// (kernel.StatsDomainValue converts the literal there), while the file's
// dictionary holds carriers at the FILE's own scale. Scale and Precision carry
// the catalog declaration so the probe layer can reconcile the two to a common
// scale before comparing — the dictionary twin of the stats-path reconcile
// (parquet.ReconcileRowGroupStats, #707/#916). They are zero for a non-DECIMAL
// probe and unused unless the file leaf is itself a DECIMAL.
type EqProbe struct {
	ColName   string
	Value     any
	Scale     int // catalog DECIMAL scale (0 otherwise)
	Precision int // catalog DECIMAL precision (0 otherwise)
}

// CanDictPruneRowGroup reports whether ANY of the equality conjuncts is
// provably unsatisfiable in this row group via a pure-dictionary probe.
// A true return means the row group cannot produce a matching row.
func CanDictPruneRowGroup(fr *pqt.FileReader, rgIdx int, probes []EqProbe) bool {
	if len(probes) == 0 {
		return false
	}
	leaves := fr.Leaves()
	// Resolve the probe's column by the same full-path discipline the native
	// reader uses (TopLevelLeafIndex), not by the first basename match. A nested
	// leaf and a top-level column can share a basename (`a.id` and `id`); the
	// old `leaf.Name == p.ColName` scan probed the nested leaf's dictionary for a
	// predicate that names the top-level column, so `id = 42` pruned a row whose
	// top-level id=42 using a nested a.id=99, dropping a matching row (#915). A
	// top-level column always wins the collision, exactly as the decode does.
	byName := pqt.TopLevelLeafIndex(leaves)
	for _, p := range probes {
		colIdx, ok := byName.Lookup(p.ColName)
		if !ok {
			continue
		}
		// Dictionary entries are RAW FILE values, but the probe is an
		// engine value. For a micro/nano TIMESTAMP column those live in
		// different units, and unlike the min/max bounds this test cannot
		// be rescaled into agreement: the engine value is a truncated
		// millisecond, so it matches a whole 1000-wide band of stored
		// micros rather than one dictionary entry. An exact-match probe
		// would find nothing and prune the row group — dropping rows that
		// do match. Decline instead; the row filter still evaluates the
		// predicate on decoded, rescaled values.
		if pqt.TimestampDivisorFromSchemaNode(leaves[colIdx]) != 1 {
			continue
		}
		// A DECIMAL dictionary holds carriers at the FILE's scale while the
		// probe carries the catalog scale, and unlike a TIMESTAMP the two can
		// be reconciled EXACTLY the same way the decoder does — so a file that
		// declares a different scale is reconciled rather than declined. When
		// the file leaf declares the catalog's own scale there is nothing to
		// move and the plain carrier probe below is exact; only a
		// scale-DISAGREEING file takes the reconcile path. A carrier that
		// cannot be moved withholds the prune (dictProbeDecimalAbsent), the
		// same discipline as ReconcileRowGroupStats on the stats side.
		if fs, isDec := pqt.DecimalFileScale(leaves[colIdx]); isDec && fs != p.Scale {
			if dictProbeDecimalAbsent(fr, rgIdx, colIdx, p, fs) {
				dictPrunedRowGroups.Add(1)
				return true
			}
			dictProbeMisses.Add(1)
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

// dictProbeDecimalAbsent reports that a DECIMAL equality value is PROVABLY
// absent from the column's row-group chunk when the file's scale disagrees
// with the catalog's. Any uncertainty — a wide (FLBA) dictionary, a carrier
// that cannot be moved to the catalog scale, or a non-int64 probe — returns
// false so the prune is WITHHELD and the row filter decides on decoded values.
//
// The reconciliation is the decoder's own (parquet.DecimalRescale, the same
// call rescaleDecimalChunk makes): each file carrier is moved from the file
// scale to the catalog scale and compared against the probe carrier, which is
// already at the catalog scale. Because the move is bit-for-bit what the
// decode does — including PostgreSQL's round-half-away-from-zero when the file
// scale is the wider one — a row whose decoded value equals the probe has a
// dictionary carrier that reconciles to exactly the probe, so a genuine match
// is never pruned. Moving the FILE side (not the probe) mirrors
// ReconcileRowGroupStats and never rounds the predicate.
func dictProbeDecimalAbsent(fr *pqt.FileReader, rgIdx, colIdx int, p EqProbe, fileScale int) bool {
	want, ok := toInt64Exact(p.Value)
	if !ok {
		return false
	}
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
	reconciledEquals := func(carrier int64) (equal bool, moved bool) {
		out, err := pqt.DecimalRescale(pqt.Decimal128From(carrier), fileScale, p.Scale, p.Precision)
		if err != nil {
			return false, false // a carrier the catalog band cannot hold: withhold
		}
		rc, ok := out.Int64()
		if !ok {
			return false, false // reconciled past int64: withhold
		}
		return rc == want, true
	}
	switch d.PhysType() {
	case pqt.PhysicalInt64:
		for _, v := range d.Int64() {
			eq, moved := reconciledEquals(v)
			if !moved {
				return false
			}
			if eq {
				return false
			}
		}
		return true
	case pqt.PhysicalInt32:
		for _, v := range d.Int32() {
			eq, moved := reconciledEquals(int64(v))
			if !moved {
				return false
			}
			if eq {
				return false
			}
		}
		return true
	default:
		// Wide (FIXED_LEN_BYTE_ARRAY) DECIMAL dictionaries are not reconciled
		// here — withhold rather than guess.
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
