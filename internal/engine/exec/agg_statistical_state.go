// This file holds statistical and ordered aggregate state representations.
// ADR-0010, ADR-0023, and ADR-0027 govern partial-state transport, key identity, and spill ownership.
package exec

import (
	"encoding/binary"
	"encoding/hex"
	"math"
	"sort"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// stringAggState accumulates strings with a separator.
type stringAggState struct {
	// sep first keeps the two pointer prefixes adjacent (scan region 24 B, not 32).
	sep   string
	parts []string
	// sorted marks a DISTINCT aggregation, whose output PostgreSQL ORDERS.
	// `string_agg(DISTINCT s, ',')` there yields the distinct values sorted —
	// it has to, because the dedup is a sort — while a plain string_agg keeps
	// arrival order, which is unspecified and is ADR-0013 nondeterminism
	// class 1. Reporting first-seen order for the DISTINCT form was a value
	// divergence with a definite PostgreSQL answer (#703 review, F5).
	sorted bool
}

// render is stringAggState's finalized value: the parts joined, sorted first
// when the aggregation was DISTINCT. Sorting a COPY, because a merged clone's
// state can be read more than once and the arrival order is the only record of
// which part came from where.
func (s *stringAggState) render() string {
	if !s.sorted || len(s.parts) < 2 {
		return strings.Join(s.parts, s.sep)
	}
	out := append([]string(nil), s.parts...)
	sort.Strings(out)
	return strings.Join(out, s.sep)
}

// varianceState tracks running variance using Welford's online algorithm:
// the running (count, mean, M2) triple, updated one value at a time and
// combined pairwise by merge. Welford accumulates the deviations directly
// instead of subtracting E[x]² from E[x²], so no digits are lost to
// cancellation when the mean dwarfs the spread — the case that breaks a
// sum-of-squares accumulator (o_totalprice: mean 2.5e5, spread 1.4e5, so
// the two terms agree to five digits before the subtraction).
//
// The triple is the whole state, which is what makes it mergeable: partial
// aggregates computed over disjoint row sets combine exactly (see merge),
// so the same accumulator serves the single-process pipeline, the
// morsel-parallel clone merge, and the distributed partial/final split.
type varianceState struct {
	count int64
	mean  float64
	m2    float64
}

func (v *varianceState) update(x float64) {
	v.count++
	delta := x - v.mean
	v.mean += delta / float64(v.count)
	delta2 := x - v.mean
	v.m2 += delta * delta2
}

// merge folds another partial's state into this one — Chan, Golub and
// LeVeque's pairwise combination:
//
//	M2 = M2_a + M2_b + delta² · n_a·n_b / n
//
// The delta² term is the between-partial contribution. Dropping it (summing
// M2 alone) leaves only the within-partial variance, and re-aggregating the
// partials' finished STDDEV values instead is not an approximation at all —
// it is the standard deviation of a handful of nearly identical numbers.
// Both were live before #339.
//
// mean is combined by the same weighting rather than recomputed from the
// two means' midpoint, so a small partial merged into a large one moves the
// mean by its own weight.
func (v *varianceState) merge(o *varianceState) {
	if o == nil || o.count == 0 {
		return
	}
	if v.count == 0 {
		*v = *o
		return
	}
	na, nb := float64(v.count), float64(o.count)
	n := na + nb
	delta := o.mean - v.mean
	v.mean += delta * nb / n
	v.m2 += o.m2 + delta*delta*na*nb/n
	v.count += o.count
}

// varianceStateWidth is the encoded width of a variance partial state:
// count (int64) + mean (float64) + M2 (float64) = 24 bytes, hex-encoded to
// 48 ASCII characters. Hex rather than raw bytes because the encoded state
// travels as a string column through parquet, the .wshf shuffle format and
// the NATS gather — every one of which is happier with text than with
// arbitrary bytes — and float64 bits round-trip exactly through it.
const varianceStateWidth = 48

// encode renders the state as a fixed-width hex string for a partial
// aggregate's output column. Exact: the float64s go over as their IEEE-754
// bit patterns, so a merge stage reads back the identical triple.
func (v *varianceState) encode() string {
	var buf [24]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(v.count))
	binary.BigEndian.PutUint64(buf[8:16], math.Float64bits(v.mean))
	binary.BigEndian.PutUint64(buf[16:24], math.Float64bits(v.m2))
	return hex.EncodeToString(buf[:])
}

// decodeVarianceState parses encode's output. Reports ok=false for anything
// else, which the caller treats as "no rows to merge" rather than as a
// silent zero — a state column that lost its encoding must not read as a
// valid empty partial.
func decodeVarianceState(s string) (varianceState, bool) {
	if len(s) != varianceStateWidth {
		return varianceState{}, false
	}
	var full [24]byte
	if _, err := hex.Decode(full[:], []byte(s)); err != nil {
		return varianceState{}, false
	}
	return varianceState{
		count: int64(binary.BigEndian.Uint64(full[0:8])),
		mean:  math.Float64frombits(binary.BigEndian.Uint64(full[8:16])),
		m2:    math.Float64frombits(binary.BigEndian.Uint64(full[16:24])),
	}, true
}

func (v *varianceState) variancePop() float64 {
	if v.count == 0 {
		return 0
	}
	return v.m2 / float64(v.count)
}

func (v *varianceState) varianceSamp() float64 {
	if v.count < 2 {
		return 0
	}
	return v.m2 / float64(v.count-1)
}

// covarianceState tracks running covariance using an online algorithm.
type covarianceState struct {
	count int64
	meanX float64
	meanY float64
	c     float64 // co-moment: sum of (xi - meanX_old)(yi - meanY_new)
	m2x   float64 // sum of (xi - meanX)^2
	m2y   float64 // sum of (yi - meanY)^2
}

func (s *covarianceState) update(x, y float64) {
	s.count++
	n := float64(s.count)
	dx := x - s.meanX
	s.meanX += dx / n
	dy := y - s.meanY
	s.meanY += dy / n
	s.c += dx * (y - s.meanY)
	s.m2x += dx * (x - s.meanX)
	s.m2y += dy * (y - s.meanY)
}

// merge folds another partial's co-moments into this one. Same pairwise
// combination as varianceState.merge, extended to the cross term:
//
//	C = C_a + C_b + dx·dy · n_a·n_b / n
//
// CORR/COVAR share varianceState's exposure to a dropped merge (they ride
// the same extraState slot), so they are combined here rather than left for
// the next report of the same defect.
func (s *covarianceState) merge(o *covarianceState) {
	if o == nil || o.count == 0 {
		return
	}
	if s.count == 0 {
		*s = *o
		return
	}
	na, nb := float64(s.count), float64(o.count)
	n := na + nb
	dx := o.meanX - s.meanX
	dy := o.meanY - s.meanY
	s.c += o.c + dx*dy*na*nb/n
	s.m2x += o.m2x + dx*dx*na*nb/n
	s.m2y += o.m2y + dy*dy*na*nb/n
	s.meanX += dx * nb / n
	s.meanY += dy * nb / n
	s.count += o.count
}

func (s *covarianceState) covarPop() float64 {
	if s.count == 0 {
		return 0
	}
	return s.c / float64(s.count)
}

func (s *covarianceState) covarSamp() float64 {
	if s.count < 2 {
		return 0
	}
	return s.c / float64(s.count-1)
}

func (s *covarianceState) correlation() float64 {
	if s.count < 2 || s.m2x == 0 || s.m2y == 0 {
		return 0
	}
	return s.c / math.Sqrt(s.m2x*s.m2y)
}

// covarianceStateWidth is the encoded width of a covariance partial state:
// count (int64) + meanX + meanY + C + M2x + M2y (five float64s) = 48 bytes,
// hex-encoded to 96 ASCII characters. Same reasoning as
// varianceStateWidth — the state travels as a string column through
// parquet, .wshf and NATS, and float64 bits round-trip exactly through hex.
const covarianceStateWidth = 96

func (s *covarianceState) encode() string {
	var buf [48]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(s.count))
	binary.BigEndian.PutUint64(buf[8:16], math.Float64bits(s.meanX))
	binary.BigEndian.PutUint64(buf[16:24], math.Float64bits(s.meanY))
	binary.BigEndian.PutUint64(buf[24:32], math.Float64bits(s.c))
	binary.BigEndian.PutUint64(buf[32:40], math.Float64bits(s.m2x))
	binary.BigEndian.PutUint64(buf[40:48], math.Float64bits(s.m2y))
	return hex.EncodeToString(buf[:])
}

// decodeCovarianceState parses encode's output. ok=false for anything else,
// which the caller treats as "no rows to merge" rather than as a valid empty
// partial.
func decodeCovarianceState(s string) (covarianceState, bool) {
	if len(s) != covarianceStateWidth {
		return covarianceState{}, false
	}
	var full [48]byte
	if _, err := hex.Decode(full[:], []byte(s)); err != nil {
		return covarianceState{}, false
	}
	return covarianceState{
		count: int64(binary.BigEndian.Uint64(full[0:8])),
		meanX: math.Float64frombits(binary.BigEndian.Uint64(full[8:16])),
		meanY: math.Float64frombits(binary.BigEndian.Uint64(full[16:24])),
		c:     math.Float64frombits(binary.BigEndian.Uint64(full[24:32])),
		m2x:   math.Float64frombits(binary.BigEndian.Uint64(full[32:40])),
		m2y:   math.Float64frombits(binary.BigEndian.Uint64(full[40:48])),
	}, true
}

// Covariance-family kinds, carried in the synthetic column's name exactly
// as the variance kinds are: one state serves all three, and which function
// finishes it is decided once, after the last merge.
const (
	CovarKindCorr      = "corr"
	CovarKindCovarSamp = "covar_samp"
	CovarKindCovarPop  = "covar_pop"
)

// FinalizeCovarianceState decodes a merged partial state and finishes it as
// the named kind. ok=false means SQL NULL, on the same thresholds the
// single-process finalization applies: fewer than two rows for CORR and
// COVAR_SAMP, no rows at all for COVAR_POP.
//
// Used by the distributed final-aggregate fold (worker/var_fold.go).
func FinalizeCovarianceState(encoded, kind string) (float64, bool) {
	st, ok := decodeCovarianceState(encoded)
	if !ok {
		return 0, false
	}
	switch kind {
	case CovarKindCorr:
		if st.count < 2 {
			return 0, false
		}
		return st.correlation(), true
	case CovarKindCovarSamp:
		if st.count < 2 {
			return 0, false
		}
		return st.covarSamp(), true
	case CovarKindCovarPop:
		if st.count == 0 {
			return 0, false
		}
		return st.covarPop(), true
	}
	return 0, false
}

// Variance-family kinds. A decomposed partial carries the same (count,
// mean, M2) triple whatever the caller asked for, so the kind travels
// beside it (in the synthetic column's name) and is applied once, by
// FinalizeVarianceState, after the last merge.
//
// STDDEV and VARIANCE without a suffix are the SAMPLE forms, matching
// DuckDB and PostgreSQL.
const (
	VarKindStddevSamp = "stddev_samp"
	VarKindVarSamp    = "var_samp"
	VarKindStddevPop  = "stddev_pop"
	VarKindVarPop     = "var_pop"
)

// FinalizeVarianceState decodes a merged partial state and finishes it as
// the named kind. ok=false means the result is SQL NULL: an unparseable or
// absent state, fewer than two rows for a sample form, or no rows at all
// for a population form — the same thresholds the single-process
// finalization applies.
//
// Used by the distributed final-aggregate fold (worker/var_fold.go), which
// is the only consumer outside this package.
func FinalizeVarianceState(encoded, kind string) (float64, bool) {
	st, ok := decodeVarianceState(encoded)
	if !ok {
		return 0, false
	}
	switch kind {
	case VarKindStddevSamp:
		if st.count < 2 {
			return 0, false
		}
		return math.Sqrt(st.varianceSamp()), true
	case VarKindVarSamp:
		if st.count < 2 {
			return 0, false
		}
		return st.varianceSamp(), true
	case VarKindStddevPop:
		if st.count == 0 {
			return 0, false
		}
		return math.Sqrt(st.variancePop()), true
	case VarKindVarPop:
		if st.count == 0 {
			return 0, false
		}
		return st.variancePop(), true
	}
	return 0, false
}

// collectState accumulates raw float64 values for percentile/mode/median.
type collectState struct {
	values []float64
}

// minMaxByState tracks the row where a comparison column is min/max.
type minMaxByState struct {
	// Field order packs this to 32 B (was 40) and puts the pointer prefix first:
	// one per group for MIN_BY/MAX_BY.
	bestVal  any     // the return column value at that row
	bestCmp  float64 // the comparison column value (min or max)
	hasValue bool
	isMin    bool
}

// boxContainerMemBytes estimates the retained heap footprint of a value
// Vector.GetValue boxed for a container column (ARRAY/MAP as []any, ROW as
// map[string]any, VECTOR as []float32) — MIN_BY/MAX_BY's bestVal shares the
// same declared-output-type rule as container MIN/MAX (aggSpecOutputType:
// "MIN_BY/MAX_BY hand the output vector the box GetValue produced"), so
// bestVal can retain an arbitrarily large nested structure the same way
// containerMinMaxState.best can. Anything else returns 0: the flat per-agg
// charge (extraStateBytes += len(h.Aggs) * 80) already tracks close to
// measured for a scalar bestVal — a STRING return column measured 487 B/
// group against 484 reported — so this must not double-charge what the
// flat estimate already covers.
func boxContainerMemBytes(v any) int64 {
	switch v.(type) {
	case []any, map[string]any, []float32:
		return boxMemBytesDeep(v)
	default:
		return 0
	}
}

// boxMemBytesDeep walks the exact shapes Vector.GetValue produces (its
// TypeArray/TypeMap/TypeRow/TypeVector/TypeString/TypeBytes arms) and sums
// a rough heap estimate. Not exhaustive over every Go type — GetValue's box
// set is closed, so it does not need to be; an unrecognized type gets a
// small fixed estimate rather than 0, so a future box shape undercounts
// rather than vanishing entirely.
func boxMemBytesDeep(v any) int64 {
	switch x := v.(type) {
	case nil:
		return 0
	case []any:
		n := int64(24) // slice header
		for _, e := range x {
			n += 16 + boxMemBytesDeep(e) // interface header + element
		}
		return n
	case map[string]any:
		n := int64(48) // map header estimate
		for k, e := range x {
			n += int64(len(k)) + 16 + boxMemBytesDeep(e)
		}
		return n
	case []float32:
		return 24 + int64(len(x))*4
	case string:
		return 16 + int64(len(x))
	case []byte:
		return 24 + int64(len(x))
	case int64, float64:
		return 8
	case int32, float32:
		return 4
	case bool:
		return 1
	default:
		return 16
	}
}

// float64Extractor reads a float64 value from a vector at a given row index.
// Pre-resolved during Init to eliminate per-row type switches in updateGroup.
type float64Extractor func(v *batch.Vector, row int) float64

// resolveFloat64Extractor returns a typed float64 extractor for the given
// column type, or nil if the type has no numeric reading — updateGroup reads
// nil as "skip every row", so the aggregate answers NULL.
//
// The type list lives in numeric_promote.go, shared with the window operator's
// vecFloat64 and matched to kernel.ResolveRowSum's, because three lists that
// disagree is three different answers to one query. PORT, PROTOCOL and
// DURATION were absent and made STDDEV/VARIANCE/MEDIAN/PERCENTILE/MODE over
// them answer NULL; TypeDate's absence was the same defect fixed for one type
// only (#353).
func resolveFloat64Extractor(typ batch.TypeID) float64Extractor {
	if !numericPromotable(typ) {
		return nil
	}
	return func(v *batch.Vector, row int) float64 {
		f, _ := numericFloat64(v, row)
		return f
	}
}

// resolveOrderKeyExtractor is resolveFloat64Extractor for MIN_BY/MAX_BY's
// ORDERING column, which only has to order — it never becomes the answer. That
// admits IPV4, MAC and BOOL, whose stored integer form orders like the value.
func resolveOrderKeyExtractor(typ batch.TypeID) float64Extractor {
	if !orderKeyPromotable(typ) {
		return nil
	}
	return func(v *batch.Vector, row int) float64 {
		f, _ := orderKeyFloat64(v, row)
		return f
	}
}
