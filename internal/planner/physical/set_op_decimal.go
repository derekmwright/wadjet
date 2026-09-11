package physical

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// setOpIntDigits is how many decimal digits an integer type's whole range
// needs, which is what it contributes to a set operation's common DECIMAL
// precision. INT32 spans 10 digits, INT64 spans 19.
const (
	setOpInt32Digits = batch.Int32DecimalDigits
	setOpInt64Digits = batch.Int64DecimalDigits
)

// setOpDecimalTarget requires every DECIMAL arm to carry one common (p,s):
// scale=max(scales), precision=max(integer digits)+scale, capped at 38.
// Never narrow scale or use max(precision) alone (#533, #532; ADR-0018 §4).
// Values not representable in Int128 at output scale fail during DecimalCoerce,
// never wrap (ADR-0012 item 9). Unknown arm (p,s) returns false; callers refuse
// and name the column (#551, #533), never leave independently scaled .wshf files.
// The shuffle writer's check sees only one writer and cannot catch cross-file
// scale disagreements.
// See docs/internals/set-operation-decimal-target.md for the design.
func setOpDecimalTarget(arms []setOpColType) (logical.DecimalMeta, bool) {
	if len(arms) == 0 {
		return logical.DecimalMeta{}, false
	}
	in := make([]batch.DecimalType, 0, len(arms))
	for _, a := range arms {
		if a.typ == parquet.TypeDecimal && !a.decKnown {
			return logical.DecimalMeta{}, false
		}
		m, ok := batch.DecimalTypeOf(a.typ, batch.DecimalType{Precision: a.dec.Precision, Scale: a.dec.Scale})
		if !ok {
			return logical.DecimalMeta{}, false
		}
		in = append(in, m)
	}
	// batch.DecimalCommon is ADR-0024 item 2's rule, and it is the SAME
	// function CASE/COALESCE/GREATEST/LEAST reconcile through: one table of
	// rules replaces the five that were independently derived. It carries
	// the 38-digit cap and the integer-part rebuild that used to live here.
	m, ok := batch.DecimalCommon(in)
	if !ok {
		return logical.DecimalMeta{}, false
	}
	return logical.DecimalMeta{Precision: m.Precision, Scale: m.Scale}, true
}

// setOpColDecimalMeta reads a resolved column's DECIMAL declaration out of
// the maps the arm walk already builds, through the same qualifier-stripping
// lookup the declared-schema walk uses. Precision 0 is the "unconstrained"
// sentinel #458 uses for a DECIMAL nothing could type, and is reported as
// UNRESOLVED here rather than taken at face value: a set operation would
// otherwise widen every arm to scale 0 and truncate all of them.
func setOpColDecimalMeta(dec map[string]logical.DecimalMeta, col string) (logical.DecimalMeta, bool) {
	m, ok := lookupColDecimal(dec, col)
	if !ok || m.Precision <= 0 {
		return logical.DecimalMeta{}, false
	}
	return m, true
}
