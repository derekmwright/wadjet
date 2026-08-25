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
	setOpInt32Digits = 10
	setOpInt64Digits = 19
)

// setOpDecimalTarget is the DECIMAL(p,s) a set operation's arms must all
// carry when their common type is DECIMAL.
//
//	scale     = max over the arms
//	precision = max over the arms of (precision - scale), plus that scale
//
// The scale is the maximum because that is the only choice that moves no
// value: a narrower one would DROP digits the wider arm holds, which is the
// truncating half of #533 (`UNION` emitted 12.75 twice for 12.7500 and
// 12.7501). Precision is then reconstructed from the widest INTEGER part
// rather than taken as max(precision), because max(precision) is not a bound
// on the widened values — DECIMAL(18,2) alongside DECIMAL(9,4) needs 16
// integer digits at scale 4, i.e. 20, where max(precision) would declare 18
// and hand the parquet writer a leaf too small for the value (ADR-0018 §4's
// encoding rule keys off precision). For every pair whose integer parts are
// ordered the same way as their precisions — which is every case the fixtures
// and the corpus reach, (9,2)/(18,4)/(38,10) included — the two rules agree,
// so this stays in step with the single-process path's own widening (#532).
//
// The result is capped at the carrier's full width (ADR-0012 item 9's SUM
// rule: 38 digits is what an Int128 holds). A value that has no Int128 at the
// output scale is then an ERROR at the moment of coercion, never a wrapped
// number — exec.DecimalCoerce raises it.
//
// ok=false means an arm's (p,s) could not be resolved — a computed DECIMAL
// expression carries none, and declaredProjectionDecimal declines on those
// for the same reason. The caller then leaves every arm as written, which is
// the pre-#533 behaviour, and the shuffle writer's own scale check
// (internal/worker/shuffle_format.go) is what keeps the residual from being
// silent.
func setOpDecimalTarget(arms []setOpColType) (logical.DecimalMeta, bool) {
	if len(arms) == 0 {
		return logical.DecimalMeta{}, false
	}
	scale, intDigits := 0, 0
	for _, a := range arms {
		var m logical.DecimalMeta
		switch a.typ {
		case parquet.TypeDecimal:
			if !a.decKnown {
				return logical.DecimalMeta{}, false
			}
			m = a.dec
		case parquet.TypeInt32:
			m = logical.DecimalMeta{Precision: setOpInt32Digits}
		case parquet.TypeInt64:
			m = logical.DecimalMeta{Precision: setOpInt64Digits}
		default:
			return logical.DecimalMeta{}, false
		}
		if m.Scale > scale {
			scale = m.Scale
		}
		if d := m.Precision - m.Scale; d > intDigits {
			intDigits = d
		}
	}
	prec := intDigits + scale
	if prec > batch.MaxDecimalPrecision {
		prec = batch.MaxDecimalPrecision
	}
	if prec < 1 {
		prec = 1
	}
	return logical.DecimalMeta{Precision: prec, Scale: scale}, true
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
