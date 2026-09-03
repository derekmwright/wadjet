package parquet

import (
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// DecimalRescale moves an unscaled DECIMAL carrier from the scale the FILE
// declares for it to the scale the CATALOG declares for the column, and holds
// the result to the catalog's precision.
//
// ADR-0018 is the charter: a parquet file's own numbers are INPUT, not fact.
// For a DECIMAL that is not a figure of speech — the column chunk carries only
// the unscaled integer and the SCHEMA carries the scale, so half of every
// value lives in a declaration. When two files of one table declare that half
// differently (a foreign writer, a pre-#647 write path, an unrepaired #608
// file), reading both under one declaration means a different NUMBER, silently:
// 12.7500 written at scale 4 reads back as 1275.00 under a scale of 2 (#707).
//
// The catalog is the authority for a table's type, so the file's carrier is
// moved to the catalog's scale rather than reinterpreted under it. PostgreSQL
// decides what "moved" means and it is the ASSIGNMENT cast, verified live on
// postgres:17-alpine:
//
//	12.7567::numeric(15,2)   -> 12.76     rounds half AWAY FROM ZERO
//	(-12.7550)::numeric(15,2) -> -12.76
//	12.75::numeric(15,4)     -> 12.7500   widening is exact
//	123456789012.3456::numeric(9,2) -> 22003 numeric field overflow
//
// which is exactly DecimalValueFromText's contract, so this routes through it
// rather than growing a second scaling rule beside the one ADR-0024 already
// gates. `Text` renders the carrier at the file's scale and the resolver reads
// it back at the catalog's; the two are inverse by construction, which is why
// a scales-agree call is the identity and returns before either runs.
//
// The cost of going through text is deliberate. This fires only when a file's
// declaration DISAGREES with the catalog's — a repair, not a read — and every
// ordinary file takes the equal-scales exit above with no work at all. Buying
// a second hand-rolled 128-bit divide for a path that by definition runs on
// files this writer did not produce is the trade ADR-0018 §3 exists to refuse.
func DecimalRescale(d Decimal128, fromScale, toScale, precision int) (Decimal128, error) {
	if fromScale < 0 || fromScale > MaxDecimalDigits {
		return Decimal128{}, decimalFileScaleError(fromScale)
	}
	if toScale == fromScale {
		// Still held to the declared precision: a file may carry a value its
		// own (p, s) admits and the catalog's does not, and admitting it here
		// would put a value past 10^p into a column that promises not to hold
		// one — the assumption the set-operation mover relies on when it skips
		// the fit check (DecimalValueFromUnscaled).
		return DecimalValueFromUnscaled(d, precision, toScale)
	}
	return DecimalValueFromText(d.Text(fromScale), precision, toScale)
}

// decimalFileScaleError refuses a file whose DECIMAL scale no carrier can
// honour. The parquet format permits a negative scale and this package's
// carrier does not represent one, so the file is refused BY NAME rather than
// read as though the scale were zero — which would be a wrong number with no
// error (ADR-0018 rule 2).
func decimalFileScaleError(scale int) error {
	return sqlerr.New("22003",
		"parquet: a DECIMAL column declares scale %d, which no 128-bit unscaled carrier "+
			"can represent (0..%d)", scale, MaxDecimalDigits)
}

// DecimalFileScale reports the scale a file's own leaf declares for a DECIMAL
// column, and whether the leaf IS a decimal at all.
//
// ok=false is the boundary of the reconciliation above, and it is a real case
// rather than a defensive one: a catalog DECIMAL over a leaf carrying no
// decimal annotation says nothing about a scale, so ADR-0018 §4's rule stands
// — the carrier is already the unscaled value at the column's scale, and there
// is nothing to move. Both read paths ask this question before rescaling, and
// both refuse such a pairing earlier anyway (checkRetypeAdmissible on the row
// side, columnDecodePlan on the native one), so ok=false reaching a caller is
// the pairing they admit: a file that carries the column as a decimal at the
// catalog's own scale.
func DecimalFileScale(leaf *SchemaNode) (int, bool) {
	if leaf == nil || !leaf.IsLeaf() {
		return 0, false
	}
	if TypeIDFromSchemaNode(leaf) != TypeDecimal {
		return 0, false
	}
	return int(leaf.Scale), true
}

// DecimalRescalePlan is what one column read needs to know to reconcile a
// file's DECIMAL declaration with the catalog's: the scale to move FROM and
// whether any move is needed.
//
// need=false covers both no-op cases — the leaf is not a decimal, or it is one
// at the catalog's own scale — so a caller writes one branch and the ordinary
// file pays one integer comparison per column chunk.
//
// The boundary, stated because it is a claim and the corpus attempts it from
// both sides: this reconciles a SCALE disagreement and nothing else. A file
// that agrees with the catalog about the scale is read exactly as before, and
// its values are NOT held to the catalog's precision at read — that would be a
// per-value band check on every DECIMAL column of every ordinary file, which is
// a different change with a different cost, and #647 already refuses such a
// value at the door it is written through.
func DecimalRescalePlan(leaf *SchemaNode, want Column) (fromScale int, need bool) {
	if want.Type != TypeDecimal {
		return 0, false
	}
	fs, ok := DecimalFileScale(leaf)
	if !ok || fs == want.Scale {
		return 0, false
	}
	return fs, true
}
