package parquet

import (
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// DecimalRescale converts FILE-scale carriers to CATALOG scale and precision
// (ADR-0018; #647, #608, #707). Never reinterpret a carrier at another scale.
// Use DecimalValueFromText over Text(fromScale) for assignment rounding:
// half away from zero, exact widening, 22003 on precision overflow (ADR-0024).
// Equal scales avoid text conversion but still enforce declared precision.
// Reject file scales outside 0..MaxDecimalDigits.
// Keep one shared scaling rule, not a separate Int128 divide (ADR-0018 §3).
// See docs/internals/parquet-decimal-file-scale-reconciliation.md for the design.
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

// DecimalFileDeclaration reports the (precision, scale) a file's own leaf
// declares for a DECIMAL column, and whether the leaf IS a decimal at all.
func DecimalFileDeclaration(leaf *SchemaNode) (precision, scale int, ok bool) {
	if leaf == nil || !leaf.IsLeaf() {
		return 0, 0, false
	}
	if TypeIDFromSchemaNode(leaf) != TypeDecimal {
		return 0, 0, false
	}
	return int(leaf.Precision), int(leaf.Scale), true
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

// DecimalRescalePlan reports file scale and whether DECIMAL (p,s) differs.
// need=false for non-DECIMAL or matching declarations; ordinary chunks pay
// only declaration comparisons, with no per-value reconciliation.
// Scale drift moves carriers; precision drift enforces the catalog's range
// even without a scale change. Row and native reads must agree
// (ADR-0013 two-path property, ADR-0024).
// See docs/internals/parquet-decimal-reconciliation-plan.md for the design.
func DecimalRescalePlan(leaf *SchemaNode, want Column) (fromScale int, need bool) {
	if want.Type != TypeDecimal {
		return 0, false
	}
	fp, fs, ok := DecimalFileDeclaration(leaf)
	if !ok {
		return 0, false
	}
	if fs == want.Scale && fp == want.Precision {
		return 0, false
	}
	return fs, true
}
