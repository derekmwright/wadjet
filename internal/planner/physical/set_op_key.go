package physical

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// setOpKeyer derives local UNION/INTERSECT/EXCEPT membership keys from DECLARED
// column types, not boxes: equality must imply identical keys (#499, #546;
// ADR-0012 items 8 and 10). Normalize equal DECIMAL scales, ±0 and NaN payloads
// (ADR-0012 item 8), and equivalent CIDR address/host-route spellings (#546, #520).
// Schema distinguishes rendered DECIMAL/CIDR text from STRING values.
// Encode the same bytes as exec.appendColumnValue so local set ops, GROUP BY
// and the DAG's set-op aggregate agree.
// See docs/internals/set-operation-equality-key-encoding.md for the design.
type setOpKeyer struct {
	cols []parquet.Column
}

// newSetOpKeyer returns nil when the schema cannot type the rows — an arm that
// produced nothing has none — and the caller then falls back to rowHashKey,
// which is what every set operation used before this existed.
func newSetOpKeyer(schema []parquet.Column) *setOpKeyer {
	if len(schema) == 0 {
		return nil
	}
	return &setOpKeyer{cols: schema}
}

// key encodes one row. Columns are visited in SCHEMA order, which is the order
// SQL says the arms correspond in, so no name sort is needed — and every field
// is self-delimiting, so two adjacent values cannot run together.
//
// A row the schema does not describe — a different width, or a name the map
// does not carry — falls back to rowHashKey for the WHOLE row rather than for
// the odd column, so a row is never keyed two different ways within one
// operation.
func (k *setOpKeyer) key(row map[string]any) string {
	if k == nil || len(row) != len(k.cols) {
		return rowHashKey(row)
	}
	var b strings.Builder
	for i := range k.cols {
		c := &k.cols[i]
		v, ok := row[c.Name]
		if !ok {
			return rowHashKey(row)
		}
		b.WriteByte(0)
		if v == nil {
			// A NULL is a member like any other in a set operation, and
			// matches a NULL on the other side. Its own tag, so it cannot
			// collide with the empty string.
			b.WriteByte('N')
			continue
		}
		b.WriteByte('v')
		b.WriteString(keyValueText(c, v))
	}
	return b.String()
}

// keyValueText encodes one boxed value under its column's declared type.
// Anything the declaration does not single out keeps the rendering
// rowHashKey always used, which is right for every type whose text is already
// one-to-one with its value.
func keyValueText(c *parquet.Column, v any) string {
	switch c.Type {
	case parquet.TypeDecimal:
		if s, ok := v.(string); ok {
			// The rendering carries exactly the column's scale, so the
			// conversion is exact and AppendDecimalKey's own trailing-zero
			// stripping does the scale normalization: 12.75 keys alike at
			// scale 2 and at scale 4, which is what the comparator says and
			// what #474 already made the columnar key do.
			//
			// The CHECKED parser, so a saturated value can never become a
			// key. The text here is FormatDecimal output for a value the
			// column already holds, so it cannot be out of range today — and
			// the checked call is what keeps that a structural fact rather
			// than an argument: Int128Max is one key for every out-of-range
			// value, so a saturating parse would silently merge distinct
			// members of a UNION (the key-side twin of #553). An error falls
			// through to the `%v` rendering below, which is injective for
			// decimal text and so is a correct, if unnormalized, key.
			if d, err := batch.ParseDecimalStringChecked(s, int(c.Scale)); err == nil {
				return string(batch.AppendDecimalKey(nil, d, int(c.Scale)))
			}
		}
	case parquet.TypeCIDR:
		if s, ok := v.(string); ok {
			// PostgreSQL's inet order, the key every OTHER CIDR consumer
			// already uses (#520): a bare address and its own /32 host route
			// are ONE value, so they must be one member of a set operation.
			// Without this arm the single-process path dedups a CIDR column
			// by its raw stored TEXT while the stage DAG — which lowers a set
			// operation to a GroupByAll aggregate, keyed through
			// kernel.CidrOrderKey — dedups it by inet, and the identical
			// UNION answers 4 rows locally and 3 on the DAG (#546).
			//
			// CidrOrderKey, not CidrSortKey: this is a KEY, and every stored
			// value needs one. CidrSortKey answers ok=false for a value that
			// names no address and the column is unvalidated text
			// (internal/storage/ingest), so discarding those to "" would
			// dedup three DISTINCT malformed values into one member — the
			// same defect this arm exists to fix, wearing the other sign.
			// CidrOrderKey falls back to the value's own raw text, which
			// keeps byte-identical garbage together and everything else
			// apart, exactly as ORDER BY and GROUP BY key it.
			return kernel.CidrOrderKey(s)
		}
	case parquet.TypeFloat64:
		if f, ok := v.(float64); ok {
			return fmt.Sprintf("%016x", kernel.KeyFloat64Bits(f))
		}
	case parquet.TypeFloat32:
		if f, ok := v.(float32); ok {
			return fmt.Sprintf("%08x", kernel.KeyFloat32Bits(f))
		}
		if f, ok := v.(float64); ok {
			return fmt.Sprintf("%08x", kernel.KeyFloat32Bits(float32(f)))
		}
	case parquet.TypeArray, parquet.TypeMap, parquet.TypeRow, parquet.TypeVector:
		// A container's `%v` rendering is not injective — ARRAY['a b'] and
		// ARRAY['a','b'] both print `[a b]`, ROW{a:'b c:d'} and
		// ROW{a:'b',c:'d'} both print `map[a:b c:d]` — so two DISTINCT values
		// became one member: UNION merged them, INTERSECT of disjoint sets
		// returned a row (#612). exec.AppendBoxedGroupKey is the engine's own
		// boxed group key, the SAME producer the coordinator's cross-worker
		// re-aggregation was moved onto in #566/ADR-0023: it walks the
		// container's elements/fields with their declared metadata, framing
		// every element so no separator can be swallowed and re-keying a
		// nested CIDR leaf into inet order and folding a nested float's NaN
		// payload / -0.0 the way the comparator does. Sharing the producer is
		// the point — a second implementation of a key is a second definition
		// of equality, which is how the local path and the DAG (whose set-op
		// lowering keys columnar) drifted apart for the flat types (#546).
		// The bytes are a KEY, not text, so they are returned verbatim as a
		// string; keyValueText's result already only ever feeds a hash map.
		return string(exec.AppendBoxedGroupKey(nil, v, c))
	}
	return fmt.Sprintf("%v", v)
}
