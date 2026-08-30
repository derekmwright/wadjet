package pgwire

import (
	"encoding/binary"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// The #454 regression. `sendTypedRowDescription` wrote a constant -1 into
// RowDescription's type modifier, which for `numeric` is where PostgreSQL
// packs the precision and scale — so `DECIMAL(9,2)` reached a JDBC or ODBC
// client as an unconstrained numeric and getPrecision()/getScale() answered
// 0 or "unlimited".
//
// The expected numbers are PostgreSQL's own: numerictypmodin packs
// ((precision << 16) | scale) + VARHDRSZ, so numeric(9,2) is 589830.

func TestPgTypeModPacksNumericPrecisionAndScale(t *testing.T) {
	cases := []struct {
		name string
		meta wadjet.ColumnMeta
		want int32
	}{
		// The three the wire oracle's dec_probe fixture declares, with the
		// values PostgreSQL sends for the same DDL.
		{"numeric_9_2", decMeta(9, 2), 589830},
		{"numeric_18_4", decMeta(18, 4), 1179656},
		{"numeric_38_10", decMeta(38, 10), 2490382},
		// Scale 0 is a declaration too: numeric(9,0) is not an unconstrained
		// numeric.
		{"numeric_9_0", decMeta(9, 0), 589828},
		// No precision means the declaration never reached us — a plan-declared
		// schema for a zero-row result, or an inferred type. -1 (unconstrained)
		// is the honest answer, not a fabricated (0,0).
		{"decimal_without_a_declaration", decMeta(0, 0), -1},
		// FIX 2 (fold-in to #457/#458): MIN/MAX/MIN_BY/MAX_BY/SUM/AVG over a
		// DECIMAL(p,s) column know their REAL (p,s) internally — Precision/
		// Scale here are non-zero, deliberately — but live postgres:17-
		// alpine's \gdesc reports typmod -1 for every one of them: an
		// aggregate function call never carries its argument's typmod
		// through, only a bare column reference does. WireUnconstrained is
		// how declaredWireUnconstrainedDecimal (physical package) tells
		// TypeMod that, independent of what Precision/Scale answer.
		{"decimal_aggregate_output_wire_unconstrained", wadjet.ColumnMeta{
			Name: "d", TypeName: "DECIMAL", TypeID: parquet.TypeDecimal,
			Precision: 9, Scale: 2, WireUnconstrained: true,
		}, -1},
		// Every other type has no modifier, and must keep sending -1.
		{"int64", wadjet.ColumnMeta{Name: "c", TypeName: "INT64", TypeID: parquet.TypeInt64}, -1},
		{"string", wadjet.ColumnMeta{Name: "c", TypeName: "STRING", TypeID: parquet.TypeString}, -1},
		{"timestamp", wadjet.ColumnMeta{Name: "c", TypeName: "TIMESTAMP", TypeID: parquet.TypeTimestamp}, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := TypeMod(tc.meta); got != tc.want {
				t.Errorf("TypeMod = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestRowDescriptionCarriesTheTypeModifier reads the modifier back off the
// WIRE, because that is the thing the client sees: a correct TypeMod that
// the writer never calls would leave the defect exactly where it was.
func TestRowDescriptionCarriesTheTypeModifier(t *testing.T) {
	rc := &recordConn{}
	c := &pgConn{conn: rc}
	metas := []wadjet.ColumnMeta{decMeta(9, 2), {Name: "n", TypeName: "INT64", TypeID: parquet.TypeInt64}}
	c.sendTypedRowDescription(metas, nil)

	oids, mods := parseRowDescOIDsAndMods(t, rc.buf.Bytes())
	if len(oids) != 2 {
		t.Fatalf("RowDescription described %d columns, want 2", len(oids))
	}
	if oids[0] != 1700 {
		t.Fatalf("DECIMAL declared OID %d, want 1700 (numeric)", oids[0])
	}
	if mods[0] != 589830 {
		t.Errorf("numeric(9,2) type modifier = %d, want 589830 ((9<<16|2)+4)", mods[0])
	}
	if mods[1] != -1 {
		t.Errorf("int8 type modifier = %d, want -1", mods[1])
	}
}

func decMeta(precision, scale int) wadjet.ColumnMeta {
	return wadjet.ColumnMeta{
		Name: "d", TypeName: "DECIMAL", TypeID: parquet.TypeDecimal,
		Precision: precision, Scale: scale,
	}
}

// parseRowDescOIDsAndMods reads a 'T' message's per-field OID and type
// modifier. The existing parseRowDescTyped deliberately skips the modifier
// bytes, which is why it never saw this.
func parseRowDescOIDsAndMods(t *testing.T, raw []byte) ([]int32, []int32) {
	t.Helper()
	if len(raw) < 5 || raw[0] != 'T' {
		t.Fatalf("not a RowDescription: % x", raw)
	}
	body := raw[5:]
	n := int(binary.BigEndian.Uint16(body[:2]))
	body = body[2:]
	oids := make([]int32, 0, n)
	mods := make([]int32, 0, n)
	for i := 0; i < n; i++ {
		end := 0
		for end < len(body) && body[end] != 0 {
			end++
		}
		body = body[end+1:] // name + NUL
		if len(body) < 18 {
			t.Fatalf("field %d truncated", i)
		}
		// tableOID(4) attnum(2) typeOID(4) size(2) typmod(4) fmt(2)
		oids = append(oids, int32(binary.BigEndian.Uint32(body[6:10])))
		mods = append(mods, int32(binary.BigEndian.Uint32(body[12:16])))
		body = body[18:]
	}
	return oids, mods
}
