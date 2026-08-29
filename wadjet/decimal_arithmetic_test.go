package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Exact DECIMAL arithmetic end to end — #555's execution half, ADR-0024
// items 3 and 4.
//
// Before this, every arithmetic expression over a DECIMAL column declared
// FLOAT64 and computed in float64: `a - b` over 12.75 and 12.7500 answered
// -9.999999999976694e-05 where the exact difference is 0, `a / b` answered 1
// where PostgreSQL answers 0.99999215690465, and a client reading the column
// got OID 701 where PostgreSQL declares numeric.
//
// Every expected value here was verified against postgres:17.11. Where the
// digit COUNT differs it is the recorded class of ADR-0012 item 12: a wadjet
// DECIMAL column has one declared scale, so a result renders with that scale's
// trailing zeros where PostgreSQL's per-value dscale prints fewer. Same number,
// same rows.

// TestDecimalArithmeticIsExact is the value gate over the ddr fixture.
func TestDecimalArithmeticIsExact(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		// a DECIMAL(9,2) = 12.75, b DECIMAL(18,4) = 12.7501 on row 2.
		// The float answer for the difference was -9.999999999976694e-05.
		{"subtraction is exact", "SELECT a - b AS v FROM decdecl WHERE id = 2", "-0.0001"},
		{"addition takes the wider scale", "SELECT a + b AS v FROM decdecl WHERE id = 1", "25.5000"},
		// PostgreSQL: 12.75 * 12.7500 = 162.562500, scale s1+s2 = 6.
		{"multiplication sums the scales", "SELECT a * b AS v FROM decdecl WHERE id = 1", "162.562500"},
		// PostgreSQL: 12.75 / 12.7501 = 0.99999215692425941757, at the scale
		// its magnitude-dependent rule picks (20 digits here); wadjet keeps 21
		// by item 3's FIXED rule. The two agree to min(scale) and differ only
		// in how many digits past it they keep — the divergence ADR-0024 item 3
		// records, and the reason a division is not an exactNumeric oracle
		// entry unless its quotient terminates.
		{"division rounds half away from zero once",
			"SELECT a / b AS v FROM decdecl WHERE id = 2", "0.999992156924259417573"},
		{"modulo takes the dividend's sign",
			"SELECT a % 3 AS v FROM decdecl WHERE id = 5", "-0.01"},
		// A numeric literal's (p,s) is its spelling, so the product keeps
		// the column's scale rather than the INT32 range's ten digits.
		{"literal keeps its spelling", "SELECT a * 2 AS v FROM decdecl WHERE id = 1", "25.50"},
		{"fractional literal contributes its scale",
			"SELECT a + 0.005 AS v FROM decdecl WHERE id = 1", "12.755"},
		{"unary minus moves no digit", "SELECT -a AS v FROM decdecl WHERE id = 1", "-12.75"},
		{"unary minus inside arithmetic",
			"SELECT -a + b AS v FROM decdecl WHERE id = 2", "0.0001"},
		{"nested arithmetic", "SELECT (a + b) * a AS v FROM decdecl WHERE id = 1", "325.125000"},
		// An INTEGER column brings its whole range at scale 0.
		{"integer column operand", "SELECT a * id AS v FROM decdecl WHERE id = 4", "8.00"},
		// An aggregate's input (p,s) is the EXPRESSION's, not a bare column's:
		// SUM over DECIMAL(11,2) is DECIMAL(38,2) and AVG over DECIMAL(13,6) is
		// DECIMAL(38,10) (ADR-0012 item 9).
		{"aggregate over arithmetic", "SELECT SUM(a * 2) AS v FROM decdecl", "105.98"},
		{"average over arithmetic", "SELECT AVG(a / 2) AS v FROM decdecl", "4.4158333333"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			if len(res.Rows) != 1 {
				t.Fatalf("%s returned %d rows, want 1", tc.sql, len(res.Rows))
			}
			got, ok := res.Rows[0]["v"].(string)
			if !ok {
				t.Fatalf("%s: v = %#v (%T), want the DECIMAL text — a non-string box means "+
					"the answer came back through float64", tc.sql, res.Rows[0]["v"], res.Rows[0]["v"])
			}
			if got != tc.want {
				t.Errorf("%s = %q, want %q", tc.sql, got, tc.want)
			}
		})
	}
}

// TestDecimalArithmeticDeclaresItsResultType is ADR-0024 item 3's (p,s) at the
// WIRE, where a client meets it. The typmod is unconstrained for every one of
// these, because PostgreSQL's select_common_typmod drops it for an operator
// (item 5).
func TestDecimalArithmeticDeclaresItsResultType(t *testing.T) {
	db := ddrOpen(t)
	for _, tc := range []struct {
		sql              string
		precision, scale int
	}{
		{"SELECT a + b AS v FROM decdecl", 19, 4},
		{"SELECT a - b AS v FROM decdecl", 19, 4},
		{"SELECT a * b AS v FROM decdecl", 28, 6},
		{"SELECT a / b AS v FROM decdecl", 32, 21},
		{"SELECT a % b AS v FROM decdecl", 11, 4},
		{"SELECT a * 2 AS v FROM decdecl", 11, 2},
		{"SELECT -a AS v FROM decdecl", 9, 2},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			res := ddrQuery(t, db, tc.sql)
			if len(res.ColumnMetas) != 1 {
				t.Fatalf("%d column metas, want 1", len(res.ColumnMetas))
			}
			m := res.ColumnMetas[0]
			if m.TypeID != parquet.TypeDecimal {
				t.Fatalf("declared %s, want DECIMAL", m.TypeID)
			}
			if m.Precision != tc.precision || m.Scale != tc.scale {
				t.Errorf("declared DECIMAL(%d,%d), want DECIMAL(%d,%d)",
					m.Precision, m.Scale, tc.precision, tc.scale)
			}
			if !m.WireUnconstrained {
				t.Errorf("declared a typmod on the wire; an OPERATOR's result is unconstrained " +
					"numeric in PostgreSQL (ADR-0024 item 5)")
			}
		})
	}
}

// TestDecimalArithmeticFaultsCarryTheirSQLSTATE is ADR-0024 item 4: a value
// with no carrier at its declared type is 22003 and a zero divisor is 22012.
// The two are separate because PostgreSQL keeps them separate — a caller that
// branched on "did it produce a value" would report a numeric overflow for
// `x / 0`.
func TestDecimalArithmeticFaultsCarryTheirSQLSTATE(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test", SpillDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	// Two DECIMAL(38,0) columns holding values whose PRODUCT needs more than
	// 38 digits: the result type of (38,0) x (38,0) is DECIMAL(38,0) after
	// item 3's adjustment, and 10^37 x 10 has no place in it.
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "big", Type: parquet.TypeDecimal, Precision: 38, Scale: 0, Nullable: true},
		{Name: "small", Type: parquet.TypeDecimal, Precision: 38, Scale: 0, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "decovf_arith", schema, nil); err != nil {
		t.Fatal(err)
	}
	big, ok := new(bigIntText).parse("10000000000000000000000000000000000000") // 10^37
	if !ok {
		t.Fatal("fixture literal is not an integer")
	}
	rows := []map[string]any{
		{"id": int64(1), "big": big, "small": decimal128Of(10)},
		{"id": int64(2), "big": decimal128Of(1), "small": decimal128Of(0)},
	}
	ing := db.NewIngester("decovf_arith", schema, nil, ingest.Config{MaxBufferRows: 8})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name  string
		sql   string
		state string
	}{
		// 10^37 * 10 = 10^38, which DECIMAL(38,0) cannot declare.
		{"product past the declared precision",
			"SELECT big * small AS v FROM decovf_arith WHERE id = 1", "22003"},
		{"division by a zero column",
			"SELECT big / small AS v FROM decovf_arith WHERE id = 2", "22012"},
		{"modulo by a zero column",
			"SELECT big % small AS v FROM decovf_arith WHERE id = 2", "22012"},
		{"division by a zero literal",
			"SELECT big / 0 AS v FROM decovf_arith", "22012"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.Query(ctx, tc.sql)
			if err == nil {
				t.Fatalf("%s answered instead of failing — an unrepresentable value is an "+
					"error, never a number (ADR-0024 item 4)", tc.sql)
			}
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Errorf("%s failed with SQLSTATE %q (%v), want %q", tc.sql, got, err, tc.state)
			}
		})
	}
}

// decimal128Of is the unscaled carrier a parquet DECIMAL box carries, sign
// extended the way two's complement requires.
func decimal128Of(v int64) parquet.Decimal128 {
	hi := int64(0)
	if v < 0 {
		hi = -1
	}
	return parquet.Decimal128{Hi: hi, Lo: uint64(v)}
}

// bigIntText parses a decimal integer wider than an int64 into the two halves
// parquet.Decimal128 carries. The fixture needs 10^37, which is 38 digits and
// past int64 entirely — a float64 box would be scaled on the way in and a
// string box goes through ParseFloat, so neither can carry it (the reason
// pgDecimalRows boxes its wide column the same way).
type bigIntText struct{}

func (bigIntText) parse(s string) (parquet.Decimal128, bool) {
	var hi, lo uint64
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return parquet.Decimal128{}, false
		}
		// value = value*10 + digit, over a 128-bit magnitude.
		var carry uint64
		lo, carry = mul10Add(lo, uint64(s[i]-'0'))
		hi, _ = mul10Add(hi, carry)
	}
	return parquet.Decimal128{Hi: int64(hi), Lo: lo}, true
}

// mul10Add returns v*10 + add and the carry out of 64 bits.
func mul10Add(v, add uint64) (uint64, uint64) {
	const mask32 = 1<<32 - 1
	loPart := (v&mask32)*10 + add
	hiPart := (v>>32)*10 + loPart>>32
	return hiPart<<32 | loPart&mask32, hiPart >> 32
}
