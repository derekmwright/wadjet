package wadjet

import (
	"context"
	"math/big"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #541: the single-process set-operation adapter did not reconcile arms of
// different TYPE at all. It boxed each arm's rows and handed them to
// batch.FromRows under the FIRST arm's schema, so the arm ORDER decided the
// answer — and the boxes are not uniform across types (a DECIMAL is its
// rendered TEXT, an integer a raw int64), which is what turned a type
// question into a value one.
//
// The stage DAG has reconciled these at plan time since #533
// (reconcileSetOpArmTypes → setOpWiden / setOpDecimalTarget). unifySetOpSchemas
// now calls the same two functions, so the two paths cannot answer with
// different types for the same query.
//
// #553 rides along: the value the adapter materializes is written through
// batch.FromRowsChecked, so a value with no Int128 at the union's scale is a
// 22003 error rather than the saturated end of the carrier's range.
//
// Every expectation below is PostgreSQL 17's, per the issue bodies.
func setopTypeDB(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	create := func(name string, cols []parquet.Column, rows []map[string]any) {
		t.Helper()
		schema := parquet.Schema{Columns: cols}
		if err := db.CreateTable(ctx, name, schema, nil); err != nil {
			t.Fatal(err)
		}
		ing := db.NewIngester(name, schema, nil, ingest.Config{MaxBufferRows: 16})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// numeric(18,4) holding 2.5000, alongside a bigint and a float8.
	create("t_dec4",
		[]parquet.Column{{Name: "d", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true}},
		[]map[string]any{{"d": parquet.Decimal128{Lo: 25000}}})
	create("t_int",
		[]parquet.Column{{Name: "i", Type: parquet.TypeInt64, Nullable: true}},
		[]map[string]any{{"i": int64(1)}})
	create("t_f8",
		[]parquet.Column{{Name: "f", Type: parquet.TypeFloat64, Nullable: true}},
		[]map[string]any{{"f": 0.5}})
	create("t_i32",
		[]parquet.Column{{Name: "s", Type: parquet.TypeInt32, Nullable: true}},
		[]map[string]any{{"s": int32(3)}})

	// The #552/#553 shape: DECIMAL(38,0) holding 10^30 beside DECIMAL(11,10).
	e30, _ := new(big.Int).SetString("1000000000000000000000000000000", 10)
	lo := new(big.Int).And(e30, new(big.Int).SetUint64(^uint64(0)))
	hi := new(big.Int).Rsh(e30, 64)
	create("t_ovf",
		[]parquet.Column{
			{Name: "d380", Type: parquet.TypeDecimal, Precision: 38, Scale: 0, Nullable: true},
			{Name: "d1110", Type: parquet.TypeDecimal, Precision: 11, Scale: 10, Nullable: true},
		},
		[]map[string]any{
			{"d380": parquet.Decimal128{Hi: hi.Int64(), Lo: lo.Uint64()}, "d1110": parquet.Decimal128{Lo: 10000000001}},
			{"d380": parquet.Decimal128{Lo: 7}, "d1110": parquet.Decimal128{Lo: 20000000000}},
		})
	return db
}

// TestSetOpReconcilesArmsOfDifferentType is #541's three shapes, all on the
// single-process engine, each asserted on the VALUES and on the output TYPE —
// a client reads the column's OID, so a right value under a wrong type is
// still a wrong answer (shape 3 was exactly that).
func TestSetOpReconcilesArmsOfDifferentType(t *testing.T) {
	ctx := context.Background()
	db := setopTypeDB(t)

	for _, tc := range []struct {
		name     string
		sql      string
		wantType parquet.TypeID
		want     []string
	}{
		// Shape 1: `numeric UNION ALL bigint` is numeric, and the integer arm
		// is a VALUE at scale 0 — it has to be multiplied by 10^s, not read
		// as the already-scaled carrier ADR-0018 §4 defines for ingest. The
		// integer 1 used to come back as 0.0001.
		{"numeric_then_bigint", "SELECT d AS v FROM t_dec4 UNION ALL SELECT i FROM t_int",
			parquet.TypeDecimal, []string{"1.0000", "2.5000"}},
		{"bigint_then_numeric", "SELECT i AS v FROM t_int UNION ALL SELECT d FROM t_dec4",
			parquet.TypeDecimal, []string{"1.0000", "2.5000"}},
		// Shape 2: `double precision UNION ALL numeric` resolves to double
		// precision. Under the first arm's FLOAT64 schema the DECIMAL arm's
		// rendered TEXT hit the #361 store guard and the query FAILED
		// ("cannot store string into FLOAT64 vector"), where PostgreSQL
		// answers.
		{"double_then_numeric", "SELECT f AS v FROM t_f8 UNION ALL SELECT d FROM t_dec4",
			parquet.TypeFloat64, []string{"0.5", "2.5"}},
		// Shape 3: the same pair the other way round. float8 is the PREFERRED
		// type of PostgreSQL's numeric category, so this is double precision
		// too — the local path kept the first arm's DECIMAL, which no value
		// comparison can see and a client reading the wire OID can.
		{"numeric_then_double", "SELECT d AS v FROM t_dec4 UNION ALL SELECT f FROM t_f8",
			parquet.TypeFloat64, []string{"0.5", "2.5"}},
		// The integer rungs of the same ladder: `integer ∪ bigint` is bigint.
		{"int_then_bigint", "SELECT s AS v FROM t_i32 UNION ALL SELECT i FROM t_int",
			parquet.TypeInt64, []string{"1", "3"}},
		{"bigint_then_int", "SELECT i AS v FROM t_int UNION ALL SELECT s FROM t_i32",
			parquet.TypeInt64, []string{"1", "3"}},
		// A DISTINCT form, so the dedup key is exercised on a widened column
		// too: the two arms hold different numbers, so both survive.
		{"union_distinct_across_types", "SELECT d AS v FROM t_dec4 UNION SELECT i FROM t_int",
			parquet.TypeDecimal, []string{"1.0000", "2.5000"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatalf("%s: %v", tc.sql, err)
			}
			if len(res.ColumnMetas) != 1 {
				t.Fatalf("%s: expected one output column, got %v", tc.sql, res.Columns)
			}
			if got := res.ColumnMetas[0].TypeID; got != tc.wantType {
				t.Errorf("%s\n  output type %s, want %s (PostgreSQL's set-operation type resolution)",
					tc.sql, got, tc.wantType)
			}
			got := make([]string, 0, len(res.Rows))
			for _, r := range res.Rows {
				got = append(got, setopCellText(t, r["v"]))
			}
			sort.Strings(got)
			if strings.Join(got, ",") != strings.Join(tc.want, ",") {
				t.Errorf("%s\n  got  %v\n  want %v (PostgreSQL 17)", tc.sql, got, tc.want)
			}
		})
	}
}

// TestSetOpDecimalWithNoCarrierIsAnError is #553 end to end on the engine that
// had it: a DECIMAL(38,0) arm holding 10^30 beside a DECIMAL(11,10) arm
// resolves to DECIMAL(38,10), 10^30 at scale 10 needs 10^40, and the value was
// SATURATED to 2^127-1 and returned as
// 17014118346046923173168730371.5884105727 with no error at all.
//
// The refusal is the ADR-0024 item 4 rule (never saturated, wrapped, narrowed
// or zeroed) and its SQLSTATE is PostgreSQL's 22003. It is also the answer the
// stage DAG already gives for the same input, so the two paths now agree —
// they disagreed before, one silently wrong and one loud.
//
// The residual is deliberate and recorded: PostgreSQL's numeric is unbounded
// and answers all four rows here, so wadjet refuses a query PostgreSQL
// answers. ADR-0024 item 7 closes that as the accepted cost of the finite
// carrier (#552), and TestSetOpDecimalCapIsARangeReduction pins it on the DAG.
func TestSetOpDecimalWithNoCarrierIsAnError(t *testing.T) {
	ctx := context.Background()
	db := setopTypeDB(t)

	const sql = "SELECT d380 AS v FROM t_ovf UNION ALL SELECT d1110 FROM t_ovf"
	res, err := db.Query(ctx, sql)
	if err == nil {
		t.Fatalf("%s answered %v; 10^30 has no DECIMAL(38,10) value, and saturating it to "+
			"Int128Max is the silent corruption of #553", sql, res.Rows)
	}
	if got := sqlerr.StateOf(err); got != "22003" {
		t.Errorf("SQLSTATE = %q, want 22003 (numeric_value_out_of_range); err = %v", got, err)
	}
	if strings.Contains(err.Error(), "17014118346046923173168730371") {
		t.Errorf("the error must name the value the ROW holds, not the saturated carrier: %v", err)
	}

	// The same two columns each answer on their own — the union is what makes
	// the value unrepresentable, which is why #552 calls the 38-digit cap a
	// range reduction rather than a rounding rule.
	for _, q := range []string{"SELECT d380 AS v FROM t_ovf", "SELECT d1110 AS v FROM t_ovf"} {
		if _, err := db.Query(ctx, q); err != nil {
			t.Errorf("%s must still answer: %v", q, err)
		}
	}
}

func setopCellText(t *testing.T, v any) string {
	t.Helper()
	switch tv := v.(type) {
	case string:
		return tv
	case float64:
		return strings.TrimRight(strings.TrimRight(bigFloatText(tv), "0"), ".")
	case int64:
		return bigIntText(tv)
	case int32:
		return bigIntText(int64(tv))
	case nil:
		return "NULL"
	}
	t.Fatalf("unexpected cell %#v (%T)", v, v)
	return ""
}

func bigFloatText(f float64) string { return new(big.Float).SetFloat64(f).Text('f', 6) }
func bigIntText(n int64) string     { return new(big.Int).SetInt64(n).String() }
