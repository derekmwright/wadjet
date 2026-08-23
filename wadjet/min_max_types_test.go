package wadjet

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #417's gate, over all 22 types and both aggregation paths.
//
// The six MIN/MAX resolvers in kernel/agg.go named `TypeString, TypeBytes`
// where five types share BytesColumn storage, so MIN/MAX over CIDR, IPV6 and
// UUID resolved to a nil updater: the accumulator was never touched, HasMin
// stayed false and the answer finalized as NULL. BOOL had no arm at all, and
// grouped DECIMAL had a second, independent hole — the flat SoA scatter
// (agg_scatter.go) allocated minDec/maxDec and never wrote them.
//
// No differential gate could see any of it: the two-path suite compares the
// stage DAG against the single-process engine and BOTH take these resolvers,
// so `minmax_c_cidr` passed as two matching wrong answers.
//
// The reference is the engine's own ORDER BY over the same column. That is
// the property the fix actually claims — MIN(col) is the first row of
// `ORDER BY col`, because the aggregate and the sort kernel are supposed to
// agree on one order per type — and it needs no per-type rendering hardcoded
// into the test. The three types the issue names are additionally pinned to
// literal values computed from the fixture generator, so a comparator and an
// aggregate that broke together would still fail.

// mmScalarPath and mmGroupedPath are the two aggregation paths: whole-input
// (agg_whole_input.go) and grouped (the hash aggregate, which for most types
// routes through the flat SoA arrays and for the byte-backed ones through the
// generic kernel.Accumulator).
func TestMinMaxEveryType(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)

	for _, col := range mbTypeCols() {
		col := col
		t.Run(col.Name, func(t *testing.T) {
			wantLo, wantHi, ok := mmOrderByReference(t, db, col.Name)

			scalar, err := db.Query(ctx, fmt.Sprintf(
				"SELECT MIN(%s) AS lo, MAX(%s) AS hi FROM mbtypes", col.Name, col.Name))
			if err != nil {
				t.Fatalf("scalar MIN/MAX: %v", err)
			}
			if len(scalar.Rows) != 1 {
				t.Fatalf("scalar aggregate returned %d rows, want 1", len(scalar.Rows))
			}

			if !ok {
				// Every column mbTypeCols() carries — scalar or container —
				// has an engine-side ORDER BY reference now: #426 gave
				// ARRAY/ROW/MAP/VECTOR a MIN/MAX answer over the same total
				// order #415 gave their ORDER BY. A column reaching here
				// would have no reference to compare against at all, so
				// fail loud rather than silently skip it.
				t.Fatalf("mmOrderByReference: %s has no engine-side reference ordering", col.Name)
			}

			mbAssertTypes(t, scalar.ColumnMetas, mmOutputType(col.Type), "lo", "hi")
			mbAssertEqual(t, "scalar lo", scalar.Rows[0]["lo"], mmWiden(t, col.Type, wantLo))
			mbAssertEqual(t, "scalar hi", scalar.Rows[0]["hi"], mmWiden(t, col.Type, wantHi))

			// The grouped path. Every group's MIN must also be the first row
			// of that group under ORDER BY, and the whole-table extremes must
			// be the extremes OF the group extremes — which is what catches a
			// path that answers NULL for one type while the scalar form
			// answers correctly (grouped DECIMAL did exactly that).
			grouped, err := db.Query(ctx, fmt.Sprintf(
				"SELECT g, MIN(%s) AS lo, MAX(%s) AS hi FROM mbtypes GROUP BY g ORDER BY g",
				col.Name, col.Name))
			if err != nil {
				t.Fatalf("grouped MIN/MAX: %v", err)
			}
			mbAssertTypes(t, grouped.ColumnMetas, mmOutputType(col.Type), "lo", "hi")
			for _, r := range grouped.Rows {
				if r["g"] == nil {
					continue
				}
				gLo, gHi, gOK := mmOrderByReference(t, db, col.Name, fmt.Sprintf("g = %v", r["g"]))
				if !gOK {
					continue
				}
				mbAssertEqual(t, fmt.Sprintf("group %v lo", r["g"]), r["lo"], mmWiden(t, col.Type, gLo))
				mbAssertEqual(t, fmt.Sprintf("group %v hi", r["g"]), r["hi"], mmWiden(t, col.Type, gHi))
			}
		})
	}
}

// TestMinMaxByteBackedLiteralValues pins the three types #417 names to values
// derived from the fixture generator rather than from another engine path, so
// an aggregate and a comparator that agreed on the same wrong answer would
// still fail. mbData writes c_ipv6 = "2001:db8::<hex i>", c_uuid =
// "00000000-0000-4000-8000-<hex i, 12 digits>" and c_cidr =
// "192.168.<i%256>.0/24" over 5000 rows.
func TestMinMaxByteBackedLiteralValues(t *testing.T) {
	ctx := context.Background()
	db := mbOpen(t)
	cases := []struct {
		col    string
		lo, hi any
	}{
		// IPV6 and UUID order by their RAW 16 bytes, so the extremes are
		// row 0 and row 4999 — not the text extremes, where "2001:db8::fff"
		// would beat "2001:db8::1387".
		{"c_ipv6", "2001:db8::", "2001:db8::1387"},
		{"c_uuid", "00000000-0000-4000-8000-000000000000", "00000000-0000-4000-8000-000000001387"},
		// CIDR's storage IS its text, so it orders as text: "99" > "255".
		{"c_cidr", "192.168.0.0/24", "192.168.99.0/24"},
	}
	for _, tc := range cases {
		t.Run(tc.col, func(t *testing.T) {
			res, err := db.Query(ctx, fmt.Sprintf(
				"SELECT MIN(%s) AS lo, MAX(%s) AS hi FROM mbtypes", tc.col, tc.col))
			if err != nil {
				t.Fatal(err)
			}
			mbAssertEqual(t, tc.col+" lo", res.Rows[0]["lo"], tc.lo)
			mbAssertEqual(t, tc.col+" hi", res.Rows[0]["hi"], tc.hi)
		})
	}
}

// mmOrderByReference reads the first and last non-NULL value of a column under
// the engine's own ORDER BY. ok=false means the type has no ordering the
// aggregate can consume.
func mmOrderByReference(t *testing.T, db *DB, col string, where ...string) (lo, hi any, ok bool) {
	t.Helper()
	ctx := context.Background()
	pred := col + " IS NOT NULL"
	for _, w := range where {
		pred += " AND " + w
	}
	read := func(dir string) any {
		res, err := db.Query(ctx, fmt.Sprintf(
			"SELECT %s AS v FROM mbtypes WHERE %s ORDER BY %s %s LIMIT 1", col, pred, col, dir))
		if err != nil {
			t.Fatalf("reference query (%s %s): %v", col, dir, err)
		}
		if len(res.Rows) == 0 {
			return nil
		}
		return res.Rows[0]["v"]
	}
	return read("ASC"), read("DESC"), true
}

// mmOutputType is the declared output type of MIN/MAX over a column of the
// given type: its own, except that the int32-class widens to INT64 and
// DECIMAL keeps the FLOAT64 its accumulator finalizes to.
func mmOutputType(in parquet.TypeID) parquet.TypeID {
	switch in {
	case parquet.TypeInt32:
		return parquet.TypeInt64
	case parquet.TypeFloat32:
		return parquet.TypeFloat64
	case parquet.TypeDecimal:
		return parquet.TypeFloat64
	}
	return in
}

// mmWiden converts a projected value into the shape MIN/MAX declares for its
// column, for the three types whose aggregate output is not the input's own:
// the int32 class widens to int64, FLOAT32 to float64, and DECIMAL finalizes
// through ToFloat64 where the projection renders its text. Nothing else is
// converted, so a genuine type change still fails.
func mmWiden(t *testing.T, in parquet.TypeID, v any) any {
	t.Helper()
	if v == nil {
		return nil
	}
	switch in {
	case parquet.TypeInt32:
		n, ok := v.(int32)
		if !ok {
			t.Fatalf("projection of an INT32 column boxed %T", v)
		}
		return int64(n)
	case parquet.TypeFloat32:
		f, ok := v.(float32)
		if !ok {
			t.Fatalf("projection of a FLOAT32 column boxed %T", v)
		}
		return float64(f)
	case parquet.TypeDecimal:
		s, ok := v.(string)
		if !ok {
			t.Fatalf("projection of a DECIMAL column boxed %T", v)
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			t.Fatalf("DECIMAL projection %q does not parse: %v", s, err)
		}
		return f
	}
	return v
}
