package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// `AVG(col * k)` is not `AVG(col) * k` (round-1 review, B2).
//
// AVG over an integer is numeric(38,4) here — a value ROUNDED to four decimals
// — so multiplying it by k rounds BEFORE the multiply and loses the last digit
// for any k that is not a power of two. PostgreSQL itself shows the rewrite is
// not an identity, which is the cleanest possible statement of it:
//
//	SELECT avg(x*3), avg(x)*3 FROM (VALUES (1),(2),(4)) t(x);
//	  -->  7.0000000000000000 | 6.9999999999999999
//
// The lift took it anyway, because `caaLiftIsSafe`'s AVG arm bounded only the
// DIGIT COUNT — whether the result FIT — and never asked whether the rounded
// average times k equals the average of the products. `AVG(m*3)` answered
// 6.9999 for the server's 7.0000.
//
// The gate's only AVG-with-`*` cell was `AVG(c_i32 * 2)`, and multiplying a
// four-decimal value by a power of two is exact: it could not fail. The values
// below are the reviewer's — 1, 2, 4, whose average is 2.3333… — and the
// multipliers are chosen to be odd.
//
// `AVG(col ± k)` stays lifted and is exact, which is what makes these cells a
// statement about the OPERATOR rather than about AVG: rounding commutes with
// adding an integer, so round(s/n, 4) + k = round(s/n + k, 4).
func caAvgDB(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "m", Type: parquet.TypeInt32},
		{Name: "n", Type: parquet.TypeInt64},
	}}
	if err := db.CreateTable(ctx, "caavg", schema, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	ing := db.NewIngester("caavg", schema, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 8})
	if err := ing.Ingest(ctx, []map[string]any{
		{"m": int32(1), "n": int64(1)},
		{"m": int32(2), "n": int64(2)},
		{"m": int32(4), "n": int64(4)},
	}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return db
}

func TestAvgOfAProductIsNotTheProductOfTheAvg(t *testing.T) {
	ctx := context.Background()
	db := caAvgDB(t)
	for _, c := range []struct {
		name, sql, want, pg string
	}{
		// PostgreSQL 17.11 over 1, 2, 4. wadjet's AVG scale is 4 where the
		// server's is 16, so these compare to the digits both keep
		// (ADR-0012 item 9) — and 7.0000 vs 6.9999 is not one of those.
		{"avg_times_three", `SELECT AVG(m * 3) AS v FROM caavg`, "7.0000",
			"7.0000000000000000"},
		{"avg_times_seven", `SELECT AVG(m * 7) AS v FROM caavg`, "16.3333",
			"16.3333333333333333"},
		{"avg_times_three_int64", `SELECT AVG(n * 3) AS v FROM caavg`, "7.0000",
			"7.0000000000000000"},
		{"avg_times_nine", `SELECT AVG(m * 9) AS v FROM caavg`, "21.0000",
			"21.0000000000000000"},
		// `*` by a power of two IS exact, which is why the old corpus could
		// not see the defect. It is a control now.
		{"ctl_avg_times_two", `SELECT AVG(m * 2) AS v FROM caavg`, "4.6667",
			"4.6666666666666667"},
		// `±` is exact for an integer k and stays lifted.
		{"ctl_avg_plus_one", `SELECT AVG(m + 1) AS v FROM caavg`, "3.3333",
			"3.3333333333333333"},
		{"ctl_avg_minus_one", `SELECT AVG(m - 1) AS v FROM caavg`, "1.3333",
			"1.3333333333333333"},
		{"ctl_avg_bare", `SELECT AVG(m) AS v FROM caavg`, "2.3333",
			"2.3333333333333333"},
	} {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, c.sql)
			}
			got, _ := res.Rows[0]["v"].(string)
			if got != c.want {
				t.Errorf("= %q, want %q (PostgreSQL 17.11 says %s). `AVG(col*k)` is not "+
					"`AVG(col)*k`: the average is rounded to four decimals before the "+
					"multiply (round-1 review, B2)\n  SQL: %s", got, c.want, c.pg, c.sql)
			}
			// The per-row arm is the reference: the lift is an identity over
			// values or it is not applied.
			tog := caaToggle(t)
			prev := tog.Set(false)
			off, err := db.Query(ctx, c.sql)
			tog.Set(prev)
			if err != nil {
				t.Fatalf("per-row: %v", err)
			}
			if off.Rows[0]["v"] != res.Rows[0]["v"] {
				t.Errorf("the kill switch changes the answer: %v on, %v off",
					res.Rows[0]["v"], off.Rows[0]["v"])
			}
		})
	}
}
