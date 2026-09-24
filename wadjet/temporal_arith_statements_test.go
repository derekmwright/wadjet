// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestTemporalArithmeticRefusedInEveryStatement is arc VL round 4's operator
// × statement gate (round-3 review B3). The `+` / `-` pairs PostgreSQL 17.11
// has no operator for once a side is a DATE or a TIMESTAMP are 42883 in EVERY
// statement kind and clause that evaluates an expression — the SELECT list,
// WHERE, GROUP BY, HAVING, ORDER BY, a window's ORDER BY, INSERT … VALUES,
// INSERT … SELECT, UPDATE SET, UPDATE WHERE, DELETE WHERE, MERGE's SET, WHEN
// and INSERT VALUES, CTAS — and nothing is written; the pairs that have a
// meaning answer everywhere. Round 3 refused them in the SELECT binder only:
// `INSERT … VALUES (TIMESTAMP '…' + 0)` stored 1.772496e+12 and `UPDATE …
// SET f = d + 1.5` stored 20516.5.
func TestTemporalArithmeticRefusedInEveryStatement(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateTable(ctx, "ar", parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "i", Type: parquet.TypeInt32, Nullable: true},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "dec", Type: parquet.TypeDecimal, Precision: 10, Scale: 2, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO ar (id, i, f, dec, s, d, ts) VALUES "+
		"(1, 5, 1.5, 2.25, 'x', '2026-03-03', '2026-03-03 10:20:30')"); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable(ctx, "ar_src", parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "i", Type: parquet.TypeInt32, Nullable: true},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "dec", Type: parquet.TypeDecimal, Precision: 10, Scale: 2, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO ar_src (id, i, f, dec, s, d, ts) VALUES "+
		"(2, 5, 1.5, 2.25, 'x', '2026-03-03', '2026-03-03 10:20:30')"); err != nil {
		t.Fatal(err)
	}
	snapshot := func() string {
		res, err := db.Query(ctx, "SELECT id, i, f, s, CAST(d AS TEXT) AS d FROM ar ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprint(res.Rows)
	}
	start := snapshot()
	// PostgreSQL 17.11: every refused pair is `operator does not exist`, 42883.
	refused := []string{
		"ts + 0", "ts - 1", "0 + ts", "ts + i", "ts - i", "ts + 1.5", "ts + f",
		"d + 1.5", "d - 1.5", "d + f", "d + dec", "1 - d", "i - d",
		"d + ts", "ts + d", "d - ts", "ts - d",
		"now() + 1", "CURRENT_TIMESTAMP - 1", "CURRENT_DATE + 1.0",
		"(d + 1) + 1.5", "CAST(d AS TIMESTAMP) + 1",
	}
	// Pairs with a meaning (PostgreSQL answers them): never 42883.
	kept := []string{
		"d + 1", "d - 1", "1 + d", "d + i", "d - d", "ts - ts",
		"ts + INTERVAL '1 day'", "d + INTERVAL '1 day'", "d - INTERVAL '1 hour'",
	}
	konst := strings.NewReplacer("ts", "TIMESTAMP '2026-03-03 10:20:30'", "dec", "CAST(2.25 AS NUMERIC(10,2))")
	colWord := regexp.MustCompile(`\b(i|f|d)\b`)
	constOf := func(e string) string {
		e = konst.Replace(e)
		return colWord.ReplaceAllStringFunc(e, func(w string) string {
			return map[string]string{"i": "5", "f": "CAST(1.5 AS DOUBLE PRECISION)", "d": "DATE '2026-03-03'"}[w]
		})
	}
	qual := regexp.MustCompile(`\b(i|f|dec|d|ts)\b`)
	statements := func(e, tgt string) [][2]string {
		x := qual.ReplaceAllString(e, "x.$1")
		return [][2]string{
			{"select", "SELECT " + e + " AS v FROM ar"},
			{"where", "SELECT id FROM ar WHERE " + e + " IS NOT NULL"},
			{"group_by", "SELECT COUNT(*) AS n FROM ar GROUP BY " + e},
			{"having", "SELECT COUNT(*) AS n FROM ar GROUP BY id HAVING MAX(" + e + ") IS NOT NULL"},
			{"order_by", "SELECT id FROM ar ORDER BY " + e},
			{"window_order", "SELECT id, COUNT(*) OVER (ORDER BY " + e + ") AS n FROM ar"},
			{"values", fmt.Sprintf("INSERT INTO ar (id, %s) VALUES (2, %s)", tgt, constOf(e))},
			{"insert_select", fmt.Sprintf("INSERT INTO ar (id, %s) SELECT 2, %s FROM ar", tgt, e)},
			{"update_set", fmt.Sprintf("UPDATE ar SET %s = %s", tgt, e)},
			{"update_where", "UPDATE ar SET i = 6 WHERE " + e + " IS NOT NULL"},
			{"delete_where", "DELETE FROM ar WHERE " + e + " IS NULL"},
			{"merge_set", fmt.Sprintf("MERGE INTO ar USING ar x ON ar.id = x.id WHEN MATCHED THEN UPDATE SET %s = %s", tgt, x)},
			{"merge_when", "MERGE INTO ar USING ar x ON ar.id = x.id WHEN MATCHED AND " + x + " IS NOT NULL THEN UPDATE SET i = 6"},
			{"merge_insert", fmt.Sprintf("MERGE INTO ar USING ar_src x ON ar.id = x.id WHEN NOT MATCHED THEN INSERT (id, %s) VALUES (3, %s)", tgt, x)},
			{"ctas", "CREATE TABLE ar_c AS SELECT " + e + " AS v FROM ar"},
		}
	}
	run := func(sql string) error {
		if strings.HasPrefix(sql, "SELECT") {
			_, err := db.Query(ctx, sql)
			return err
		}
		_, err := db.Execute(ctx, sql)
		return err
	}
	cells := 0
	for _, e := range refused {
		for _, st := range statements(e, "f") {
			cells++
			err := run(st[1])
			if got := sqlerr.StateOf(err); got != "42883" {
				t.Errorf("%s / %s: got %q (%v), PostgreSQL 17.11 42883\n  %s", e, st[0], got, err, st[1])
			}
			if now := snapshot(); now != start {
				t.Fatalf("%s / %s wrote: %s (was %s)", e, st[0], now, start)
			}
			if st[0] == "ctas" && err == nil {
				_, _ = db.Execute(ctx, "DROP TABLE ar_c")
			}
		}
	}
	for _, e := range kept {
		for _, st := range statements(e, "s") {
			cells++
			err := run(st[1])
			if sqlerr.StateOf(err) == "42883" {
				t.Errorf("%s / %s: refused 42883, PostgreSQL answers it\n  %s", e, st[0], st[1])
			}
			if st[0] == "ctas" && err == nil {
				_, _ = db.Execute(ctx, "DROP TABLE ar_c")
			}
			// Restore the one-row fixture for the next cell.
			if _, err := db.Execute(ctx, "DELETE FROM ar"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Execute(ctx, "INSERT INTO ar (id, i, f, dec, s, d, ts) VALUES "+
				"(1, 5, 1.5, 2.25, 'x', '2026-03-03', '2026-03-03 10:20:30')"); err != nil {
				t.Fatal(err)
			}
		}
	}
	t.Logf("%d operator × statement cells", cells)
}
