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

// temporalRangeForms are the spellings that construct a DATE or TIMESTAMP
// past PostgreSQL's range (DATE 4714-11-24 BC … 5874897-12-31, TIMESTAMP …
// 294276-12-31): arithmetic, an INTERVAL shift, a cast from a number, a date,
// a text and a timestamp, and the date_add family. col is the target column a
// write door assigns the form to; konst is a spelling with no column
// reference, for the VALUES door. PostgreSQL 17.11 answers every one 22008
// (`date out of range`, `timestamp out of range`, `date out of range for
// timestamp`, or — for the 300-million-year interval, which its interval type
// cannot hold — `interval out of range`, also 22008). date_add is wadjet's own
// (no PostgreSQL spelling); its range is the operator's.
var temporalRangeForms = []struct {
	name, col, expr, konst string
}{
	{"date_plus_int", "d", "d + i", "DATE '2026-03-03' + 2147483647"},
	{"date_minus_int", "d", "d - i", "DATE '2026-03-03' - 2147483647"},
	{"int_plus_date", "d", "i + d", "2147483647 + DATE '2026-03-03'"},
	{"date_minus_5e6", "d", "d - 5000000", "DATE '2026-03-03' - 5000000"},
	{"date_last_plus_1", "d", "CAST('5874897-12-31' AS DATE) + 1", "DATE '5874897-12-31' + 1"},
	{"date_add", "d", "date_add(d, i)", "date_add(DATE '2026-03-03', 2147483647)"},
	{"date_sub", "d", "date_sub(d, i)", "date_sub(DATE '2026-03-03', 2147483647)"},
	{"cast_int_date", "d", "CAST(i AS DATE)", "CAST(2147483647 AS DATE)"},
	{"cast_text_date", "d", "CAST('5874898-01-01' AS DATE)", "CAST('5874898-01-01' AS DATE)"},
	{"ts_plus_interval", "ts", "ts + INTERVAL '300000000 years'", "TIMESTAMP '2026-03-03 00:00:00' + INTERVAL '300000000 years'"},
	{"ts_minus_interval", "ts", "ts - INTERVAL '300000000 years'", "TIMESTAMP '2026-03-03 00:00:00' - INTERVAL '300000000 years'"},
	{"date_plus_interval", "ts", "d + INTERVAL '200000000 days'", "DATE '2026-03-03' + INTERVAL '200000000 days'"},
	{"cast_date_ts", "ts", "CAST(CAST('5874897-12-31' AS DATE) AS TIMESTAMP)", "CAST(DATE '5874897-12-31' AS TIMESTAMP)"},
	{"date_into_ts", "ts", "CAST('5874897-12-31' AS DATE)", "DATE '5874897-12-31'"},
}

// TestTemporalConstructionRefusesPastPostgreSQLRange is arc VL round 4's range
// gate (round-3 review B1): every form above, on every door — SELECT, WHERE,
// INSERT … VALUES, INSERT … SELECT, UPDATE … SET, UPDATE … WHERE, DELETE …
// WHERE, MERGE, CTAS — is 22008, and nothing is stored: the table reads back
// the rows it started with. At base the write doors stored the int32-narrowed
// day (`-5877585-08-22`) and the wrapped millisecond count
// (`-284552024-11-30`), and SELECT answered `-5877585-08-24` for `d - i`.
func TestTemporalConstructionRefusesPastPostgreSQLRange(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateTable(ctx, "rg", parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "i", Type: parquet.TypeInt32, Nullable: true},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO rg (id, i, d, ts) VALUES "+
		"(1, 2147483647, '2026-03-03', '2026-03-03 10:20:30')"); err != nil {
		t.Fatal(err)
	}
	const want = "1 2147483647 2026-03-03 2026-03-03 10:20:30"
	snapshot := func() string {
		res, err := db.Query(ctx, "SELECT id, i, CAST(d AS TEXT) AS d, CAST(ts AS TEXT) AS ts FROM rg ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		var rows []string
		for _, r := range res.Rows {
			rows = append(rows, fmt.Sprint(r["id"], " ", r["i"], " ", r["d"], " ", r["ts"]))
		}
		return strings.Join(rows, "; ")
	}
	cells := 0
	for _, f := range temporalRangeForms {
		// A DATE past TIMESTAMP's range is itself a valid DATE: only its
		// ASSIGNMENT to a TIMESTAMP column is out of range.
		assignOnly := f.name == "date_into_ts"
		for _, door := range []struct{ name, sql string }{
			{"select", "SELECT " + f.expr + " AS v FROM rg"},
			{"select_const", "SELECT " + f.konst + " AS v"},
			{"where", "SELECT id FROM rg WHERE " + f.expr + " IS NOT NULL"},
			{"values", fmt.Sprintf("INSERT INTO rg (id, %s) VALUES (2, %s)", f.col, f.konst)},
			{"insert_select", fmt.Sprintf("INSERT INTO rg (id, %s) SELECT 2, %s FROM rg", f.col, f.expr)},
			{"update_set", fmt.Sprintf("UPDATE rg SET %s = %s", f.col, f.expr)},
			{"update_where", "UPDATE rg SET i = 0 WHERE " + f.expr + " IS NOT NULL"},
			{"delete_where", "DELETE FROM rg WHERE " + f.expr + " IS NOT NULL"},
			{"merge", fmt.Sprintf("MERGE INTO rg USING rg x ON rg.id = x.id WHEN MATCHED THEN UPDATE SET %s = %s",
				f.col, qualifyRangeCols(f.expr))},
			{"ctas", "CREATE TABLE rg_ctas AS SELECT " + f.expr + " AS v FROM rg"},
		} {
			if assignOnly && door.name != "values" && door.name != "insert_select" &&
				door.name != "update_set" && door.name != "merge" {
				continue
			}
			cells++
			var err error
			if strings.HasPrefix(door.sql, "SELECT") {
				_, err = db.Query(ctx, door.sql)
			} else {
				_, err = db.Execute(ctx, door.sql)
			}
			if got := sqlerr.StateOf(err); got != "22008" {
				t.Errorf("%s/%s: %s\n  got SQLSTATE %q (%v), PostgreSQL 17.11 22008", f.name, door.name, door.sql, got, err)
			}
			if got := snapshot(); got != want {
				t.Fatalf("%s/%s: the table changed to %q (want %q) — an out-of-range value was stored", f.name, door.name, got, want)
			}
			if door.name == "ctas" {
				if _, gerr := db.Catalog().GetTable(ctx, "rg_ctas"); gerr == nil {
					t.Errorf("%s/ctas: the table was created", f.name)
					_, _ = db.Execute(ctx, "DROP TABLE rg_ctas")
				}
			}
		}
	}
	t.Logf("%d form × door cells, every one 22008 with nothing stored", cells)
}

// qualifyRangeCols respells a form's column references against MERGE's source
// alias.
func qualifyRangeCols(e string) string {
	return regexp.MustCompile(`\b(d|i|ts)\b`).ReplaceAllString(e, "x.$1")
}
