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

// TestQuotedTemporalOperandIsResolvedInEveryStatement is arc VL round 5's
// quoted axis of the operator × statement gate (round-4 review B3). A quoted
// literal (or NULL) beside a DATE or TIMESTAMP in `+` / `-` is typed by
// PostgreSQL's operator resolution (expr.ResolveUnknownTemporal): `date +
// unknown` is 42725, and the literal of `date - '…'`, `ts - '…'` and `ts +
// '…'` is read as a DATE, a TIMESTAMP and an INTERVAL — refused 22007 when
// its text is not one, in EVERY statement kind, with nothing written. Round 4
// typed the quoted side `unknown` and let it through, and the kernel read its
// leading number: `INSERT … VALUES (DATE '…' + '1.5')` stored 20516.5 and
// `(n) VALUES (TIMESTAMP '…' + '0')` stored the epoch milliseconds. The pairs
// PostgreSQL answers answer its value. PostgreSQL 17.11 SQLSTATEs and values.
func TestQuotedTemporalOperandIsResolvedInEveryStatement(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cols := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "i", Type: parquet.TypeInt32, Nullable: true},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
	}
	seed := "(1, 5, 1.5, 'x', '2026-03-03', '2026-03-03 10:20:30')"
	for _, name := range []string{"qo", "qo_src"} {
		if err := db.CreateTable(ctx, name, parquet.Schema{Columns: cols}, nil); err != nil {
			t.Fatal(err)
		}
	}
	reset := func() {
		for _, st := range []string{"DELETE FROM qo", "INSERT INTO qo (id, i, f, s, d, ts) VALUES " + seed} {
			if _, err := db.Execute(ctx, st); err != nil {
				t.Fatal(err)
			}
		}
	}
	reset()
	if _, err := db.Execute(ctx, "INSERT INTO qo_src (id, i, f, s, d, ts) VALUES (2, 5, 1.5, 'x', '2026-03-03', '2026-03-03 10:20:30')"); err != nil {
		t.Fatal(err)
	}
	snapshot := func() string {
		res, err := db.Query(ctx, "SELECT id, i, f, s, CAST(d AS TEXT) AS d, CAST(ts AS TEXT) AS ts FROM qo ORDER BY id")
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprint(res.Rows)
	}
	start := snapshot()
	qual := regexp.MustCompile(`\b(d|ts)\b`)
	konst := func(e string) string {
		return qual.ReplaceAllStringFunc(e, func(w string) string {
			return map[string]string{"d": "DATE '2026-03-03'", "ts": "TIMESTAMP '2026-03-03 10:20:30'"}[w]
		})
	}
	ctasN := 0
	statements := func(e, tgt string) [][2]string {
		x := qual.ReplaceAllString(e, "x.$1")
		ctasN++
		return [][2]string{
			{"select", "SELECT " + e + " AS v FROM qo"},
			{"where", "SELECT id FROM qo WHERE " + e + " IS NOT NULL"},
			{"group_by", "SELECT COUNT(*) AS n FROM qo GROUP BY " + e},
			{"having", "SELECT COUNT(*) AS n FROM qo GROUP BY id HAVING MAX(" + e + ") IS NOT NULL"},
			{"order_by", "SELECT id FROM qo ORDER BY " + e},
			{"window_order", "SELECT id, COUNT(*) OVER (ORDER BY " + e + ") AS n FROM qo"},
			{"values", fmt.Sprintf("INSERT INTO qo (id, %s) VALUES (2, %s)", tgt, konst(e))},
			{"insert_select", fmt.Sprintf("INSERT INTO qo (id, %s) SELECT 2, %s FROM qo", tgt, e)},
			{"update_set", fmt.Sprintf("UPDATE qo SET %s = %s", tgt, e)},
			{"update_where", "UPDATE qo SET i = 6 WHERE " + e + " IS NOT NULL"},
			{"delete_where", "DELETE FROM qo WHERE " + e + " IS NULL"},
			{"merge_set", fmt.Sprintf("MERGE INTO qo USING qo x ON qo.id = x.id WHEN MATCHED THEN UPDATE SET %s = %s", tgt, x)},
			{"merge_when", "MERGE INTO qo USING qo x ON qo.id = x.id WHEN MATCHED AND " + x + " IS NOT NULL THEN UPDATE SET i = 6"},
			{"merge_insert", fmt.Sprintf("MERGE INTO qo USING qo_src x ON qo.id = x.id WHEN NOT MATCHED THEN INSERT (id, %s) VALUES (3, %s)", tgt, x)},
			{"ctas", fmt.Sprintf("CREATE TABLE qo_c%d AS SELECT %s AS v FROM qo", ctasN, e)},
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
	// PostgreSQL 17.11: the refusal of each quoted pair, in every statement.
	for _, tc := range []struct{ e, state string }{
		{"d + '1'", "42725"}, {"d + '1.5'", "42725"}, {"d + '1 day'", "42725"},
		{"'1' + d", "42725"}, {"d + NULL", "42725"}, {"CURRENT_DATE + '1'", "42725"},
		{"d - '1'", "22007"}, {"'1' - d", "22007"}, {"d - 'abc'", "22007"},
		{"ts - '1 day'", "22007"}, {"ts - 'abc'", "22007"},
		{"ts + 'abc'", "22007"}, {"'abc' + ts", "22007"},
	} {
		for _, st := range statements(tc.e, "f") {
			cells++
			err := run(st[1])
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Errorf("%s / %s: got %q (%v), PostgreSQL 17.11 %s\n  %s", tc.e, st[0], got, err, tc.state, st[1])
			}
			if now := snapshot(); now != start {
				t.Fatalf("%s / %s wrote: %s (was %s)", tc.e, st[0], now, start)
			}
		}
	}
	// The pairs PostgreSQL answers: the literal read as the resolved type, in
	// every statement, never refused.
	for _, tc := range []struct{ e, tgt string }{
		{"d - '2026-03-01'", "i"}, {"'2026-03-05' - d", "i"},
		{"ts + '1 day'", "ts"}, {"'1 day' + ts", "ts"}, {"ts + '0'", "ts"},
		{"ts - '2026-03-01 10:00:00'", "f"},
	} {
		for _, st := range statements(tc.e, tc.tgt) {
			cells++
			if err := run(st[1]); err != nil {
				t.Errorf("%s / %s: %v — PostgreSQL answers it\n  %s", tc.e, st[0], err, st[1])
			}
			reset()
		}
	}
	for _, tc := range []struct{ sql, want string }{
		{"SELECT d - '2026-03-01' AS v FROM qo", "2"},
		{"SELECT '2026-03-05' - d AS v FROM qo", "2"},
		{"SELECT CAST(ts + '1 day' AS TEXT) AS v FROM qo", "2026-03-04 10:20:30"},
		{"SELECT CAST('1 day' + ts AS TEXT) AS v FROM qo", "2026-03-04 10:20:30"},
		{"SELECT CAST(ts + '0' AS TEXT) AS v FROM qo", "2026-03-03 10:20:30"},
		{"SELECT CAST(ts + '90 minutes' AS TEXT) AS v FROM qo", "2026-03-03 11:50:30"},
		// No INTERVAL type: `timestamp - timestamp` is the documented milliseconds.
		{"SELECT ts - '2026-03-01 10:00:00' AS v FROM qo", "1.7403e+08"},
		{"SELECT d - NULL AS v FROM qo", "<nil>"},
	} {
		res, err := db.Query(ctx, tc.sql)
		if err != nil || len(res.Rows) != 1 || fmt.Sprint(res.Rows[0]["v"]) != tc.want {
			t.Errorf("%s = %v (%v), want %s", tc.sql, res, err, tc.want)
		}
	}
	// The assignment reads the resolved type: a TIMESTAMP into a bigint is
	// 42804 (PostgreSQL), a day count into an integer is the count.
	for _, tc := range []struct{ sql, state string }{
		{"INSERT INTO qo (id, i) VALUES (4, TIMESTAMP '2026-03-03 00:00:00' + '0')", "42804"},
		{"INSERT INTO qo (id, i) VALUES (4, DATE '2026-03-03' - '2026-03-01')", ""},
		{"INSERT INTO qo (id, ts) VALUES (5, TIMESTAMP '2026-03-03 00:00:00' + '1 day')", ""},
		{"UPDATE qo SET ts = ts + '1 day' WHERE id = 1", ""},
	} {
		_, err := db.Execute(ctx, tc.sql)
		if got := sqlerr.StateOf(err); got != tc.state || (tc.state == "" && err != nil) {
			t.Errorf("%s: got %q (%v), PostgreSQL 17.11 %q", tc.sql, got, err, tc.state)
		}
	}
	res, err := db.Query(ctx, "SELECT id, i, CAST(ts AS TEXT) AS t FROM qo ORDER BY id")
	if err != nil || fmt.Sprint(res.Rows) != "[map[i:5 id:1 t:2026-03-04 10:20:30] map[i:2 id:4 t:<nil>] map[i:<nil> id:5 t:2026-03-04 00:00:00]]" {
		t.Errorf("stored %v (%v)", res.Rows, err)
	}
	t.Logf("%d quoted operand × statement cells", cells)
}
