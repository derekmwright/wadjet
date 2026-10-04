// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// ARC TI — 'infinity' AND '-infinity' ARE TIMESTAMP AND DATE VALUES.
//
// The embedded door end to end: INSERT VALUES (every spelling the grammar
// reads), INSERT … SELECT over date arithmetic and casts, UPDATE, CREATE
// TABLE AS, a table PARTITIONED BY a DATE and one by a TIMESTAMP, the
// catalog closed and reopened from its data directory, and DELETE / UPDATE
// whose WHERE compares with an infinite value. Every want is PostgreSQL
// 17.11's over the same statements (ti_author/pg_dml.out, pg_dml_part.out).
// At 8e681724 the first INSERT raised 22007 (`invalid input syntax for type
// timestamp: "infinity"`) and every later statement had no infinite row to
// act on.

type tiStep struct {
	sql  string
	want string // the command tag, or the table dump after a "dump:" step
}

func tiDump(t *testing.T, ctx context.Context, db *DB, table string) string {
	t.Helper()
	res, err := db.Query(ctx, "SELECT id, CAST(ts AS TEXT), CAST(d AS TEXT) FROM "+table+" ORDER BY id")
	if err != nil {
		t.Fatalf("reading %s: %v", table, err)
	}
	var out []string
	for i := range res.Rows {
		var f []string
		for _, v := range res.Cells(i) {
			if v == nil {
				f = append(f, "")
			} else {
				f = append(f, fmt.Sprint(v))
			}
		}
		out = append(out, strings.Join(f, "|"))
	}
	return strings.Join(out, " ; ")
}

func tiRun(t *testing.T, ctx context.Context, db *DB, steps []tiStep) {
	t.Helper()
	for _, s := range steps {
		if table, ok := strings.CutPrefix(s.sql, "dump:"); ok {
			if got := tiDump(t, ctx, db, table); got != s.want {
				t.Fatalf("%s\n  got  %s\n  want %s", s.sql, got, s.want)
			}
			continue
		}
		if strings.HasPrefix(s.sql, "CREATE TABLE") && !strings.Contains(s.sql, " AS ") {
			if _, err := db.Query(ctx, s.sql); err != nil {
				t.Fatalf("%s: %v", s.sql, err)
			}
			continue
		}
		res, err := db.Execute(ctx, s.sql)
		if err != nil {
			t.Fatalf("%s: %v (PostgreSQL 17.11: %s)", s.sql, err, s.want)
		}
		if got := fmt.Sprintf("%s %d", res.Command, res.RowsAffected); s.want != "" && got != s.want {
			t.Fatalf("%s: %s, want %s", s.sql, got, s.want)
		}
	}
}

func TestArcTIInfinityStoresReopensAndAnswersDML(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := Open(ctx, Config{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	tiRun(t, ctx, db, []tiStep{
		{`CREATE TABLE e (id BIGINT, ts TIMESTAMP, d DATE)`, ""},
		{`INSERT INTO e VALUES (1, 'infinity', 'infinity'), (2, '-infinity', '-infinity'), (3, '2024-01-15 10:30:00', '2024-01-15'), (4, NULL, NULL), (5, '+infinity', ' -Infinity ')`, "INSERT 5"},
		{`INSERT INTO e SELECT id + 10, ts + INTERVAL '1 day', d + 1 FROM e WHERE id <= 3`, "INSERT 3"},
		{`INSERT INTO e (id, ts, d) SELECT 20, CAST(d AS TIMESTAMP), CAST(ts AS DATE) FROM e WHERE id = 2`, "INSERT 1"},
		{`UPDATE e SET ts = 'infinity' WHERE id = 3`, "UPDATE 1"},
		{`UPDATE e SET d = '-infinity' WHERE ts = 'infinity' AND id = 3`, "UPDATE 1"},
		{"dump:e", "1|infinity|infinity ; 2|-infinity|-infinity ; 3|infinity|-infinity ; 4|| ; 5|infinity|-infinity ; " +
			"11|infinity|infinity ; 12|-infinity|-infinity ; 13|2024-01-16 10:30:00|2024-01-16 ; 20|-infinity|-infinity"},
		{`CREATE TABLE e2 AS SELECT id, ts, d FROM e WHERE ts > '-infinity'`, ""},
		{"dump:e2", "1|infinity|infinity ; 3|infinity|-infinity ; 5|infinity|-infinity ; 11|infinity|infinity ; 13|2024-01-16 10:30:00|2024-01-16"},
		{`CREATE TABLE ep (id BIGINT, ts TIMESTAMP, d DATE) PARTITION BY (d)`, ""},
		{`INSERT INTO ep SELECT id, ts, d FROM e WHERE id IN (1, 2, 13)`, "INSERT 3"},
		{`CREATE TABLE et (id BIGINT, ts TIMESTAMP, d DATE) PARTITION BY (ts)`, ""},
		{`INSERT INTO et SELECT id, ts, d FROM e WHERE id IN (1, 2, 13)`, "INSERT 3"},
	})
	db.Close()
	db, err = Open(ctx, Config{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tiRun(t, ctx, db, []tiStep{
		{"dump:e", "1|infinity|infinity ; 2|-infinity|-infinity ; 3|infinity|-infinity ; 4|| ; 5|infinity|-infinity ; " +
			"11|infinity|infinity ; 12|-infinity|-infinity ; 13|2024-01-16 10:30:00|2024-01-16 ; 20|-infinity|-infinity"},
		{"dump:ep", "1|infinity|infinity ; 2|-infinity|-infinity ; 13|2024-01-16 10:30:00|2024-01-16"},
		{"dump:et", "1|infinity|infinity ; 2|-infinity|-infinity ; 13|2024-01-16 10:30:00|2024-01-16"},
		{`DELETE FROM ep WHERE d = 'infinity'`, "DELETE 1"},
		{"dump:ep", "2|-infinity|-infinity ; 13|2024-01-16 10:30:00|2024-01-16"},
		{`DELETE FROM et WHERE ts < 'infinity'`, "DELETE 2"},
		{"dump:et", "1|infinity|infinity"},
		{`DELETE FROM e WHERE ts < 'infinity'`, "DELETE 4"},
		{"dump:e", "1|infinity|infinity ; 3|infinity|-infinity ; 4|| ; 5|infinity|-infinity ; 11|infinity|infinity"},
		{`DELETE FROM e WHERE d = '-infinity'`, "DELETE 2"},
		{"dump:e", "1|infinity|infinity ; 4|| ; 11|infinity|infinity"},
		{`UPDATE e2 SET ts = ts + INTERVAL '1 hour', d = d - 1 WHERE id IN (1, 2, 13)`, "UPDATE 2"},
		{"dump:e2", "1|infinity|infinity ; 3|infinity|-infinity ; 5|infinity|-infinity ; 11|infinity|infinity ; 13|2024-01-16 11:30:00|2024-01-15"},
		{`DELETE FROM e2 WHERE d BETWEEN '-infinity' AND '2030-01-01'`, "DELETE 3"},
		{"dump:e2", "1|infinity|infinity ; 11|infinity|infinity"},
	})
}
