// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// scUnboundClockGuard, when set (arc_sc_statement_clock_seam_test.go),
// fails a test that compiled a clock function with no statement clock bound.
var scUnboundClockGuard func(t *testing.T)

// The statement clock on the embedded door (#1566): every row a statement
// writes or reads sees ONE now() / CURRENT_TIMESTAMP / LOCALTIMESTAMP /
// CURRENT_DATE — INSERT … SELECT, a many-row VALUES, UPDATE SET, CTAS, a
// table function's arguments — and two statements read two values.
// PostgreSQL 17.11 answers 1 distinct value for each single statement (the
// counts below are its answers, measured).

func scQuery1(t *testing.T, ctx context.Context, db *DB, sql string) string {
	t.Helper()
	res, err := db.Query(ctx, sql)
	if err != nil {
		return "ERR " + err.Error()
	}
	if len(res.Rows) != 1 {
		return fmt.Sprintf("%d rows", len(res.Rows))
	}
	var f []string
	for _, c := range res.Cells(0) {
		f = append(f, fmt.Sprint(c))
	}
	return strings.Join(f, " ")
}

func scExec(t *testing.T, ctx context.Context, db *DB, sql string) {
	t.Helper()
	var err error
	if strings.HasPrefix(sql, "CREATE TABLE") && !strings.Contains(sql, " AS ") {
		_, err = db.Query(ctx, sql)
	} else {
		_, err = db.Execute(ctx, sql)
	}
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func scOpen(t *testing.T, ctx context.Context) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// scValuesRows is a VALUES list of n rows, each carrying now().
func scValuesRows(n int) string {
	var vals []string
	for i := 1; i <= n; i++ {
		vals = append(vals, fmt.Sprintf("(%d, now())", i))
	}
	return strings.Join(vals, ", ")
}

func TestArcSCEmbeddedStatementWritesOneClock(t *testing.T) {
	if scUnboundClockGuard != nil {
		scUnboundClockGuard(t)
	}
	ctx := context.Background()
	for rep := 1; rep <= 8; rep++ {
		t.Run(fmt.Sprint(rep), func(t *testing.T) {
			db := scOpen(t, ctx)
			scExec(t, ctx, db, "CREATE TABLE sc (id BIGINT, ts TIMESTAMP)")
			scExec(t, ctx, db, "INSERT INTO sc SELECT g, now() FROM generate_series(1, 4096) g")
			if got := scQuery1(t, ctx, db, "SELECT count(DISTINCT ts), count(*) FROM sc"); got != "1 4096" {
				t.Errorf("INSERT … SELECT now() over 4096 rows: distinct, count = %s; PostgreSQL 17.11: 1 4096", got)
			}
			scExec(t, ctx, db, "UPDATE sc SET ts = now()")
			if got := scQuery1(t, ctx, db, "SELECT count(DISTINCT ts) FROM sc"); got != "1" {
				t.Errorf("UPDATE sc SET ts = now() over 4096 rows: %s distinct; PostgreSQL 17.11: 1", got)
			}
			scExec(t, ctx, db, "CREATE TABLE sv (id BIGINT, ts TIMESTAMP)")
			scExec(t, ctx, db, "INSERT INTO sv VALUES "+scValuesRows(2048))
			if got := scQuery1(t, ctx, db, "SELECT count(DISTINCT ts) FROM sv"); got != "1" {
				t.Errorf("INSERT VALUES (…, now()) × 2048: %s distinct; PostgreSQL 17.11: 1", got)
			}
			scExec(t, ctx, db, "CREATE TABLE sa AS SELECT g AS id, now() AS ts, CURRENT_TIMESTAMP AS ct FROM generate_series(1, 4096) g")
			if got := scQuery1(t, ctx, db, "SELECT count(DISTINCT ts), count(*) FROM sa WHERE ts = ct"); got != "1 4096" {
				t.Errorf("CTAS now(), CURRENT_TIMESTAMP over 4096 rows: distinct, equal rows = %s; PostgreSQL 17.11: 1 4096", got)
			}
			// A table function's argument: the series starts at the statement's
			// now() (PostgreSQL 17.11: 1 row equals it, every time). Many runs,
			// because a fold that read its own clock disagreed in a few.
			for i := 0; i < 64; i++ {
				q := "SELECT count(*) FROM generate_series(CAST(extract(epoch FROM now()) * 1000 AS BIGINT), CAST(extract(epoch FROM now()) * 1000 AS BIGINT) + 5) g WHERE g = CAST(extract(epoch FROM now()) * 1000 AS BIGINT)"
				if got := scQuery1(t, ctx, db, q); got != "1" {
					t.Errorf("generate_series(now() in ms, … + 5) WHERE g = now() in ms, run %d: %s; PostgreSQL 17.11: 1", i, got)
					break
				}
			}
			// A UDF's body: PostgreSQL's SQL function reads the statement's now()
			// too (17.11: count(DISTINCT sc_stamp(id)) over 4096 rows is 1).
			if _, err := db.Query(ctx, "CREATE OR REPLACE FUNCTION sc_stamp(x) AS now()"); err != nil {
				t.Fatalf("CREATE FUNCTION: %v", err)
			}
			if got := scQuery1(t, ctx, db, "SELECT count(DISTINCT sc_stamp(id)), count(*) FROM sc WHERE sc_stamp(id) = now()"); got != "1 4096" {
				t.Errorf("a UDF whose body is now(), over 4096 rows: distinct, rows equal to now() = %s; PostgreSQL 17.11: 1 4096", got)
			}
			if _, err := db.Query(ctx, "DROP FUNCTION sc_stamp"); err != nil {
				t.Fatalf("DROP FUNCTION: %v", err)
			}
			if got := scQuery1(t, ctx, db, "SELECT count(*) FROM sc WHERE ts < now() AND now() = now()"); got != "4096" {
				t.Errorf("a later statement's now() is after the stored one and equal to itself: %s; PostgreSQL 17.11: 4096", got)
			}
		})
	}
}

// TestArcSCEmbeddedTwoStatementsTwoClocks: the clock is the STATEMENT's — a
// second statement reads a later value (PostgreSQL 17.11 outside an explicit
// transaction: two values). Not a constant captured once per process or per
// DB.
func TestArcSCEmbeddedTwoStatementsTwoClocks(t *testing.T) {
	if scUnboundClockGuard != nil {
		scUnboundClockGuard(t)
	}
	ctx := context.Background()
	db := scOpen(t, ctx)
	scExec(t, ctx, db, "CREATE TABLE s2 (id BIGINT, ts TIMESTAMP)")
	scExec(t, ctx, db, "INSERT INTO s2 VALUES (1, now())")
	time.Sleep(5 * time.Millisecond)
	scExec(t, ctx, db, "INSERT INTO s2 SELECT 2, now()")
	if got := scQuery1(t, ctx, db, "SELECT count(DISTINCT ts) FROM s2"); got != "2" {
		t.Errorf("two statements: %s distinct; PostgreSQL 17.11: 2", got)
	}
	a := scQuery1(t, ctx, db, "SELECT CAST(now() AS TEXT)")
	time.Sleep(5 * time.Millisecond)
	if b := scQuery1(t, ctx, db, "SELECT CAST(now() AS TEXT)"); a == b {
		t.Errorf("two SELECT now() 5 ms apart answered the same %s", a)
	}
}
