// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ARC TC (#1512) — A TEXT POSTGRESQL REFUSES AS A DATE OR TIMESTAMP, COMPARED
// WITH ONE, RAISES; IT NEVER MATCHES THE EPOCH.
//
// At 93e4804e the TIMESTAMP comparison read a refused text as 0, which is
// 1970-01-01 00:00:00, a value the column holds: `DELETE FROM dm WHERE ts =
// 'garbage'` removed the epoch row where PostgreSQL 17.11 raises 22007 and
// removes nothing. Every want below is PostgreSQL 17.11's, measured over the
// same rows (tc_author/pg_dml.txt, pg_push.txt).

func tcOpenFixture(t *testing.T, ctx context.Context, table string, rows []map[string]any, perFile, perGroup int) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	sch := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
	}}
	if err := db.CreateTable(ctx, table, sch, nil); err != nil {
		t.Fatal(err)
	}
	// One flush per perFile rows: several FILES, each of several ROW GROUPS,
	// so a statement crosses every pruning level the scan has.
	for lo := 0; lo < len(rows); lo += perFile {
		hi := min(lo+perFile, len(rows))
		ing := db.NewIngester(table, sch, nil, ingest.Config{MaxBufferRows: perFile + 1, RowGroupSize: perGroup})
		if err := ing.Ingest(ctx, rows[lo:hi]); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return db
}

func tcMs(s string) int64 {
	tm, err := time.Parse("2006-01-02 15:04:05", s)
	if err != nil {
		panic(err)
	}
	return tm.UnixMilli()
}

func tcDMLRows() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "ts": tcMs("1970-01-01 00:00:00"), "d": "1970-01-01"},
		{"id": int64(2), "ts": tcMs("2024-01-15 10:30:00"), "d": "2024-01-15"},
		{"id": int64(3), "ts": nil, "d": nil},
		{"id": int64(4), "ts": tcMs("2000-02-29 00:00:00"), "d": "2000-02-29"},
	}
}

func tcDump(t *testing.T, ctx context.Context, db *DB, table string) string {
	t.Helper()
	res, err := db.Query(ctx, "SELECT id, ts, d FROM "+table+" ORDER BY id")
	if err != nil {
		t.Fatalf("reading %s: %v", table, err)
	}
	var out []string
	for i := range res.Rows {
		out = append(out, fmt.Sprint(res.Cells(i)))
	}
	return strings.Join(out, " ")
}

// TestArcTCDMLWhereARefusedTemporalTextChangesNothing: DELETE / UPDATE whose
// WHERE compares a TIMESTAMP or DATE with a text PostgreSQL refuses raises
// that text's SQLSTATE and leaves the table as it was — the epoch row
// included.
func TestArcTCDMLWhereARefusedTemporalTextChangesNothing(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct{ sql, state, msg string }{
		{`DELETE FROM dm WHERE ts = 'garbage'`, "22007", `invalid input syntax for type timestamp: "garbage"`},
		{`DELETE FROM dm WHERE ts = ''`, "22007", `invalid input syntax for type timestamp: ""`},
		{`DELETE FROM dm WHERE ts = '2024-02-30'`, "22008", `date/time field value out of range: "2024-02-30"`},
		{`DELETE FROM dm WHERE ts = '2024-01-15 10:30:00+16'`, "22009", `time zone displacement out of range: "2024-01-15 10:30:00+16"`},
		{`DELETE FROM dm WHERE ts = '0000-01-01'`, "22008", `date/time field value out of range: "0000-01-01"`},
		{`DELETE FROM dm WHERE d = '0000-01-01'`, "22008", `date/time field value out of range: "0000-01-01"`},
		{`DELETE FROM dm WHERE d = 'garbage'`, "22007", `invalid input syntax for type date: "garbage"`},
		{`DELETE FROM dm WHERE d = '2024-01-15 10:30:00+16'`, "22009", `time zone displacement out of range: "2024-01-15 10:30:00+16"`},
		{`DELETE FROM dm WHERE id > 100 AND ts = 'garbage'`, "22007", `invalid input syntax for type timestamp: "garbage"`},
		{`DELETE FROM dm WHERE ts IN ('2000-02-29', 'garbage')`, "22007", `invalid input syntax for type timestamp: "garbage"`},
		{`DELETE FROM dm WHERE ts <> 'garbage'`, "22007", `invalid input syntax for type timestamp: "garbage"`},
		{`UPDATE dm SET id = id + 100 WHERE ts = 'garbage'`, "22007", `invalid input syntax for type timestamp: "garbage"`},
		{`UPDATE dm SET id = id + 100 WHERE d = '0000-01-01'`, "22008", `date/time field value out of range: "0000-01-01"`},
		{`UPDATE dm SET id = id + 100 WHERE d < 'garbage'`, "22007", `invalid input syntax for type date: "garbage"`},
		// Kept (ADR-0012 temporal r25): PostgreSQL reads a BC date and
		// answers DELETE 0; the grammar refuses it 22007, as the CAST does.
		{`DELETE FROM dm WHERE ts = '2024-01-15 BC'`, "22007", `invalid input syntax for type timestamp: "2024-01-15 BC"`},
	} {
		t.Run(c.sql, func(t *testing.T) {
			db := tcOpenFixture(t, ctx, "dm", tcDMLRows(), 2, 1)
			before := tcDump(t, ctx, db, "dm")
			res, err := db.Execute(ctx, c.sql)
			if err == nil {
				t.Fatalf("answered %s %d where PostgreSQL 17.11 raises %s; dm is now %s",
					res.Command, res.RowsAffected, c.state, tcDump(t, ctx, db, "dm"))
			}
			if st := sqlerr.StateOf(err); st != c.state {
				t.Errorf("SQLSTATE %q, want %q: %v", st, c.state, err)
			}
			if !strings.Contains(err.Error(), c.msg) {
				t.Errorf("message\n  got  %v\n  want PostgreSQL 17.11's %q", err, c.msg)
			}
			if after := tcDump(t, ctx, db, "dm"); after != before {
				t.Errorf("the refused statement changed dm:\n  %s\n  %s", before, after)
			}
		})
	}
	// The valid epoch still deletes exactly the epoch row.
	db := tcOpenFixture(t, ctx, "dm", tcDMLRows(), 2, 1)
	res, err := db.Execute(ctx, `DELETE FROM dm WHERE ts = '1970-01-01 00:00:00'`)
	if err != nil || res.RowsAffected != 1 {
		t.Fatalf("DELETE of the epoch: %v %v", res, err)
	}
	if got, want := tcDump(t, ctx, db, "dm"), "[2 1705314600000 2024-01-15] [3 <nil> <nil>] [4 951782400000 2000-02-29]"; got != want {
		t.Errorf("after the epoch DELETE\n  got  %s\n  want %s", got, want)
	}
}

// TestArcTCPrunedScanStillRaises: the refusal does not wait for a row. A
// conjunct whose valid literal lets the scan skip every file and row group
// by their statistics, a key no row holds, and an empty table: PostgreSQL
// coerces the literal while it analyses the statement, so each raises.
// Before #1512 the DATE refusal was a per-batch raise that a pruned scan
// never reached, and '0000-01-01' read by time.Parse as year zero let the
// scan's row predicate drop every row before the kernel could refuse it.
func TestArcTCPrunedScanStillRaises(t *testing.T) {
	ctx := context.Background()
	var rows []map[string]any
	for g := 0; g < 12; g++ {
		day := time.Unix(int64(g)*86400, 0).UTC()
		rows = append(rows, map[string]any{"id": int64(g), "ts": day.UnixMilli(), "d": day.Format("2006-01-02")})
	}
	db := tcOpenFixture(t, ctx, "pd", rows, 4, 2)
	empty := tcOpenFixture(t, ctx, "pe", nil, 1, 1)
	for _, c := range []struct {
		db         *DB
		sql, state string
	}{
		{db, `SELECT count(*) FROM pd WHERE ts = 'garbage'`, "22007"},
		{db, `SELECT count(*) FROM pd WHERE ts > '2100-01-01' AND ts = 'garbage'`, "22007"},
		{db, `SELECT count(*) FROM pd WHERE id > 1000 AND ts = '2024-01-15 10:30:00+16'`, "22009"},
		{db, `SELECT count(*) FROM pd WHERE ts < '1960-01-01' AND ts = '0000-01-01'`, "22008"},
		{db, `SELECT count(*) FROM pd WHERE d > '2100-01-01' AND d = '0000-01-01'`, "22008"},
		{db, `SELECT count(*) FROM pd WHERE id > 1000 AND d = 'garbage'`, "22007"},
		{db, `SELECT count(*) FROM pd WHERE d < '1960-01-01' AND d = '2024-02-30'`, "22008"},
		{db, `SELECT count(*) FROM pd WHERE ts > '2100-01-01'`, "0"},
		{db, `SELECT count(*) FROM pd WHERE ts = '1970-01-01'`, "1"},
		{db, `SELECT count(*) FROM pd WHERE d = '1970-01-12'`, "1"},
		{empty, `SELECT count(*) FROM pe WHERE ts = 'garbage'`, "22007"},
		{empty, `SELECT count(*) FROM pe WHERE d = '0000-01-01'`, "22008"},
	} {
		res, err := c.db.Query(ctx, c.sql)
		if len(c.state) == 5 {
			if err == nil {
				t.Errorf("%s: answered %v where PostgreSQL 17.11 raises %s", c.sql, res.Cells(0), c.state)
			} else if st := sqlerr.StateOf(err); st != c.state {
				t.Errorf("%s: SQLSTATE %q, want %q: %v", c.sql, st, c.state, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", c.sql, err)
			continue
		}
		if got := fmt.Sprint(res.Cells(0)[0]); got != c.state {
			t.Errorf("%s: answered %s, want %s", c.sql, got, c.state)
		}
	}
}

// TestArcTCDMLSpecialWordsAnswerAsPostgres: PostgreSQL's special date/time
// words in a DML WHERE (#1512 round 2) — 'epoch' read by the grammar,
// 'now' / 'today' resolved and '±infinity' folded where the comparison meets
// a DATE / TIMESTAMP column. Each want is PostgreSQL 17.11's over the same
// four rows (tc_author/r2/pg_dml2.txt); NULL row 3 stays.
func TestArcTCDMLSpecialWordsAnswerAsPostgres(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		sql  string
		n    int64
		left string
	}{
		{`DELETE FROM dm WHERE ts < 'infinity'`, 3, "3"},
		{`DELETE FROM dm WHERE ts = 'now'`, 0, "1,2,3,4"},
		{`DELETE FROM dm WHERE ts = 'epoch'`, 1, "2,3,4"},
		{`DELETE FROM dm WHERE d > '-infinity'`, 3, "3"},
		{`DELETE FROM dm WHERE ts NOT IN ('infinity')`, 3, "3"},
		{`DELETE FROM dm WHERE d < 'today'`, 3, "3"},
		{`UPDATE dm SET id = id + 100 WHERE ts = 'infinity'`, 0, "1,2,3,4"},
		{`UPDATE dm SET id = id + 100 WHERE ts BETWEEN '-infinity' AND 'epoch'`, 1, "2,3,4,101"},
	} {
		t.Run(c.sql, func(t *testing.T) {
			db := tcOpenFixture(t, ctx, "dm", tcDMLRows(), 2, 1)
			res, err := db.Execute(ctx, c.sql)
			if err != nil {
				t.Fatalf("%v (PostgreSQL 17.11: %d rows)", err, c.n)
			}
			if res.RowsAffected != c.n {
				t.Errorf("%s %d, want %d", res.Command, res.RowsAffected, c.n)
			}
			q, err := db.Query(ctx, "SELECT id FROM dm ORDER BY id")
			if err != nil {
				t.Fatal(err)
			}
			var ids []string
			for i := range q.Rows {
				ids = append(ids, fmt.Sprint(q.Cells(i)[0]))
			}
			if got := strings.Join(ids, ","); got != c.left {
				t.Errorf("rows left %s, want %s", got, c.left)
			}
		})
	}
}
