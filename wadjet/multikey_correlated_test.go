package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/multikey"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// #562 — a correlated subquery that correlates on MORE THAN ONE column.
//
// `EXISTS (SELECT 1 FROM b WHERE b.s = a.s AND b.n = a.n)` answered ZERO rows;
// its NOT EXISTS twin answered EVERY row; each single-column form and the
// equivalent explicit join were correct. dedupSemiAntiBuildSide narrows a
// semi/anti join's build side to Project(keys) → Distinct, and it read those
// keys out of the condition TEXT with a split on " and " while a decorrelation
// renders " AND " — so it kept the first conjunct's key and projected away the
// column the second conjunct compares.
//
// Every expectation here is PostgreSQL 17's answer over the same fixture, and
// internal/oracle/multikey's own test is where that is re-checked against a
// live server. An agreement between wadjet's two execution paths could not
// have caught this: they share the logical optimizer, so a planner defect
// reaches both identically.
func TestMultiKeyCorrelatedSubqueries(t *testing.T) {
	ctx := context.Background()
	db := mkOpen(t)
	for _, c := range multikey.Corpus() {
		t.Run(c.Name, func(t *testing.T) {
			res, err := tmRun(ctx, db, c.SQL)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, c.SQL)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("%d rows, want 1\n  SQL: %s", len(res.Rows), c.SQL)
			}
			got := mkCount(t, res.Rows[0]["n"])
			if c.KnownBug != "" {
				if got == c.Want {
					t.Errorf("this entry now AGREES with PostgreSQL (%d), so %s is FIXED:\n  %s\n"+
						"Delete its pin in internal/oracle/multikey so the entry is gated again.",
						got, c.Issue, c.KnownBug)
					return
				}
				t.Logf("known divergence, tracked in %s — NOT gated:\n  %s\n  got %d, want %d\n  SQL: %s",
					c.Issue, c.KnownBug, got, c.Want, c.SQL)
				return
			}
			if got != c.Want {
				t.Errorf("got %d, want %d (live PostgreSQL 17, %d correlated key(s))\n  SQL: %s",
					got, c.Want, c.Keys, c.SQL)
			}
		})
	}
}

// mkOpen loads the multi-key fixture into an embedded DB.
func mkOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	for _, tbl := range multikey.Tables() {
		if err := db.CreateTable(ctx, tbl.Name, tbl.Schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.Name, err)
		}
		ing := db.NewIngester(tbl.Name, tbl.Schema, nil, ingest.Config{
			MaxBufferRows: len(tbl.Rows) + 1, RowGroupSize: 16,
		})
		if err := ing.Ingest(ctx, tbl.Rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.Name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.Name, err)
		}
	}
	return db
}

func mkCount(t *testing.T, v any) int64 {
	t.Helper()
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case float64:
		return int64(x)
	}
	t.Fatalf("count came back as %#v (%T)", v, v)
	return 0
}
