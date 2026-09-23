// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// THE TOP-N-PER-GROUP IDIOM OVER 100 000 OUTER ROWS × 10 INNER EACH FINISHES
// WITHIN A STATED BOUND, on the single arm and on the spilled512k arm — arc
// LT's performance cell (#1019, ADR-0021 §1s).
//
// The bound travels with the correlation key as a per-key ROW_NUMBER over the
// inner relation, so the cost is ONE pass over the 1 000 000 inner rows plus
// the join — measured 168 ms single / 1.08 s spilled512k at the tip on the
// author's host. The bounds below are twenty times that, so the cell fails
// on a per-outer-row blow-up (a rerun costs 10.9 ms per outer row over this
// inner relation, 1 100 s here) and never on a slow host. The row count and
// the sum are asserted too: the fixture is deterministic, and a fast wrong
// answer is the failure mode the idiom had.
func TestArcLTTopNPerGroupOver100kOuterRowsIsBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: ingests 1.1M rows")
	}
	ctx := context.Background()
	for _, arm := range []struct {
		name   string
		budget int64
		bound  time.Duration
	}{
		{"single", 0, 5 * time.Second},
		{"spilled512k", 512 * 1024, 30 * time.Second},
	} {
		t.Run(arm.name, func(t *testing.T) {
			cfg := Config{Store: objstore.NewMemStore(), Bucket: "test"}
			if arm.budget > 0 {
				cfg.MemoryBudget = arm.budget
				cfg.SpillDir = t.TempDir()
			}
			db, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ltIngestTopNFixture(t, ctx, db)
			for _, c := range []struct{ name, sql, want string }{
				// top-3 by v: per key the three largest of ten values.
				{"top3", `SELECT count(*) AS n, sum(s.v) AS sv FROM big_o o JOIN LATERAL (SELECT i.v FROM big_i i WHERE i.k = o.k ORDER BY i.v DESC, i.id LIMIT 3) s ON true`,
					"300000,16391517"},
				{"top1 LEFT", `SELECT count(*) AS n, sum(s.v) AS sv FROM big_o o LEFT JOIN LATERAL (SELECT i.v FROM big_i i WHERE i.k = o.k ORDER BY i.v DESC, i.id LIMIT 1) s ON true`,
					"100000,5653536"},
			} {
				start := time.Now()
				got, err := ltArcRun(ctx, db, c.sql)
				el := time.Since(start)
				if err != nil {
					t.Fatalf("%s: %v", c.name, err)
				}
				if got != c.want {
					t.Fatalf("%s\n  got  %s\n  want %s", c.sql, got, c.want)
				}
				if el > arm.bound {
					t.Fatalf("%s took %s on %s, bound %s: the bound is not per key any more", c.name, el, arm.name, arm.bound)
				}
				t.Logf("%s on %s: %s", c.name, arm.name, el)
			}
		})
	}
}

// ltIngestTopNFixture writes big_o (100k rows, k = id) and big_i (1M rows,
// ten per key, v = id mod 97) through the ingester, so the spilled arm can
// hold them too.
func ltIngestTopNFixture(t *testing.T, ctx context.Context, db *DB) {
	t.Helper()
	i64 := func(n string) parquet.Column { return parquet.Column{Name: n, Type: parquet.TypeInt64} }
	oSchema := parquet.Schema{Columns: []parquet.Column{i64("id"), i64("k"), i64("total")}}
	iSchema := parquet.Schema{Columns: []parquet.Column{i64("id"), i64("k"), i64("v")}}
	for _, tb := range []struct {
		name   string
		schema parquet.Schema
		n      int64
		row    func(x int64) map[string]any
	}{
		{"big_o", oSchema, 100000, func(x int64) map[string]any { return map[string]any{"id": x, "k": x, "total": x % 100} }},
		{"big_i", iSchema, 1000000, func(x int64) map[string]any { return map[string]any{"id": x, "k": (x-1)/10 + 1, "v": x % 97} }},
	} {
		if err := db.CreateTable(ctx, tb.name, tb.schema, nil); err != nil {
			t.Fatal(err)
		}
		ing := db.NewIngester(tb.name, tb.schema, nil, ingest.Config{MaxBufferRows: 100001, RowGroupSize: 8192})
		rows := make([]map[string]any, 0, 100000)
		for x := int64(1); x <= tb.n; x++ {
			rows = append(rows, tb.row(x))
			if len(rows) == 100000 {
				if err := ing.Ingest(ctx, rows); err != nil {
					t.Fatal(err)
				}
				rows = rows[:0]
			}
		}
		if len(rows) > 0 {
			if err := ing.Ingest(ctx, rows); err != nil {
				t.Fatal(err)
			}
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}
	_ = os.Getenv
}
