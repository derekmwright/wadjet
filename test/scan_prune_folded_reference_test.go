package test

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// spCamelFixture registers a table whose catalog schema carries the CamelCase
// column names a parquet dataset gives it — `hits` is the standing example:
// `WatchID`, `UserAgent`. Every other fixture that exercises the scan's prune
// spells its columns lower case, which is precisely why none of them could
// see the defect this gate exists for.
//
// The rows are laid out so the prune has something to do: `UserAgent` is one
// value for the first half and another for the second, at three rows per row
// group, so a point filter on it leaves two of the four groups holding no
// candidate value and each chunk is pure-dictionary.
func spCamelFixture(tb testing.TB) (*wadjet.DB, context.Context) {
	tb.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	tb.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "WatchID", Type: parquet.TypeInt64},
		{Name: "UserAgent", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "hits", schema, nil); err != nil {
		tb.Fatalf("create hits: %v", err)
	}
	rows := make([]map[string]any, 0, 12)
	for i := 1; i <= 12; i++ {
		ua := "agent-A"
		if i > 6 {
			ua = "agent-B"
		}
		rows = append(rows, map[string]any{"WatchID": int64(i), "UserAgent": ua})
	}
	ing := db.NewIngester("hits", schema, nil, ingest.Config{MaxBufferRows: 1000, RowGroupSize: 3})
	if err := ing.Ingest(ctx, rows); err != nil {
		tb.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		tb.Fatalf("flush: %v", err)
	}
	return db, ctx
}

// spCount runs a single-value count and reports how much dictionary-probe
// work the scan did while answering it.
//
// The probe counters are the observable for the WHOLE hunk, not just the
// dictionary half: the row-group statistics predicate and the dictionary
// probe are built side by side from ONE column lookup, so a reference that
// fails to resolve withholds both, and a probe that was BUILT proves the
// lookup landed. Nothing else about a prune is globally observable — the
// range-prune counters live on the row-group iterator — and a prune that
// merely fails to fire answers exactly what a full scan answers, which is
// why this defect was silent.
func spCount(tb testing.TB, db *wadjet.DB, ctx context.Context, sql string) (int64, int64) {
	tb.Helper()
	pruned0, missed0 := scan.DictPruneStatsSnapshot()
	res, err := db.Query(ctx, sql)
	if err != nil {
		tb.Fatalf("query %q: %v", sql, err)
	}
	pruned1, missed1 := scan.DictPruneStatsSnapshot()
	if len(res.Rows) != 1 {
		tb.Fatalf("query %q: want 1 row, got %d", sql, len(res.Rows))
	}
	n, ok := res.Cells(0)[0].(int64)
	if !ok {
		tb.Fatalf("query %q: count is %T, want int64", sql, res.Cells(0)[0])
	}
	return n, (pruned1 - pruned0) + (missed1 - missed0)
}

// TestScanPruneResolvesFoldedReferences is the regression gate for the scan's
// row-group prune standing down on every CamelCase schema.
//
// An unquoted identifier folds to lower case at the lexer (#731), so
// `WHERE UserAgent = 'agent-A'` reaches the physical planner as `useragent`
// while the catalog schema still says `UserAgent`. The scanner looked the
// predicate's column up byte-exactly in a map keyed by the schema's names,
// missed, and dropped the predicate on the floor — building neither a
// statistics predicate nor a dictionary probe. It did so silently, because a
// prune that does not fire returns exactly what a full scan returns: the only
// symptom is that every row group of every CamelCase table is read.
//
// Reverting the resolver to a byte-exact map lookup fails the first cell
// below: the probe count goes to zero.
func TestScanPruneResolvesFoldedReferences(t *testing.T) {
	db, ctx := spCamelFixture(t)

	prevStats := scan.StatsPrune.Set(true)
	prevDict := scan.DictPrune.Set(true)
	t.Cleanup(func() {
		scan.StatsPrune.Set(prevStats)
		scan.DictPrune.Set(prevDict)
	})

	t.Run("folded reference to a CamelCase column reaches the prune", func(t *testing.T) {
		n, probes := spCount(t, db, ctx, `SELECT COUNT(*) FROM hits WHERE UserAgent = 'agent-A'`)
		if n != 6 {
			t.Fatalf("count = %d, want 6", n)
		}
		if probes == 0 {
			t.Fatalf("the scan built no dictionary probe: a folded reference to " +
				"a CamelCase column did not resolve against the catalog schema, " +
				"so neither the statistics predicate nor the probe was pushed down")
		}
	})

	t.Run("delimited reference in the schema's own case", func(t *testing.T) {
		n, probes := spCount(t, db, ctx, `SELECT COUNT(*) FROM hits WHERE "UserAgent" = 'agent-A'`)
		if n != 6 {
			t.Fatalf("count = %d, want 6", n)
		}
		if probes == 0 {
			t.Fatalf("a byte-exact reference stopped reaching the prune")
		}
	})

	t.Run("the pruned answer is the scanned answer", func(t *testing.T) {
		for _, sql := range []string{
			`SELECT COUNT(*) FROM hits WHERE UserAgent = 'agent-A'`,
			`SELECT COUNT(*) FROM hits WHERE UserAgent = 'agent-Z'`,
			`SELECT COUNT(*) FROM hits WHERE WatchID > 1000`,
			`SELECT COUNT(*) FROM hits WHERE WatchID >= 7 AND UserAgent = 'agent-B'`,
		} {
			scan.StatsPrune.Set(true)
			scan.DictPrune.Set(true)
			on, _ := spCount(t, db, ctx, sql)
			scan.StatsPrune.Set(false)
			scan.DictPrune.Set(false)
			off, _ := spCount(t, db, ctx, sql)
			scan.StatsPrune.Set(true)
			scan.DictPrune.Set(true)
			if on != off {
				t.Errorf("%s: pruned answer %d, unpruned answer %d", sql, on, off)
			}
		}
	})
}
