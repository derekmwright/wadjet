package physical

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A statement's manifest and its column statistics are ONE view of ONE
// revision (#540).
//
// AggregateColumnStats fetched the manifest itself, as the first statement
// of its body, to key its memo on the revision. So a statement that had
// already pinned a manifest read the same key a second time — and a writer
// can commit between the two. Measured with a RevisionReader-masked KV (the
// NATS-equivalent path, see the countingKV shape guard below):
//
//	READ 1 (GetManifest):          files=2  rows=200
//	  <- concurrent AddFiles commits 100 more rows ->
//	READ 2 (AggregateColumnStats): TotalRows=300
//	=> statistics describing 100 rows the pinned manifest does not contain,
//	   and the tear then PINNED for the whole statement.
//
// The direction is fixed: annotateScanColumns reads the manifest first and
// the statistics second, so the statistics are always the newer half. Both
// directions are reachable, though — AddFiles makes the stats describe
// absent files, RemoveFiles and compaction make them omit present ones — so
// both are asserted here; a one-sided fix would pass only one.
//
// This is not a wrong ANSWER today: both non-test consumers are cost-model
// only. That is a property of the current code, not an invariant, which is
// the argument for closing it now rather than when an optimizer rule starts
// proving predicates unsatisfiable from these Min/Max values.
func TestPinnedStatisticsDescribeThePinnedManifest(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, ctx context.Context, cat *catalog.Catalog)
	}{
		{
			"a concurrent AddFiles must not appear in the pinned statistics",
			func(t *testing.T, ctx context.Context, cat *catalog.Catalog) {
				if err := cat.AddFiles(ctx, "orders", map[string]string{}, "tables/orders/", []catalog.FileEntry{
					{Path: "tables/orders/chunk_0002.parquet", SizeBytes: 1024, NumRows: 100,
						ColumnStats: map[string]catalog.FileColumnStats{
							"o_orderkey": {MinValue: int64(500), MaxValue: int64(600)},
						}},
				}); err != nil {
					t.Fatalf("concurrent AddFiles: %v", err)
				}
			},
		},
		{
			"a concurrent RemoveFiles must not disappear from the pinned statistics",
			func(t *testing.T, ctx context.Context, cat *catalog.Catalog) {
				if err := cat.RemoveFiles(ctx, "orders",
					[]string{"tables/orders/chunk_0001.parquet"}); err != nil {
					t.Fatalf("concurrent RemoveFiles: %v", err)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cat, _, ctx := setupStatsCatalog(t)
			snap := NewManifestSnapshot()

			// READ 1: the statement pins the manifest.
			man, err := snap.Get(ctx, cat, "orders")
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			pinnedRows := int64(0)
			for _, f := range manifestFiles(man) {
				pinnedRows += f.NumRows
			}
			if pinnedRows == 0 {
				t.Fatal("the pinned manifest has no rows — this cell would compare nothing")
			}

			// A writer commits between the statement's two reads.
			tc.mutate(t, ctx, cat)

			// READ 2: the statistics. They must describe READ 1's manifest.
			stats, err := snap.AggregateColumnStats(ctx, cat, "orders")
			if err != nil {
				t.Fatalf("AggregateColumnStats: %v", err)
			}
			st, ok := stats["o_orderkey"]
			if !ok {
				t.Fatalf("no statistics for o_orderkey; got %v", keysOf(stats))
			}
			if st.TotalRows != pinnedRows {
				t.Fatalf("the statistics describe %d rows and the PINNED manifest holds %d.\n"+
					"  A statement's manifest and its column statistics are one view of one "+
					"revision; two reads of the same key can straddle a writer (#540).",
					st.TotalRows, pinnedRows)
			}
		})
	}
}

// TestCountingKVModelsNATSByNotOfferingRevision is #829.
//
// countingKV embeds the catalog.MetaKV INTERFACE, not *catalog.MemKV, so it
// does not satisfy catalog.RevisionReader and manifestWithRevision takes the
// same path it takes against NATS — where NATSKVAdapter implements
// Get/Put/Update/Delete/List and no Revision at all. That is the correct
// model, it is load-bearing for every read-count assertion in this package,
// and it was documented nowhere: change the embed to the concrete type and
// the counts silently drop while every test keeps passing.
func TestCountingKVModelsNATSByNotOfferingRevision(t *testing.T) {
	kv := newCountingKV()
	if _, ok := any(kv).(catalog.RevisionReader); ok {
		t.Fatal("countingKV now satisfies catalog.RevisionReader, so manifestWithRevision " +
			"takes the revision-probe fast path this fixture exists NOT to take. " +
			"NATSKVAdapter has no Revision method, so a fixture that offers one models " +
			"embedded mode, not production, and every read-count assertion in this " +
			"package silently stops measuring what it claims to (#829).")
	}
}

func manifestFiles(m *catalog.PartitionManifest) []catalog.FileEntry {
	var out []catalog.FileEntry
	for _, p := range m.Partitions {
		out = append(out, p.Files...)
	}
	return out
}

func keysOf(m map[string]catalog.TableColumnStats) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// setupStatsCatalog is setupMetadataFoldCatalog with per-column statistics
// on every file, so AggregateColumnStats has something to aggregate.
func setupStatsCatalog(t *testing.T) (*catalog.Catalog, *countingKV, context.Context) {
	t.Helper()
	ctx := context.Background()
	kv := newCountingKV()
	cat := catalog.New(kv, objstore.NewMemStore(), "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "o_orderkey", Type: parquet.TypeInt64},
		{Name: "o_custkey", Type: parquet.TypeInt64},
	}}
	if err := cat.CreateTable(ctx, "orders", schema, nil); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if err := cat.AddFiles(ctx, "orders", map[string]string{}, "tables/orders/", []catalog.FileEntry{
		{Path: "tables/orders/chunk_0000.parquet", SizeBytes: 1024, NumRows: 100,
			ColumnStats: map[string]catalog.FileColumnStats{
				"o_orderkey": {MinValue: int64(1), MaxValue: int64(100)},
			}},
		{Path: "tables/orders/chunk_0001.parquet", SizeBytes: 1024, NumRows: 100,
			ColumnStats: map[string]catalog.FileColumnStats{
				"o_orderkey": {MinValue: int64(101), MaxValue: int64(200)},
			}},
	}); err != nil {
		t.Fatalf("add files: %v", err)
	}
	return cat, kv, ctx
}
