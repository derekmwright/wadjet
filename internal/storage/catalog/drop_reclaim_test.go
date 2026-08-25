package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// TestReclaimNeverDeletesOperatorRegisteredFilesTheEngineNeverWrote is a
// permanent #494 regression, promoted from the second adversarial review's
// reproducer.
//
// This is the shape every benchmark and harness loader uses in production
// (cmd/tpch-bench's discoverData under --data-prefix "tables/",
// internal/harness/s3_catalog.go's primeFromS3, cmd/clickbench-bench under
// --s3-prefix "tables/hits/"): parquet objects the OPERATOR staged in the
// bucket, registered into a catalog table with AddFiles — the registration
// path, as distinct from AddNewFiles, which is the engine's own write path.
//
// Those objects sit under tables/<name>/, so the prefix layer permits them;
// once the table is dropped no live manifest references them, so the
// live-manifest guard permits them; and their mtime predates the drop, so
// the recreated-object guard permits them. Only the ownership marker stands
// between one DROP plus one grace period and an emptied bench bucket.
func TestReclaimNeverDeletesOperatorRegisteredFilesTheEngineNeverWrote(t *testing.T) {
	store := objstore.NewMemStore()
	cat := New(NewMemKV(), store, "wadjet-bench-sf100-use2")
	ctx := context.Background()
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// The operator's own dataset, staged in their bucket long before any
	// wadjet process ran, under the loaders' default "tables/" prefix.
	staged := []string{
		"tables/lineitem/lineitem_0001.parquet",
		"tables/lineitem/lineitem_0002.parquet",
	}
	for _, p := range staged {
		if _, err := store.Put(ctx, cat.Bucket(), p, bytes.NewReader([]byte("PAR1-operator-data")), 18, "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
	}

	// The loader discovers and registers them (AddFiles, the #278
	// idempotent registration path — not AddNewFiles).
	if err := cat.CreateTable(ctx, "lineitem", testSchema(), nil); err != nil {
		t.Fatal(err)
	}
	entries := make([]FileEntry, 0, len(staged))
	for _, p := range staged {
		entries = append(entries, FileEntry{Path: p, SizeBytes: 18, NumRows: 1, CreatedAt: time.Now().UTC()})
	}
	if err := cat.AddFiles(ctx, "lineitem", nil, "tables/lineitem/", entries); err != nil {
		t.Fatal(err)
	}

	// Anything drops the table: a soak/fuzz run, a two-path suite's
	// DROP+recreate, an operator resetting the catalog.
	if err := cat.DropTable(ctx, "lineitem"); err != nil {
		t.Fatal(err)
	}

	// Nothing registered is even scheduled: the ownership marker is checked
	// at snapshot time, not at delete time.
	cat.dropMu.Lock()
	scheduled := len(cat.pendingDrops)
	cat.dropMu.Unlock()
	if scheduled != 0 {
		t.Errorf("registered (never-written) files must not be scheduled for reclaim at all, got %d pending entries", scheduled)
	}

	if n := cat.FlushDroppedTableFiles(ctx, -time.Minute); n != 0 {
		t.Errorf("flush deleted %d objects the engine never wrote, want 0", n)
	}
	for _, p := range staged {
		if _, _, err := store.Get(ctx, cat.Bucket(), p); err != nil {
			t.Errorf("DATA LOSS: operator-staged object %q, registered (never written) by wadjet, was deleted: %v", p, err)
		}
	}
}

// TestOwnershipMarkerIsStampedOnlyByTheEngineWritePaths pins which call
// stamps FileEntry.EngineWritten. It is the whole ownership contract: get
// this wrong in either direction and reclaim either deletes an operator's
// data (marking AddFiles) or silently stops reclaiming anything (dropping
// the stamp from AddNewFiles).
func TestOwnershipMarkerIsStampedOnlyByTheEngineWritePaths(t *testing.T) {
	cat, ctx := setupCatalog(t)
	if err := cat.CreateTable(ctx, "events", testSchema(), nil); err != nil {
		t.Fatal(err)
	}

	registered := FileEntry{Path: "tables/events/staged_0001.parquet", SizeBytes: 4, NumRows: 1, CreatedAt: time.Now().UTC()}
	if err := cat.AddFiles(ctx, "events", nil, "tables/events", []FileEntry{registered}); err != nil {
		t.Fatal(err)
	}
	written := FileEntry{Path: "tables/events/chunk_0198ff00-0000-7000-8000-000000000001.parquet", SizeBytes: 4, NumRows: 1, CreatedAt: time.Now().UTC()}
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []FileEntry{written}); err != nil {
		t.Fatal(err)
	}

	// The caller's own FileEntry values must not have been mutated: both
	// were passed by value in a slice the catalog does not own.
	if registered.EngineWritten || written.EngineWritten {
		t.Error("the catalog stamped the caller's FileEntry in place; it must copy")
	}

	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			got[f.Path] = f.EngineWritten
		}
	}
	if got[registered.Path] {
		t.Errorf("AddFiles must not mark %q as engine-written: it registers objects somebody else staged", registered.Path)
	}
	if !got[written.Path] {
		t.Errorf("AddNewFiles must mark %q as engine-written: it is wadjet's own output", written.Path)
	}
}

// TestGCRewriteOutputIsMarkedEngineWritten covers the third engine write
// path: SwapFileForGC's rewrite_<uuid> is a file the GC sweep itself wrote,
// so a later DROP must be able to reclaim it.
func TestGCRewriteOutputIsMarkedEngineWritten(t *testing.T) {
	cat, ctx := setupCatalog(t)
	if err := cat.CreateTable(ctx, "events", testSchema(), nil); err != nil {
		t.Fatal(err)
	}
	old := FileEntry{Path: "tables/events/chunk_old.parquet", SizeBytes: 4, NumRows: 2, CreatedAt: time.Now().UTC()}
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []FileEntry{old}); err != nil {
		t.Fatal(err)
	}
	rewritten := &FileEntry{Path: "tables/events/rewrite_0198ff00-0000-7000-8000-000000000002.parquet", SizeBytes: 2, NumRows: 1, CreatedAt: time.Now().UTC()}
	if err := cat.SwapFileForGC(ctx, "events", old.Path, rewritten, nil, "tables/events", map[int64]bool{0: true}); err != nil {
		t.Fatal(err)
	}
	if rewritten.EngineWritten {
		t.Error("SwapFileForGC stamped the caller's FileEntry in place; it must copy")
	}

	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range manifest.Partitions {
		for _, f := range part.Files {
			if f.Path == rewritten.Path && !f.EngineWritten {
				t.Errorf("GC rewrite output %q must be marked engine-written", f.Path)
			}
		}
	}
}

// TestLegacyManifestWithoutTheOwnershipMarkerIsNeverReclaimed is the
// back-compat half of the marker's contract: every manifest written before
// the field existed decodes with it false, and false means "not ours". A
// binary carrying this change must not reclaim a single object out of a
// catalog an older binary populated.
//
// The manifest here is raw JSON with no engine_written key anywhere, which
// is exactly what an older wadjet wrote.
func TestLegacyManifestWithoutTheOwnershipMarkerIsNeverReclaimed(t *testing.T) {
	cat, ctx := setupCatalog(t)
	if err := cat.CreateTable(ctx, "events", testSchema(), nil); err != nil {
		t.Fatal(err)
	}

	path := "tables/events/chunk_legacy.parquet"
	if _, err := cat.Store().Put(ctx, cat.Bucket(), path, bytes.NewReader([]byte("old")), 3, "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	legacy := `{"table":"events","partitions":[{"path":"tables/events","values":null,` +
		`"files":[{"path":"` + path + `","size_bytes":3,"num_rows":1,"created_at":"2026-01-01T00:00:00Z"}]}],` +
		`"updated_at":"2026-01-01T00:00:00Z"}`
	if strings.Contains(legacy, "engine_written") {
		t.Fatal("the legacy fixture must not carry the new field")
	}
	if _, err := cat.KV().Put(cat.key("manifest.events"), []byte(legacy)); err != nil {
		t.Fatal(err)
	}
	cat.invalidateManifestCache("events")

	manifest, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.Partitions) != 1 || len(manifest.Partitions[0].Files) != 1 {
		t.Fatalf("legacy manifest did not decode as expected: %+v", manifest.Partitions)
	}
	if manifest.Partitions[0].Files[0].EngineWritten {
		t.Fatal("a manifest with no engine_written key must decode as NOT engine-written")
	}

	if err := cat.DropTable(ctx, "events"); err != nil {
		t.Fatal(err)
	}
	if n := cat.FlushDroppedTableFiles(ctx, -time.Minute); n != 0 {
		t.Errorf("flush deleted %d legacy-manifest objects, want 0", n)
	}
	if _, _, err := cat.Store().Get(ctx, cat.Bucket(), path); err != nil {
		t.Errorf("DATA LOSS: an object from a pre-marker manifest was reclaimed: %v", err)
	}
}

// TestOwnershipMarkerSurvivesTheManifestRoundTrip pins the wire shape: the
// marker is persisted in the manifest JSON under "engine_written", and it
// is omitted entirely for an unmarked entry (omitempty), so a catalog full
// of registered files pays nothing for it.
func TestOwnershipMarkerSurvivesTheManifestRoundTrip(t *testing.T) {
	cat, ctx := setupCatalog(t)
	if err := cat.CreateTable(ctx, "events", testSchema(), nil); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []FileEntry{
		{Path: "tables/events/chunk_a.parquet", SizeBytes: 1, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddFiles(ctx, "events", nil, "tables/events", []FileEntry{
		{Path: "tables/events/staged_b.parquet", SizeBytes: 1, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	raw, _, err := cat.KV().Get(cat.key("manifest.events"))
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(raw), `"engine_written":true`); n != 1 {
		t.Errorf(`manifest JSON has %d "engine_written":true entries, want exactly 1: %s`, n, raw)
	}
	if strings.Contains(string(raw), `"engine_written":false`) {
		t.Errorf("an unmarked entry must omit the key entirely (omitempty): %s", raw)
	}

	// And it survives a decode/encode cycle through the exported type.
	var decoded PartitionManifest
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	marked := 0
	for _, part := range decoded.Partitions {
		for _, f := range part.Files {
			if f.EngineWritten {
				marked++
			}
		}
	}
	if marked != 1 {
		t.Errorf("decoded manifest has %d engine-written entries, want 1", marked)
	}
}
