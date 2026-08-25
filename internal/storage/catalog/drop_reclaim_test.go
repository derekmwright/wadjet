package catalog

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
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

// hookKV wraps a MetaKV and fires onGet AFTER the underlying value has been
// read but BEFORE it is returned to the caller. That models the ordinary
// interleaving of a concurrent writer landing between the moment a flush
// reads the catalog state and the moment it acts on it — the read observes
// the world as it was, the writer's change is invisible to it, and the
// caller proceeds on the stale picture.
//
// It deliberately does NOT implement RevisionReader, so manifest reads take
// the kv.Get path and stay observable here.
type hookKV struct {
	MetaKV
	armed bool
	onGet func()
}

func (h *hookKV) Get(key string) ([]byte, uint64, error) {
	v, rev, err := h.MetaKV.Get(key)
	if h.armed && h.onGet != nil {
		h.armed = false // one-shot, and re-entrant calls from onGet see it disarmed
		h.onGet()
	}
	return v, rev, err
}

// TestReclaimReVerifiesBeforeDeletingAPathThatBecameLiveMidFlush is a
// permanent #494 regression, promoted from the second adversarial review's
// reproducer.
//
// The live-manifest guard used to be a time-of-check/time-of-use check: the
// live set was built ONCE, up front, and the delete loop never re-checked.
// A re-registration that landed after the set was built and before the
// Delete fired was invisible to the guard, and the live table's file was
// deleted. The one-shot Get hook lands a re-creation at exactly that
// instant; in production it is the ordinary interleaving of a 5-minute
// background sweep against any process that re-registers, including
// iceberg.CatalogIntegration.RefreshTable, whose drop/re-register window
// spans an S3 metadata read.
//
// Note which layer this pins. The re-created table's file arrives through
// AddFiles (registration), so it is unowned and could never have been
// scheduled — but the PENDING entry under test was written by AddNewFiles,
// so the deletion here is one ownership does not stop. The per-entry
// re-observation is what stops it.
func TestReclaimReVerifiesBeforeDeletingAPathThatBecameLiveMidFlush(t *testing.T) {
	store := objstore.NewMemStore()
	kv := &hookKV{MetaKV: NewMemKV()}
	cat := New(kv, store, "test-bucket")
	ctx := context.Background()
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	schema := testSchema()

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	path := "tables/events/chunk_0198ff00-0000-7000-8000-000000000001.parquet"
	if _, err := store.Put(ctx, "test-bucket", path, bytes.NewReader([]byte("real-user-data")), 14, "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []FileEntry{
		{Path: path, SizeBytes: 14, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	// The operator drops the table. Its exact paths are scheduled.
	if err := cat.DropTable(ctx, "events"); err != nil {
		t.Fatal(err)
	}

	// The loader re-runs (#278's idempotent re-registration) at the worst
	// possible instant: after the sweep has read the catalog's table list
	// and before the sweep deletes.
	kv.onGet = func() {
		if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
			t.Errorf("recreate: %v", err)
			return
		}
		if err := cat.AddFiles(ctx, "events", nil, "tables/events", []FileEntry{
			{Path: path, SizeBytes: 14, NumRows: 1, CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Errorf("re-register: %v", err)
		}
	}
	kv.armed = true

	if n := cat.FlushDroppedTableFiles(ctx, -time.Minute); n != 0 {
		t.Errorf("flush deleted %d objects that became live mid-flush, want 0", n)
	}

	// The table is LIVE and its manifest references the path.
	man, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatalf("live table's manifest: %v", err)
	}
	refs := 0
	for _, p := range man.Partitions {
		refs += len(p.Files)
	}
	if refs != 1 {
		t.Fatalf("precondition: live manifest should reference 1 file, got %d", refs)
	}
	if _, _, err := store.Get(ctx, "test-bucket", path); err != nil {
		t.Errorf("DATA LOSS: the LIVE table's manifest references %q but the flush deleted it: %v", path, err)
	}
}

// TestReclaimStillCollectsTheOldIncarnationAfterADropRecreateInsert is the
// coverage half of the guards: they must block the losses without blocking
// the feature. DROP a table whose files the engine wrote, recreate the same
// name, INSERT fresh (UUIDv7-named) files into it — the OLD incarnation's
// owned paths are dead and must be reclaimed, the new ones must not be
// touched.
//
// This is the case the mid-flush "reborn" rule must NOT swallow: the name is
// live in the flush's very first observation, so nothing changed under us,
// and the recreated table's files are protected precisely — by path.
func TestReclaimStillCollectsTheOldIncarnationAfterADropRecreateInsert(t *testing.T) {
	cat, ctx := setupCatalog(t)
	schema := testSchema()

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	oldPath := "tables/events/chunk_0198ff00-0000-7000-8000-00000000000a.parquet"
	if _, err := cat.Store().Put(ctx, cat.Bucket(), oldPath, bytes.NewReader([]byte("v1")), 2, "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []FileEntry{
		{Path: oldPath, SizeBytes: 2, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}
	if err := cat.DropTable(ctx, "events"); err != nil {
		t.Fatal(err)
	}

	// Same name back, brand-new files: every engine-minted path carries a
	// full UUIDv7, so the new incarnation shares nothing with the old.
	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	newPath := "tables/events/chunk_0198ff00-0000-7000-8000-00000000000b.parquet"
	if _, err := cat.Store().Put(ctx, cat.Bucket(), newPath, bytes.NewReader([]byte("v2")), 2, "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []FileEntry{
		{Path: newPath, SizeBytes: 2, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	if n := cat.FlushDroppedTableFiles(ctx, -time.Minute); n != 1 {
		t.Errorf("reclaim collected %d files, want exactly the dropped incarnation's 1", n)
	}
	if _, _, err := cat.Store().Get(ctx, cat.Bucket(), oldPath); err == nil {
		t.Errorf("the dropped incarnation's owned file %q should have been reclaimed", oldPath)
	}
	if _, _, err := cat.Store().Get(ctx, cat.Bucket(), newPath); err != nil {
		t.Errorf("DATA LOSS: the live table's file %q was reclaimed: %v", newPath, err)
	}
	man, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(man.Partitions) != 1 || len(man.Partitions[0].Files) != 1 || man.Partitions[0].Files[0].Path != newPath {
		t.Fatalf("live manifest should still reference exactly %q, got %+v", newPath, man.Partitions)
	}
}

// deleteFailingStore fails Delete for the named keys, so the reclaim
// sweep's delete-error path can be exercised without an S3 outage.
type deleteFailingStore struct {
	objstore.Store
	fail map[string]bool
}

func (d *deleteFailingStore) Delete(ctx context.Context, bucket, key string) error {
	if d.fail[key] {
		return errors.New("injected: delete failed")
	}
	return d.Store.Delete(ctx, bucket, key)
}

// captureLogs redirects the default slog logger into a buffer for the
// duration of the test and returns an accessor for what was written.
func captureLogs(t *testing.T) func() string {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return buf.String
}

// TestReclaimLogsEverySkipClass pins reclaim's observability. Every branch
// that declines to delete is an operator signal — most of all the
// live-manifest one, which means something re-registered a path a dropped
// table also owned — and a bare `continue` makes "the sweep protected your
// data" indistinguishable from "the sweep did nothing".
func TestReclaimLogsEverySkipClass(t *testing.T) {
	logs := captureLogs(t)

	base := objstore.NewMemStore()
	failing := &deleteFailingStore{Store: base, fail: map[string]bool{}}
	cat := New(NewMemKV(), failing, "test-bucket")
	ctx := context.Background()
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	schema := testSchema()

	put := func(key string) {
		t.Helper()
		if _, err := failing.Put(ctx, "test-bucket", key, bytes.NewReader([]byte("x")), 1, "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
	}

	// events owns four engine-written files, each of which will be skipped
	// (or fail) for a different reason.
	livePath := "tables/events/chunk_live.parquet"
	prefixPath := "warehouse/events/chunk_foreign.parquet"
	modifiedPath := "tables/events/chunk_modified.parquet"
	failPath := "tables/events/chunk_undeletable.parquet"
	for _, p := range []string{livePath, prefixPath, modifiedPath, failPath} {
		put(p)
	}
	failing.fail[failPath] = true

	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	entries := []FileEntry{}
	for _, p := range []string{livePath, prefixPath, modifiedPath, failPath} {
		entries = append(entries, FileEntry{Path: p, SizeBytes: 1, NumRows: 1, CreatedAt: time.Now().UTC()})
	}
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", entries); err != nil {
		t.Fatal(err)
	}

	// A second, surviving table registers one of those paths, so it is
	// live at flush time.
	if err := cat.CreateTable(ctx, "keeper", schema, nil); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddFiles(ctx, "keeper", nil, "tables/keeper", []FileEntry{
		{Path: livePath, SizeBytes: 1, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	if err := cat.DropTable(ctx, "events"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	put(modifiedPath) // rewritten after the drop was recorded

	if n := cat.FlushDroppedTableFiles(ctx, 0); n != 0 {
		t.Fatalf("every path should have been skipped or failed, got %d deleted", n)
	}

	out := logs()
	for _, want := range []string{
		"path is still referenced by a live table's manifest",
		"path falls outside its own table's prefix",
		"object was written after the drop was recorded",
		"reclaim failed to delete a dropped table's file",
		"dropped-table reclaim finished with skips",
		"skipped_live=1",
		"skipped_prefix=1",
		"skipped_modified=1",
		"delete_errors=1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("reclaim logs are missing %q\n--- logs ---\n%s", want, out)
		}
	}
}

// TestReclaimLogsTheRecreatedTableSkip covers the one skip class the test
// above cannot stage without a mid-flush writer: the dropped name coming
// back while the sweep runs.
func TestReclaimLogsTheRecreatedTableSkip(t *testing.T) {
	logs := captureLogs(t)

	store := objstore.NewMemStore()
	kv := &hookKV{MetaKV: NewMemKV()}
	cat := New(kv, store, "test-bucket")
	ctx := context.Background()
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	schema := testSchema()
	if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}
	path := "tables/events/chunk_0198ff00-0000-7000-8000-00000000000c.parquet"
	if _, err := store.Put(ctx, "test-bucket", path, bytes.NewReader([]byte("d")), 1, "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []FileEntry{
		{Path: path, SizeBytes: 1, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}
	if err := cat.DropTable(ctx, "events"); err != nil {
		t.Fatal(err)
	}
	kv.onGet = func() {
		if err := cat.CreateTable(ctx, "events", schema, nil); err != nil {
			t.Errorf("recreate: %v", err)
		}
	}
	kv.armed = true

	if n := cat.FlushDroppedTableFiles(ctx, -time.Minute); n != 0 {
		t.Fatalf("expected the re-created name to block the entry, got %d deleted", n)
	}
	out := logs()
	for _, want := range []string{
		"the dropped table's name was re-created while this sweep was running",
		"skipped_recreated_table=1",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("reclaim logs are missing %q\n--- logs ---\n%s", want, out)
		}
	}
}
