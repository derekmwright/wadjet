package catalog

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ANALYZE writes the manifest through the catalog's revision CAS, not over
// the top of it (#830).
//
// AnalyzeTable reads the manifest, then spends MINUTES decoding every file
// (1-2 minutes for SF10 lineitem, longer at SF100), then wrote back
// whatever it had read at the start with a blind Put. An AddFiles that
// committed anywhere in that window was silently overwritten and the files
// it registered vanished — and ingest flushes on a 60-second cadence by
// default, so the window is not theoretical. Every other manifest writer in
// the catalog goes through the revision CAS the storage layer is built on
// (CLAUDE.md §Storage); this was the one that did not.
//
// The assertion is the file SET after ANALYZE, and it must be the UNION:
// the files ANALYZE sampled keep their sketch keys, and the file the
// concurrent writer added is still there.
func TestAnalyzeCommitsThroughCASAgainstAConcurrentAddFiles(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	kv := NewMemKV()
	cat := New(kv, store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("init: %v", err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
	if err := cat.CreateTable(ctx, "t", schema, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := cat.AddFiles(ctx, "t", map[string]string{}, "tables/t/", []FileEntry{
		{Path: "tables/t/chunk_0000.parquet", SizeBytes: 10, NumRows: 1},
	}); err != nil {
		t.Fatalf("seed AddFiles: %v", err)
	}

	// The shape of the race, made deterministic: ANALYZE's read has already
	// happened (loadManifest at the top of AnalyzeTable), a writer commits,
	// and ANALYZE then persists. commitAnalyzed is that persist, called
	// with the manifest state ANALYZE would be holding.
	sketched := map[string]sketchedFile{
		"tables/t/chunk_0000.parquet": {
			key:   "sketches/t/chunk_0000.bin",
			stats: map[string]FileColumnStats{"id": {MinValue: int64(1), MaxValue: int64(1)}},
		},
	}

	// The concurrent writer, committing between ANALYZE's read and its write.
	if err := cat.AddFiles(ctx, "t", map[string]string{}, "tables/t/", []FileEntry{
		{Path: "tables/t/chunk_0001.parquet", SizeBytes: 10, NumRows: 1},
	}); err != nil {
		t.Fatalf("concurrent AddFiles: %v", err)
	}

	if err := cat.commitAnalyzed("t", sketched, "rgmeta/t.bin"); err != nil {
		t.Fatalf("commitAnalyzed: %v", err)
	}

	man, err := cat.GetManifest(ctx, "t")
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	paths := map[string]FileEntry{}
	for _, p := range man.Partitions {
		for _, f := range p.Files {
			paths[f.Path] = f
		}
	}
	if _, ok := paths["tables/t/chunk_0001.parquet"]; !ok {
		t.Fatalf("the file a concurrent AddFiles registered is GONE after ANALYZE; "+
			"the manifest holds %v. ANALYZE must commit through the revision CAS, "+
			"not over the top of whatever it read minutes earlier (#830).", keysOfFiles(paths))
	}
	got, ok := paths["tables/t/chunk_0000.parquet"]
	if !ok {
		t.Fatalf("ANALYZE lost the file it sampled; the manifest holds %v", keysOfFiles(paths))
	}
	if got.SketchesKey != "sketches/t/chunk_0000.bin" {
		t.Fatalf("the sampled file's sketch key is %q, want it re-attached after the "+
			"CAS re-read", got.SketchesKey)
	}
	if man.RGMetaKey != "rgmeta/t.bin" {
		t.Fatalf("RGMetaKey = %q, want it carried through the CAS re-read", man.RGMetaKey)
	}
}

// TestAnalyzeCommitSkipsFilesRemovedDuringTheScan is the other direction: a
// file that is GONE by commit time must not be resurrected by its sketch.
func TestAnalyzeCommitSkipsFilesRemovedDuringTheScan(t *testing.T) {
	ctx := context.Background()
	cat := New(NewMemKV(), objstore.NewMemStore(), "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("init: %v", err)
	}
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
	if err := cat.CreateTable(ctx, "t", schema, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := cat.AddFiles(ctx, "t", map[string]string{}, "tables/t/", []FileEntry{
		{Path: "tables/t/chunk_0000.parquet", SizeBytes: 10, NumRows: 1},
		{Path: "tables/t/chunk_0001.parquet", SizeBytes: 10, NumRows: 1},
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	sketched := map[string]sketchedFile{
		"tables/t/chunk_0000.parquet": {key: "a"},
		"tables/t/chunk_0001.parquet": {key: "b"},
	}
	if err := cat.RemoveFiles(ctx, "t", []string{"tables/t/chunk_0001.parquet"}); err != nil {
		t.Fatalf("concurrent RemoveFiles: %v", err)
	}
	if err := cat.commitAnalyzed("t", sketched, ""); err != nil {
		t.Fatalf("commitAnalyzed: %v", err)
	}
	man, err := cat.GetManifest(ctx, "t")
	if err != nil {
		t.Fatalf("GetManifest: %v", err)
	}
	for _, p := range man.Partitions {
		for _, f := range p.Files {
			if f.Path == "tables/t/chunk_0001.parquet" {
				t.Fatal("ANALYZE resurrected a file a concurrent RemoveFiles had removed")
			}
		}
	}
}

func keysOfFiles(m map[string]FileEntry) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
