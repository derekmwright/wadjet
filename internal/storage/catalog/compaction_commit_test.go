package catalog

import (
	"context"
	"errors"
	"testing"
	"time"
)

// commitFixture is a table with two registered files and no markers.
func commitFixture(t *testing.T) (*Catalog, context.Context, []FileEntry) {
	t.Helper()
	cat, ctx := setupCatalog(t)
	if err := cat.CreateTable(ctx, "events", testSchema(), nil); err != nil {
		t.Fatal(err)
	}
	files := []FileEntry{
		{Path: "tables/events/chunk_a.parquet", SizeBytes: 10, NumRows: 2, CreatedAt: time.Now().UTC()},
		{Path: "tables/events/chunk_b.parquet", SizeBytes: 10, NumRows: 2, CreatedAt: time.Now().UTC()},
	}
	if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", files); err != nil {
		t.Fatal(err)
	}
	return cat, ctx, files
}

func commitOutput() *FileEntry {
	return &FileEntry{
		Path:      "tables/events/compacted_01a07200-0000-7000-8000-00000000000a.parquet",
		SizeBytes: 18, NumRows: 4, CreatedAt: time.Now().UTC(),
	}
}

// TestCommitCompactionPublishesInOneTransaction is the ordinary case: the
// inputs leave, their markers leave with them, the replacement arrives.
func TestCommitCompactionPublishesInOneTransaction(t *testing.T) {
	cat, ctx, files := commitFixture(t)
	if err := cat.AddDeleteMarkers(ctx, "events", []DeleteMarker{
		{FilePath: files[0].Path, RowIndices: []int64{1}, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}
	out := commitOutput()
	err := cat.CommitCompaction(ctx, CompactionCommit{
		Table: "events", PartPath: "tables/events",
		Inputs: []string{files[0].Path, files[1].Path},
		Output: out,
		AppliedDeletes: map[string]map[int64]bool{
			files[0].Path: {1: true},
			files[1].Path: nil,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := manifestFilePaths(t, cat, ctx, "events"); len(got) != 1 || got[0] != out.Path {
		t.Fatalf("expected only the replacement, got %v", got)
	}
	m, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if len(m.DeleteMarkers) != 0 {
		t.Errorf("the inputs' markers must leave with them, got %+v", m.DeleteMarkers)
	}
	// And the output is stamped engine-written, without touching the caller's
	// entry (ADR-0020 layer 0).
	if out.EngineWritten {
		t.Error("CommitCompaction stamped the caller's FileEntry in place; it must copy")
	}
	if !m.Partitions[0].Files[0].EngineWritten {
		t.Error("a compaction output is an object this engine wrote and must be marked")
	}
}

// TestCommitCompactionRefusesAnInputThatMoved is #895's rule at the catalog
// door: a replacement whose originals another writer already consumed must
// not be added beside the winner's.
func TestCommitCompactionRefusesAnInputThatMoved(t *testing.T) {
	cat, ctx, files := commitFixture(t)
	// Another writer consumed chunk_a.
	if err := cat.RemoveFiles(ctx, "events", []string{files[0].Path}); err != nil {
		t.Fatal(err)
	}
	err := cat.CommitCompaction(ctx, CompactionCommit{
		Table: "events", PartPath: "tables/events",
		Inputs: []string{files[0].Path, files[1].Path},
		Output: commitOutput(),
	})
	if !errors.Is(err, ErrCompactionInputMoved) {
		t.Fatalf("expected ErrCompactionInputMoved, got %v", err)
	}
	if got := manifestFilePaths(t, cat, ctx, "events"); len(got) != 1 || got[0] != files[1].Path {
		t.Fatalf("a refused commit must change nothing, got %v", got)
	}
}

// TestCommitCompactionRefusesAMarkerSnapshotThatAdvanced is #894's rule at the
// catalog door, in both directions: an output that misses a committed delete,
// and an output that dropped a row nobody deleted.
func TestCommitCompactionRefusesAMarkerSnapshotThatAdvanced(t *testing.T) {
	t.Run("a_delete_committed_since", func(t *testing.T) {
		cat, ctx, files := commitFixture(t)
		if err := cat.AddDeleteMarkers(ctx, "events", []DeleteMarker{
			{FilePath: files[0].Path, RowIndices: []int64{0}, CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
		// The output was cut before that DELETE: it applied nothing.
		err := cat.CommitCompaction(ctx, CompactionCommit{
			Table: "events", PartPath: "tables/events",
			Inputs: []string{files[0].Path, files[1].Path},
			Output: commitOutput(),
		})
		if !errors.Is(err, ErrCompactionDeletesAdvanced) {
			t.Fatalf("expected ErrCompactionDeletesAdvanced, got %v", err)
		}
		if got := manifestFilePaths(t, cat, ctx, "events"); len(got) != 2 {
			t.Fatalf("a refused commit must change nothing, got %v", got)
		}
	})

	t.Run("a_marker_the_manifest_does_not_have", func(t *testing.T) {
		cat, ctx, files := commitFixture(t)
		// An output that dropped a row the table still has loses data just as
		// surely as one that keeps a deleted row.
		err := cat.CommitCompaction(ctx, CompactionCommit{
			Table: "events", PartPath: "tables/events",
			Inputs:         []string{files[0].Path, files[1].Path},
			Output:         commitOutput(),
			AppliedDeletes: map[string]map[int64]bool{files[0].Path: {0: true}},
		})
		if !errors.Is(err, ErrCompactionDeletesAdvanced) {
			t.Fatalf("expected ErrCompactionDeletesAdvanced, got %v", err)
		}
	})

	t.Run("an_unrelated_write_does_not_refuse", func(t *testing.T) {
		// The predicate is exact, not conservative: a DELETE on a file this
		// commit does not consume leaves it valid.
		cat, ctx, files := commitFixture(t)
		other := FileEntry{Path: "tables/events/chunk_c.parquet", SizeBytes: 10, NumRows: 2, CreatedAt: time.Now().UTC()}
		if err := cat.AddNewFiles(ctx, "events", nil, "tables/events", []FileEntry{other}); err != nil {
			t.Fatal(err)
		}
		if err := cat.AddDeleteMarkers(ctx, "events", []DeleteMarker{
			{FilePath: other.Path, RowIndices: []int64{0}, CreatedAt: time.Now().UTC()},
		}); err != nil {
			t.Fatal(err)
		}
		if err := cat.CommitCompaction(ctx, CompactionCommit{
			Table: "events", PartPath: "tables/events",
			Inputs: []string{files[0].Path, files[1].Path},
			Output: commitOutput(),
		}); err != nil {
			t.Fatalf("an unrelated write must not refuse this commit: %v", err)
		}
		m, err := cat.GetManifest(ctx, "events")
		if err != nil {
			t.Fatal(err)
		}
		if len(m.DeleteMarkers) != 1 || m.DeleteMarkers[0].FilePath != other.Path {
			t.Errorf("the other file's marker must survive, got %+v", m.DeleteMarkers)
		}
	})
}
