package catalog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func incTestSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
}

// TestCreateTableStampsADistinctIncarnation pins that every incarnation of a
// name gets its own identity: DROP+CREATE mints a new one (#919).
func TestCreateTableStampsADistinctIncarnation(t *testing.T) {
	ctx := context.Background()
	cat := NewWithStore(objstore.NewMemStore(), "review")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateTable(ctx, "events", incTestSchema(), nil); err != nil {
		t.Fatal(err)
	}
	first, err := cat.TableIncarnation(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if first == "" {
		t.Fatal("CreateTable stamped no incarnation")
	}
	if err := cat.DropTable(ctx, "events"); err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateTable(ctx, "events", incTestSchema(), nil); err != nil {
		t.Fatal(err)
	}
	second, err := cat.TableIncarnation(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if second == "" || second == first {
		t.Fatalf("recreate must mint a distinct incarnation: first=%q second=%q", first, second)
	}
}

// TestAddNewFilesForIncarnationRefusesAChangedIncarnation is the catalog half
// of #919: a file add bound to an incarnation the manifest no longer carries is
// refused with ErrTableIncarnationChanged, and the manifest is left untouched.
func TestAddNewFilesForIncarnationRefusesAChangedIncarnation(t *testing.T) {
	ctx := context.Background()
	cat := NewWithStore(objstore.NewMemStore(), "review")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateTable(ctx, "events", incTestSchema(), nil); err != nil {
		t.Fatal(err)
	}
	bound, err := cat.TableIncarnation(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	// Drop and recreate under the same name: the manifest now carries a new
	// incarnation, so the add bound to the old one must be refused.
	if err := cat.DropTable(ctx, "events"); err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateTable(ctx, "events", incTestSchema(), nil); err != nil {
		t.Fatal(err)
	}
	entry := FileEntry{Path: "tables/events/chunk_stale.parquet", SizeBytes: 10, NumRows: 1, CreatedAt: time.Now().UTC()}
	err = cat.AddNewFilesForIncarnation(ctx, "events", bound, nil, "tables/events", []FileEntry{entry})
	if !errors.Is(err, ErrTableIncarnationChanged) {
		t.Fatalf("expected ErrTableIncarnationChanged, got %v", err)
	}
	m, err := cat.GetManifest(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range m.Partitions {
		if len(p.Files) != 0 {
			t.Fatalf("refused add still registered a file: %+v", p.Files)
		}
	}

	// The current incarnation writes normally, and an empty expectation skips
	// the guard entirely (the legacy path).
	cur, err := cat.TableIncarnation(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if err := cat.AddNewFilesForIncarnation(ctx, "events", cur, nil, "tables/events",
		[]FileEntry{{Path: "tables/events/chunk_ok.parquet", SizeBytes: 10, NumRows: 1, CreatedAt: time.Now().UTC()}}); err != nil {
		t.Fatalf("current incarnation must write: %v", err)
	}
	if err := cat.AddNewFilesForIncarnation(ctx, "events", "", nil, "tables/events",
		[]FileEntry{{Path: "tables/events/chunk_legacy.parquet", SizeBytes: 10, NumRows: 1, CreatedAt: time.Now().UTC()}}); err != nil {
		t.Fatalf("empty expectation must skip the guard: %v", err)
	}
}
