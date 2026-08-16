package clickbench

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// TestSubSecondFloor reproduces the c6a run's 100-part registration shape
// locally (FileStore over symlinks to one real part) and times the
// sub-second-class queries per try. On metal the leaders answer these in
// single-digit MILLISECONDS; wadjet's floor is 130-800ms and is
// file-count-proportional — this probe is the local harness for tearing
// that floor apart (plan cost, per-file footer reads, scan setup, worker
// spin-up), with WADJET_FLOOR_SQL to time any one query under -cpuprofile.
func TestSubSecondFloor(t *testing.T) {
	part := os.Getenv("WADJET_HITS_PART")
	if part == "" {
		t.Skip("WADJET_HITS_PART not set")
	}
	nFiles := 100

	dir := t.TempDir()
	bucketDir := filepath.Join(dir, "bench")
	tblDir := filepath.Join(bucketDir, "tables", "hits")
	if err := os.MkdirAll(tblDir, 0o755); err != nil {
		t.Fatal(err)
	}
	abs, err := filepath.Abs(part)
	if err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(abs)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < nFiles; i++ {
		if err := os.Symlink(abs, filepath.Join(tblDir, fmt.Sprintf("hits_%d.parquet", i))); err != nil {
			t.Fatal(err)
		}
	}

	f, err := os.Open(abs)
	if err != nil {
		t.Fatal(err)
	}
	r, err := parquet.NewReader(f, st.Size())
	if err != nil {
		t.Fatal(err)
	}
	schema := r.Schema()
	for i := range schema.Columns {
		if schema.Columns[i].Type == parquet.TypeBytes {
			schema.Columns[i].Type = parquet.TypeString
		}
		if schema.Columns[i].Name == "EventDate" {
			schema.Columns[i].Type = parquet.TypeDate
		}
	}
	rows := r.NumRows()
	f.Close()

	store, err := objstore.NewFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "bench"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateTable(ctx, "hits", schema, nil); err != nil {
		t.Fatal(err)
	}
	entries := make([]catalog.FileEntry, nFiles)
	for i := range entries {
		entries[i] = catalog.FileEntry{
			Path:      fmt.Sprintf("tables/hits/hits_%d.parquet", i),
			SizeBytes: st.Size(),
			NumRows:   rows,
			CreatedAt: time.Now(),
		}
	}
	if err := db.Catalog().AddFiles(ctx, "hits", nil, "", entries); err != nil {
		t.Fatal(err)
	}

	queries := []struct{ name, sql string }{
		{"count-star", "SELECT COUNT(*) FROM hits"},
		{"count-where", "SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0"},
		{"three-aggs", "SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits"},
		{"point-filter", "SELECT UserID FROM hits WHERE UserID = 435090932899640449"},
	}
	if q := os.Getenv("WADJET_FLOOR_SQL"); q != "" {
		queries = []struct{ name, sql string }{{"custom", q}}
	}
	// The registered rowcount must be what COUNT(*) reports, whichever
	// path answers it (metadata fast path or full scan).
	res, err := db.Query(ctx, "SELECT COUNT(*) AS c FROM hits")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 1 || res.Rows[0]["c"] != rows*int64(nFiles) {
		t.Fatalf("COUNT(*) = %v, want %d", res.Rows, rows*int64(nFiles))
	}

	reps := 6
	for _, q := range queries {
		if _, err := db.Query(ctx, q.sql); err != nil { // warmup
			t.Fatalf("%s: %v", q.name, err)
		}
		times := make([]time.Duration, 0, reps)
		for i := 0; i < reps; i++ {
			start := time.Now()
			if _, err := db.Query(ctx, q.sql); err != nil {
				t.Fatal(err)
			}
			times = append(times, time.Since(start))
		}
		min := times[0]
		for _, d := range times {
			if d < min {
				min = d
			}
		}
		t.Logf("%-12s min %7.1fms  all=%v", q.name, float64(min.Microseconds())/1000, times)
	}
}
