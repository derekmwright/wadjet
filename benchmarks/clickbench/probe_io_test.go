package clickbench

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/wadjet"
)

func readRchar(t *testing.T) int64 {
	b, err := os.ReadFile("/proc/self/io")
	if err != nil {
		t.Fatal(err)
	}
	var rchar int64
	for _, line := range splitLines(string(b)) {
		if n, ok := cut(line, "rchar: "); ok {
			rchar = n
		}
	}
	return rchar
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return out
}

func cut(line, prefix string) (int64, bool) {
	if len(line) < len(prefix) || line[:len(prefix)] != prefix {
		return 0, false
	}
	var n int64
	for _, c := range line[len(prefix):] {
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + int64(c-'0')
	}
	return n, true
}

// openHitsDBFileStore mirrors cmd/clickbench-bench's registration: FileStore
// over a dir holding the part, probed schema with the arc type mapping.
func openHitsDBFileStore(tb testing.TB, ctx context.Context) *wadjet.DB {
	tb.Helper()
	src := os.Getenv("WADJET_HITS_PART")
	if src == "" {
		tb.Skip("WADJET_HITS_PART not set")
	}
	dir := tb.TempDir()
	bucketDir := filepath.Join(dir, "cb")
	if err := os.MkdirAll(bucketDir, 0o755); err != nil {
		tb.Fatal(err)
	}
	data, err := os.ReadFile(src)
	if err != nil {
		tb.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(bucketDir, "hits_0.parquet"), data, 0o644); err != nil {
		tb.Fatal(err)
	}
	store, err := objstore.NewFileStore(dir)
	if err != nil {
		tb.Fatal(err)
	}
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "cb"})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { db.Close() })
	pr, err := parquet.NewReaderFromBytes(data)
	if err != nil {
		tb.Fatal(err)
	}
	schema := pr.Schema()
	for i := range schema.Columns {
		if schema.Columns[i].Type == parquet.TypeBytes {
			schema.Columns[i].Type = parquet.TypeString
		}
		if schema.Columns[i].Name == "EventDate" {
			schema.Columns[i].Type = parquet.TypeDate
		}
	}
	if err := db.CreateTable(ctx, "hits", schema, nil); err != nil {
		tb.Fatal(err)
	}
	if err := db.Catalog().AddFiles(ctx, "hits", nil, "", []catalog.FileEntry{{
		Path: "hits_0.parquet", SizeBytes: int64(len(data)), NumRows: pr.NumRows(), CreatedAt: time.Now(),
	}}); err != nil {
		tb.Fatal(err)
	}
	return db
}

func TestProbeIOPerQuery(t *testing.T) {
	ctx := context.Background()
	db := openHitsDBFileStore(t, ctx)
	for _, q := range []string{
		"SELECT COUNT(*) FROM hits",
		"SELECT COUNT(*) FROM hits",
		"SELECT COUNT(*) FROM hits WHERE AdvEngineID <> 0",
		"SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits",
		"SELECT AVG(UserID) FROM hits",
	} {
		c0, b0, _ := parquet.PreadStats()
		r0 := readRchar(t)
		start := time.Now()
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		el := time.Since(start)
		c1, b1, _ := parquet.PreadStats()
		r1 := readRchar(t)
		t.Logf("%-55.55s %7.2fs pread_chunks=%-5d pread_MB=%-7.1f rchar_MB=%.1f", q, el.Seconds(), c1-c0, float64(b1-b0)/1e6, float64(r1-r0)/1e6)
	}
}
