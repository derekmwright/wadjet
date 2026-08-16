package clickbench

import (
	"os"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestHitsParquetCompat reads a REAL ClickBench hits part (athena_
// partitioned/hits_N.parquet) end to end through wadjet's parquet reader —
// schema mapping, every column, every row group. The file is not vendored
// (~122MB); point WADJET_HITS_PART at a downloaded part to run:
//
//	curl -sLO https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_0.parquet
//	WADJET_HITS_PART=./hits_0.parquet go test -run TestHitsParquetCompat ./benchmarks/clickbench/
//
// Skips when unset so CI stays hermetic.
func TestHitsParquetCompat(t *testing.T) {
	path := os.Getenv("WADJET_HITS_PART")
	if path == "" {
		t.Skip("WADJET_HITS_PART not set")
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	r, err := parquet.NewReader(f, st.Size())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	sch := r.Schema()
	t.Logf("columns=%d row_groups=%d rows=%d", len(sch.Columns), r.NumRowGroups(), r.NumRows())
	if len(sch.Columns) < 100 {
		t.Fatalf("expected ~105 columns, got %d", len(sch.Columns))
	}
	rows, err := r.ReadRows(nil) // every column, every row group
	if err != nil {
		t.Fatalf("full read: %v", err)
	}
	if int64(len(rows)) != r.NumRows() {
		t.Fatalf("read %d rows, footer says %d", len(rows), r.NumRows())
	}
	// Spot sanity: a few known columns must exist and be non-degenerate.
	nonNull := 0
	for _, col := range []string{"UserID", "EventDate", "URL", "CounterID"} {
		if _, ok := rows[0][col]; !ok {
			t.Errorf("column %s missing from decoded rows", col)
			continue
		}
		for _, row := range rows[:1000] {
			if row[col] != nil {
				nonNull++
				break
			}
		}
	}
	if nonNull == 0 {
		t.Fatal("all probed columns decoded as null")
	}
}
