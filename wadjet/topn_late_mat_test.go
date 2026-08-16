package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Top-N late materialization (ClickBench Q24 shape): SELECT * over a wide
// table with a selective filter and ORDER BY ... LIMIT runs the top-N over
// only filter+sort columns, then refetches the winners' full rows. This
// test builds a 24-column table across several ingested files, computes
// the expected winners directly, and verifies every payload column of
// every returned row — a shifted/permuted/mis-refetched row cannot pass.
// Engagement is asserted via physical.TopNLateMatPlanned so the test fails
// loudly if the rewrite silently stops engaging.
func TestTopNLateMaterialization(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const payloadCols = 22
	cols := []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "grp", Type: parquet.TypeString},
	}
	for i := 0; i < payloadCols; i++ {
		if i%2 == 0 {
			cols = append(cols, parquet.Column{Name: fmt.Sprintf("p%02d", i), Type: parquet.TypeInt64})
		} else {
			cols = append(cols, parquet.Column{Name: fmt.Sprintf("p%02d", i), Type: parquet.TypeString})
		}
	}
	schema := parquet.Schema{Columns: cols}
	if err := db.CreateTable(ctx, "wide", schema, nil); err != nil {
		t.Fatal(err)
	}

	// 3 files, 400 rows each. k is a permutation so the sort order is
	// unique; grp selects ~1/3 of rows; every payload column is a pure
	// function of k so refetched rows are fully checkable.
	const nRows, nFiles = 1200, 3
	ing := db.NewIngester("wide", schema, nil, ingest.Config{MaxBufferRows: nRows / nFiles})
	for f := 0; f < nFiles; f++ {
		rows := make([]map[string]any, nRows/nFiles)
		for i := range rows {
			global := f*(nRows/nFiles) + i
			k := int64((global * 731) % nRows) // permutation of 0..1199 (gcd(731,1200)=1)
			r := map[string]any{"k": k, "grp": fmt.Sprintf("g%d", k%3)}
			for c := 0; c < payloadCols; c++ {
				if c%2 == 0 {
					r[fmt.Sprintf("p%02d", c)] = k*100 + int64(c)
				} else {
					r[fmt.Sprintf("p%02d", c)] = fmt.Sprintf("v-%d-%d", k, c)
				}
			}
			rows[i] = r
		}
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}

	checkRow := func(tb testing.TB, row map[string]any, wantK int64) {
		tb.Helper()
		if row["k"] != wantK {
			tb.Fatalf("k: got %v, want %d", row["k"], wantK)
		}
		if got := row["grp"]; got != fmt.Sprintf("g%d", wantK%3) {
			tb.Fatalf("k=%d grp: got %v", wantK, got)
		}
		for c := 0; c < payloadCols; c++ {
			name := fmt.Sprintf("p%02d", c)
			if c%2 == 0 {
				if row[name] != wantK*100+int64(c) {
					tb.Fatalf("k=%d %s: got %v, want %d", wantK, name, row[name], wantK*100+int64(c))
				}
			} else {
				if row[name] != fmt.Sprintf("v-%d-%d", wantK, c) {
					tb.Fatalf("k=%d %s: got %v", wantK, name, row[name])
				}
			}
		}
	}

	before := physical.TopNLateMatPlanned.Load()

	// Filtered ascending: winners are the 7 smallest k with k%3==1.
	res, err := db.Query(ctx, "SELECT * FROM wide WHERE grp = 'g1' ORDER BY k LIMIT 7")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 7 {
		t.Fatalf("got %d rows, want 7", len(res.Rows))
	}
	for i, row := range res.Rows {
		checkRow(t, row, int64(1+3*i)) // 1, 4, 7, ...
	}

	// Unfiltered descending: winners are the 5 largest k.
	res, err = db.Query(ctx, "SELECT * FROM wide ORDER BY k DESC LIMIT 5")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 5 {
		t.Fatalf("got %d rows, want 5", len(res.Rows))
	}
	for i, row := range res.Rows {
		checkRow(t, row, int64(nRows-1-i))
	}

	// OFFSET rides the same rewrite (LIMIT+OFFSET kept rows, Limit op skips).
	res, err = db.Query(ctx, "SELECT * FROM wide ORDER BY k LIMIT 3 OFFSET 10")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 3 {
		t.Fatalf("offset: got %d rows, want 3", len(res.Rows))
	}
	for i, row := range res.Rows {
		checkRow(t, row, int64(10+i))
	}

	if got := physical.TopNLateMatPlanned.Load() - before; got != 3 {
		t.Fatalf("TopNLateMatPlanned: %d engagements, want 3", got)
	}

	// Dormancy: a narrow projection must NOT engage (nothing to save).
	before = physical.TopNLateMatPlanned.Load()
	res, err = db.Query(ctx, "SELECT k FROM wide ORDER BY k LIMIT 3")
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Rows) != 3 || res.Rows[0]["k"] != int64(0) {
		t.Fatalf("narrow topN: %v", res.Rows)
	}
	if got := physical.TopNLateMatPlanned.Load() - before; got != 0 {
		t.Fatalf("narrow projection engaged late-mat (%d)", got)
	}
}
