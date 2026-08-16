package clickbench

import (
	"encoding/json"
	"hash/crc32"
	"os"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestHitsBitExact compares wadjet's decode of a real hits part against
// pyarrow, column by column, using position-weighted checksums (the exact
// signature definition lives in gen_hits_sig.py — both sides implement it).
// A shifted, permuted, value-corrupted, or mis-nulled decode cannot match.
//
//	python3 gen_hits_sig.py hits_0.parquet hits_0.sig.json
//	WADJET_HITS_PART=hits_0.parquet WADJET_HITS_SIG=hits_0.sig.json \
//	  go test -run TestHitsBitExact ./benchmarks/clickbench/ -v
//
// Skips when either env var is unset so CI stays hermetic.
func TestHitsBitExact(t *testing.T) {
	path := os.Getenv("WADJET_HITS_PART")
	sigPath := os.Getenv("WADJET_HITS_SIG")
	if path == "" || sigPath == "" {
		t.Skip("WADJET_HITS_PART / WADJET_HITS_SIG not set")
	}

	var want struct {
		Rows    int64 `json:"rows"`
		Columns map[string]struct {
			Weighted uint64 `json:"weighted"`
			Nonnull  int64  `json:"nonnull"`
			Nullpos  uint64 `json:"nullpos"`
		} `json:"columns"`
	}
	sigBytes, err := os.ReadFile(sigPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(sigBytes, &want); err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}
	if r.NumRows() != want.Rows {
		t.Fatalf("row count: wadjet %d, pyarrow %d", r.NumRows(), want.Rows)
	}

	sch := r.Schema()
	if len(sch.Columns) != len(want.Columns) {
		t.Fatalf("column count: wadjet %d, pyarrow %d", len(sch.Columns), len(want.Columns))
	}

	for _, col := range sch.Columns {
		w, ok := want.Columns[col.Name]
		if !ok {
			t.Errorf("col %s: not in pyarrow signature file", col.Name)
			continue
		}
		rows, err := r.ReadRows([]string{col.Name})
		if err != nil {
			t.Errorf("col %s: read: %v", col.Name, err)
			continue
		}
		var weighted, nullpos uint64
		var nonnull int64
		for i, row := range rows {
			v, present := row[col.Name]
			if !present || v == nil {
				nullpos += uint64(i + 1)
				continue
			}
			nonnull++
			switch x := v.(type) {
			case int64:
				weighted += uint64(i+1) * uint64(x)
			case []byte:
				weighted += uint64(i+1) * uint64(crc32.ChecksumIEEE(x))
			case string:
				weighted += uint64(i+1) * uint64(crc32.ChecksumIEEE([]byte(x)))
			default:
				t.Errorf("col %s row %d: unhandled type %T", col.Name, i, v)
			}
		}
		if weighted != w.Weighted || nonnull != w.Nonnull || nullpos != w.Nullpos {
			t.Errorf("col %s MISMATCH: wadjet{weighted:%d nonnull:%d nullpos:%d} pyarrow{%d %d %d}",
				col.Name, weighted, nonnull, nullpos, w.Weighted, w.Nonnull, w.Nullpos)
		}
	}
}
