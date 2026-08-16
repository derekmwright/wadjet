package clickbench

import (
	"os"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func TestHitsColumnProbe(t *testing.T) {
	path := os.Getenv("WADJET_HITS_PART")
	if path == "" {
		t.Skip("unset")
	}
	f, _ := os.Open(path)
	defer f.Close()
	st, _ := f.Stat()
	r, err := parquet.NewReader(f, st.Size())
	if err != nil {
		t.Fatal(err)
	}
	sch := r.Schema()
	bad := 0
	for _, col := range sch.Columns {
		func() {
			defer func() {
				if p := recover(); p != nil {
					bad++
					t.Logf("PANIC col=%s type=%v: %v", col.Name, col.Type, p)
				}
			}()
			if _, err := r.ReadRows([]string{col.Name}); err != nil {
				bad++
				t.Logf("ERR   col=%s type=%v: %v", col.Name, col.Type, err)
			}
		}()
	}
	t.Logf("bad columns: %d / %d", bad, len(sch.Columns))
}
