package scan

import (
	"bytes"
	"fmt"
	"testing"

	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// BenchmarkReadRowGroupBacking is the allocation arm for the scan row-group
// backing pool (docs/design/scan-output-backing-reuse.md). Each iteration
// decodes every row group of a multi-row-group file and releases each batch
// immediately — the steady state of a fragment whose consumer keeps nothing.
// pool=false is the control (the WADJET_SCAN_BACKING_REUSE=0 shape): a fresh
// NewRecordBatch plus a fresh PreAllocBytes arena per BYTES column per group.
//
// B/op and allocs/op are the headline. ns/op should be flat or better:
// ResetForWrite pays the same memclr make() was already paying, and the BYTES
// arena's PreAllocBytes becomes a no-op past its high-water mark.
func BenchmarkReadRowGroupBacking(b *testing.B) {
	schema := []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "name", Type: pqt.TypeString},
		{Name: "amount", Type: pqt.TypeFloat64},
		{Name: "category", Type: pqt.TypeString},
	}
	for _, rows := range []int{100_000, 400_000} {
		data := groupedParquetData(rows, 25_000)
		reader, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			b.Fatal(err)
		}
		fr := reader.FileReader()
		groups := fr.NumRowGroups()
		if groups < 2 {
			b.Fatalf("need a multi-row-group file, got %d", groups)
		}
		for _, pooled := range []bool{false, true} {
			b.Run(fmt.Sprintf("rows=%d/pool=%v", rows, pooled), func(b *testing.B) {
				var pool *BackingPool
				if pooled {
					pool = NewBackingPool(BackingPoolOpts{})
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for rg := 0; rg < groups; rg++ {
						rb, err := ReadRowGroupNativeBacked(fr, rg, schema, nil, pool)
						if err != nil {
							b.Fatal(err)
						}
						pool.Recycle(rb, rb.Mint()) // nil pool: no-op
					}
				}
			})
		}
	}
}

// groupedParquetData writes n rows with an explicit row-group size so a decode
// loop crosses group boundaries.
func groupedParquetData(n, rowsPerGroup int) []byte {
	schema := pqt.Schema{
		Columns: []pqt.Column{
			{Name: "id", Type: pqt.TypeInt64},
			{Name: "name", Type: pqt.TypeString},
			{Name: "amount", Type: pqt.TypeFloat64},
			{Name: "category", Type: pqt.TypeString},
		},
	}
	cats := []string{"purchase", "refund", "view", "click", "impression"}
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{
			"id":       int64(i),
			"category": cats[i%len(cats)],
			"name":     fmt.Sprintf("user_%06d", i),
			"amount":   float64(i) * 1.5,
		}
	}
	cfg := pqt.DefaultWriterConfig()
	cfg.RowGroupSize = rowsPerGroup
	var buf bytes.Buffer
	pw, err := pqt.NewWriter(&buf, schema, cfg)
	if err != nil {
		panic(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		panic(err)
	}
	if err := pw.Close(); err != nil {
		panic(err)
	}
	return buf.Bytes()
}
