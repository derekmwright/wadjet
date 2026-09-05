package parquet

import (
	"bytes"
	"fmt"
	"testing"

	gp "github.com/parquet-go/parquet-go"
)

// BenchmarkColumnPageDecode measures the page decode loop with and without a
// page checksum in the file, because the checksum check is CPU spent per byte
// of every page body the reader decodes.
//
// Two arms on purpose. `wadjet_nochecksum` is a file wadjet's own writer
// produced, which carries no checksum at all — the arm every wadjet
// benchmark, every TPC-H run and every pyarrow-written ClickBench part sits
// in, where the check is one predictable branch per page. `parquetgo_checksum`
// is the arm that actually pays: a parquet-go file, checksum on every page,
// where the whole chunk is hashed.
func BenchmarkColumnPageDecode(b *testing.B) {
	const n = 100_000

	wadjetFile := func() []byte {
		rows := make([]map[string]any, n)
		for i := range rows {
			rows[i] = map[string]any{"x": int64(i), "s": fmt.Sprintf("value-%06d", i)}
		}
		var buf bytes.Buffer
		w, err := NewWriter(&buf, Schema{Columns: []Column{
			{Name: "x", Type: TypeInt64},
			{Name: "s", Type: TypeString},
		}}, WriterConfig{Compression: CompressionNone, RowGroupSize: n})
		if err != nil {
			b.Fatal(err)
		}
		if err := w.WriteRows(rows); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
		return buf.Bytes()
	}

	parquetGo := func() []byte {
		type rec struct {
			X int64  `parquet:"x,plain"`
			S string `parquet:"s,plain"`
		}
		var buf bytes.Buffer
		w := gp.NewGenericWriter[rec](&buf, gp.DataPageVersion(1), gp.Compression(&gp.Uncompressed))
		rows := make([]rec, n)
		for i := range rows {
			rows[i] = rec{int64(i), fmt.Sprintf("value-%06d", i)}
		}
		if _, err := w.Write(rows); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
		return buf.Bytes()
	}

	for _, arm := range []struct {
		name string
		data []byte
	}{
		{"wadjet_nochecksum", wadjetFile()},
		{"parquetgo_checksum", parquetGo()},
	} {
		b.Run(arm.name, func(b *testing.B) {
			r, err := NewReaderFromBytes(arm.data)
			if err != nil {
				b.Fatal(err)
			}
			fr := r.FileReader()
			b.ReportAllocs()
			b.SetBytes(int64(len(arm.data)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for rg := 0; rg < fr.NumRowGroups(); rg++ {
					for col := 0; col < 2; col++ {
						pr := fr.ColumnPages(rg, col)
						if pr == nil {
							b.Fatal("no chunk")
						}
						if _, err := pr.NextDictionary(); err != nil {
							b.Fatal(err)
						}
						for {
							p, err := pr.NextPage()
							if err != nil {
								b.Fatal(err)
							}
							if p == nil {
								break
							}
							p.Release()
						}
						pr.Close()
					}
				}
			}
		})
	}
}
