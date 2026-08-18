package scan

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Sel-aware materialization must be a pure projection of the full decode:
// for every selection shape, selected rows carry byte-identical values and
// null flags, unselected rows are zero-length non-null slots, and offsets
// stay monotonic. Driven over both the shared mixed-encoding fixture
// (dictionary + PLAIN-fallback pages, nullable strings) and an
// internal-writer file with small pages, empty strings, and multiple row
// groups.

func selShapes(n int, r *rand.Rand) map[string][]uint32 {
	shapes := map[string][]uint32{
		"first":    {0},
		"last":     {uint32(n - 1)},
		"middle":   {uint32(n / 2)},
		"boundary": {0, uint32(n - 1)},
	}
	var every97, every3, all, run []uint32
	for i := 0; i < n; i += 97 {
		every97 = append(every97, uint32(i))
	}
	for i := 0; i < n; i += 3 {
		every3 = append(every3, uint32(i))
	}
	for i := 0; i < n; i++ {
		all = append(all, uint32(i))
	}
	lo := n / 3
	hi := lo + 200
	if hi > n {
		hi = n
	}
	for i := lo; i < hi; i++ {
		run = append(run, uint32(i))
	}
	var sparse []uint32
	for i := 0; i < n; i++ {
		if r.Intn(100) == 0 {
			sparse = append(sparse, uint32(i))
		}
	}
	shapes["every97"] = every97
	shapes["every3"] = every3
	shapes["all"] = all
	shapes["denserun"] = run
	if len(sparse) > 0 {
		shapes["sparse1pct"] = sparse
	}
	return shapes
}

func runSelDecodeDifferential(t *testing.T, fr *pqt.FileReader, schema []pqt.Column) {
	t.Helper()
	r := rand.New(rand.NewSource(299))
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		n := int(fr.RowGroupNumRows(rg))
		if n == 0 {
			continue
		}
		full, err := ReadRowGroupNative(fr, rg, schema, nil)
		if err != nil {
			t.Fatalf("rg %d full decode: %v", rg, err)
		}
		for name, sel := range selShapes(n, r) {
			got, err := readRowGroupNative(fr, rg, schema, nil, nil, sel, nil)
			if err != nil {
				t.Fatalf("rg %d sel %s: %v", rg, name, err)
			}
			inSel := make(map[uint32]bool, len(sel))
			for _, s := range sel {
				inSel[s] = true
			}
			for ci, col := range schema {
				fv, gv := full.Columns[ci], got.Columns[ci]
				isBytes := col.Type == pqt.TypeString || col.Type == pqt.TypeBytes
				if isBytes {
					offs := gv.BytesData.Offsets
					for i := 0; i < n; i++ {
						if offs[i+1] < offs[i] {
							t.Fatalf("rg %d sel %s col %s: offsets not monotonic at %d", rg, name, col.Name, i)
						}
					}
				}
				for i := 0; i < n; i++ {
					if !isBytes || inSel[uint32(i)] {
						if fv.Nulls.IsNull(i) != gv.Nulls.IsNull(i) {
							t.Fatalf("rg %d sel %s col %s row %d: null mismatch full=%v sel=%v",
								rg, name, col.Name, i, fv.Nulls.IsNull(i), gv.Nulls.IsNull(i))
						}
						if isBytes && !fv.Nulls.IsNull(i) && !bytes.Equal(fv.BytesData.Value(i), gv.BytesData.Value(i)) {
							t.Fatalf("rg %d sel %s col %s row %d: value mismatch full=%q sel=%q",
								rg, name, col.Name, i, fv.BytesData.Value(i), gv.BytesData.Value(i))
						}
					} else if len(gv.BytesData.Value(i)) != 0 {
						t.Fatalf("rg %d sel %s col %s row %d: unselected row materialized %q",
							rg, name, col.Name, i, gv.BytesData.Value(i))
					}
				}
				if !isBytes && fv.Type == batch.TypeInt64 {
					for i := 0; i < n; i++ {
						if fv.Int64Data[i] != gv.Int64Data[i] {
							t.Fatalf("rg %d sel %s col %s row %d: int mismatch", rg, name, col.Name, i)
						}
					}
				}
			}
		}
	}
}

func TestSelDecodeMixedEncodingFixture(t *testing.T) {
	data, err := os.ReadFile("../../storage/parquet/testdata/dict_fallback.parquet")
	if err != nil {
		t.Fatalf("read fixture (regen with gen_dict_fallback.py): %v", err)
	}
	r, err := pqt.NewReaderFromBytes(data)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	runSelDecodeDifferential(t, r.FileReader(), r.Schema().Columns)
}

func TestSelDecodeInternalWriter(t *testing.T) {
	const numRows = 30000
	schema := pqt.Schema{Columns: []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "s", Type: pqt.TypeString},                  // low-cardinality → dictionary pages
		{Name: "u", Type: pqt.TypeString},                  // unique long values → dict overflow / plain
		{Name: "sn", Type: pqt.TypeString, Nullable: true}, // nulls + empty strings
	}}
	r := rand.New(rand.NewSource(7))
	rows := make([]map[string]any, numRows)
	for i := range rows {
		row := map[string]any{
			"id": int64(i),
			"s":  fmt.Sprintf("cat-%03d", i%50),
			"u":  fmt.Sprintf("https://example.test/%d/%056d", i, r.Intn(1<<30)),
		}
		switch i % 5 {
		case 0:
			row["sn"] = nil
		case 1:
			row["sn"] = ""
		default:
			row["sn"] = fmt.Sprintf("v%d", i)
		}
		rows[i] = row
	}
	var buf bytes.Buffer
	cfg := pqt.DefaultWriterConfig()
	cfg.RowGroupSize = 8192   // multiple row groups
	cfg.PageBufferSize = 8192 // many pages per chunk
	pw, err := pqt.NewWriter(&buf, schema, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	rd, err := pqt.NewReaderFromBytes(buf.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if rd.NumRowGroups() < 2 {
		t.Fatalf("fixture produced %d row groups, want >= 2", rd.NumRowGroups())
	}
	runSelDecodeDifferential(t, rd.FileReader(), schema.Columns)
}

// BenchmarkSelDecode measures byte-array materialization under a sparse
// selection vs the full decode it replaces (100k rows × ~90B URLs, 1%
// selected — the ClickBench Q23 shape).
func BenchmarkSelDecode(b *testing.B) {
	const numRows = 100000
	schema := pqt.Schema{Columns: []pqt.Column{{Name: "u", Type: pqt.TypeString}}}
	r := rand.New(rand.NewSource(1))
	rows := make([]map[string]any, numRows)
	for i := range rows {
		rows[i] = map[string]any{"u": fmt.Sprintf("https://example.test/path/%d/%060d", i, r.Intn(1<<30))}
	}
	var buf bytes.Buffer
	pw, err := pqt.NewWriter(&buf, schema, pqt.DefaultWriterConfig())
	if err != nil {
		b.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		b.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		b.Fatal(err)
	}
	rd, err := pqt.NewReaderFromBytes(buf.Bytes())
	if err != nil {
		b.Fatal(err)
	}
	fr := rd.FileReader()
	var sel []uint32
	for i := 0; i < numRows; i += 100 {
		sel = append(sel, uint32(i))
	}
	b.Run("full", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := readRowGroupNative(fr, 0, schema.Columns, nil, nil, nil, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("sel1pct", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := readRowGroupNative(fr, 0, schema.Columns, nil, nil, sel, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
	// Clustered selection: one dense 1000-row run — every page outside it
	// skips decompress+decode via the header walk.
	var clustered []uint32
	for i := 40000; i < 41000; i++ {
		clustered = append(clustered, uint32(i))
	}
	b.Run("clustered1pct", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := readRowGroupNative(fr, 0, schema.Columns, nil, nil, clustered, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
	sparse := []uint32{5, 25000, 50000, 75000, 99999}
	b.Run("sparse5rows", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := readRowGroupNative(fr, 0, schema.Columns, nil, nil, sparse, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}
