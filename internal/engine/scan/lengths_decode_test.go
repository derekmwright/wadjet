package scan

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"

	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The lengths-only decode must be a pure SHAPE projection of the full
// decode: for every row, LengthAt equals len(full value), the null flags
// are identical, and the offsets stay monotonic. Nothing else about the
// column may be read — Data is empty and Value panics by contract.
//
// Driven over the shared mixed-encoding fixture (dictionary + PLAIN
// fallback pages, nullable strings) and an internal-writer file with small
// pages, empty strings, nulls, and multiple row groups, mirroring
// sel_decode_test.go.

func runLengthsDecodeDifferential(t *testing.T, fr *pqt.FileReader, schema []pqt.Column) {
	t.Helper()
	shapeOnly := make(map[string]bool)
	byteCols := 0
	for _, c := range schema {
		if c.Type == pqt.TypeString || c.Type == pqt.TypeBytes {
			shapeOnly[c.Name] = true
			byteCols++
		}
	}
	if byteCols == 0 {
		t.Fatal("fixture has no byte-array columns — the differential proves nothing")
	}
	for rg := 0; rg < fr.NumRowGroups(); rg++ {
		n := int(fr.RowGroupNumRows(rg))
		if n == 0 {
			continue
		}
		full, err := ReadRowGroupNative(fr, rg, schema, nil)
		if err != nil {
			t.Fatalf("rg %d full decode: %v", rg, err)
		}
		got, err := readRowGroupNative(fr, rg, schema, nil, nil, nil, shapeOnly)
		if err != nil {
			t.Fatalf("rg %d lengths decode: %v", rg, err)
		}
		for ci, col := range schema {
			fv, gv := full.Columns[ci], got.Columns[ci]
			if !shapeOnly[col.Name] {
				// Non-shape columns must be untouched by the mode.
				for i := 0; i < n; i++ {
					if fv.Nulls.IsNull(i) != gv.Nulls.IsNull(i) {
						t.Fatalf("rg %d col %s row %d: null mismatch on a full-decode column", rg, col.Name, i)
					}
				}
				continue
			}
			if !gv.BytesData.ShapeOnly {
				t.Fatalf("rg %d col %s: shape-only column not marked", rg, col.Name)
			}
			if len(gv.BytesData.Data) != 0 {
				t.Fatalf("rg %d col %s: shape-only column materialized %d value bytes", rg, col.Name, len(gv.BytesData.Data))
			}
			offs := gv.BytesData.Offsets
			for i := 0; i < n; i++ {
				if offs[i+1] < offs[i] {
					t.Fatalf("rg %d col %s: offsets not monotonic at %d", rg, col.Name, i)
				}
				if fv.Nulls.IsNull(i) != gv.Nulls.IsNull(i) {
					t.Fatalf("rg %d col %s row %d: null mismatch full=%v lengths=%v",
						rg, col.Name, i, fv.Nulls.IsNull(i), gv.Nulls.IsNull(i))
				}
				if gv.Nulls.IsNull(i) {
					continue
				}
				want := len(fv.BytesData.Value(i))
				if got := gv.BytesData.LengthAt(i); got != want {
					t.Fatalf("rg %d col %s row %d: length %d want %d (value %q)",
						rg, col.Name, i, got, want, fv.BytesData.Value(i))
				}
			}
		}
	}
}

func TestLengthsDecodeMixedEncodingFixture(t *testing.T) {
	data, err := os.ReadFile("../../storage/parquet/testdata/dict_fallback.parquet")
	if err != nil {
		t.Fatalf("read fixture (regen with gen_dict_fallback.py): %v", err)
	}
	r, err := pqt.NewReaderFromBytes(data)
	if err != nil {
		t.Fatalf("open reader: %v", err)
	}
	runLengthsDecodeDifferential(t, r.FileReader(), r.Schema().Columns)
}

// lengthsFixture writes the internal-writer file the differential and the
// benchmark share: dictionary pages, dict-overflow/PLAIN pages, nulls,
// empty strings, multibyte UTF-8, and multiple row groups.
func lengthsFixture(tb testing.TB, numRows int, rowGroupSize int) (*pqt.Reader, pqt.Schema) {
	tb.Helper()
	schema := pqt.Schema{Columns: []pqt.Column{
		{Name: "id", Type: pqt.TypeInt64},
		{Name: "s", Type: pqt.TypeString},                  // low-cardinality → dictionary pages
		{Name: "u", Type: pqt.TypeString},                  // unique long values → dict overflow / plain
		{Name: "sn", Type: pqt.TypeString, Nullable: true}, // nulls + empty strings + multibyte
	}}
	r := rand.New(rand.NewSource(28))
	rows := make([]map[string]any, numRows)
	for i := range rows {
		row := map[string]any{
			"id": int64(i),
			"s":  fmt.Sprintf("cat-%03d", i%50),
			"u":  fmt.Sprintf("https://example.test/%d/%056d", i, r.Intn(1<<30)),
		}
		switch i % 6 {
		case 0:
			row["sn"] = nil
		case 1:
			row["sn"] = ""
		case 2:
			row["sn"] = "日本語テキスト" // multibyte: byte length != rune count
		case 3:
			row["sn"] = "émoji 😀"
		default:
			row["sn"] = fmt.Sprintf("v%d", i)
		}
		rows[i] = row
	}
	var buf bytes.Buffer
	cfg := pqt.DefaultWriterConfig()
	cfg.RowGroupSize = rowGroupSize
	cfg.PageBufferSize = 8192
	pw, err := pqt.NewWriter(&buf, schema, cfg)
	if err != nil {
		tb.Fatal(err)
	}
	if err := pw.WriteRows(rows); err != nil {
		tb.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		tb.Fatal(err)
	}
	rd, err := pqt.NewReaderFromBytes(buf.Bytes())
	if err != nil {
		tb.Fatal(err)
	}
	return rd, schema
}

func TestLengthsDecodeInternalWriter(t *testing.T) {
	rd, schema := lengthsFixture(t, 30000, 8192)
	if rd.NumRowGroups() < 2 {
		t.Fatalf("fixture produced %d row groups, want >= 2", rd.NumRowGroups())
	}
	runLengthsDecodeDifferential(t, rd.FileReader(), schema.Columns)
}

// TestLengthsDecodeValueReadPanics pins the correctness net: a shape-only
// column that reaches a VALUE consumer fails loudly instead of returning a
// wrong answer.
func TestLengthsDecodeValueReadPanics(t *testing.T) {
	rd, schema := lengthsFixture(t, 4096, 4096)
	b, err := readRowGroupNative(rd.FileReader(), 0, schema.Columns, nil, nil, nil, map[string]bool{"u": true})
	if err != nil {
		t.Fatal(err)
	}
	col := b.Columns[2]
	defer func() {
		if recover() == nil {
			t.Fatal("reading a value from a shape-only column must panic")
		}
	}()
	_ = col.BytesData.Value(0)
}

// TestLengthsDecodeToggleOff pins that the kill switch restores the full
// decode through the public entry point.
func TestLengthsDecodeToggleOff(t *testing.T) {
	rd, schema := lengthsFixture(t, 4096, 4096)
	fr := rd.FileReader()
	shapeOnly := map[string]bool{"u": true}

	prev := lengthsOnlyToggle.Set(false)
	off, err := ReadRowGroupNativeShaped(fr, 0, schema.Columns, nil, nil, shapeOnly)
	lengthsOnlyToggle.Set(prev)
	if err != nil {
		t.Fatal(err)
	}
	if off.Columns[2].BytesData.ShapeOnly {
		t.Error("kill switch off must decode values")
	}
	if len(off.Columns[2].BytesData.Data) == 0 {
		t.Error("kill switch off must materialize value bytes")
	}

	on, err := ReadRowGroupNativeShaped(fr, 0, schema.Columns, nil, nil, shapeOnly)
	if err != nil {
		t.Fatal(err)
	}
	if !on.Columns[2].BytesData.ShapeOnly {
		t.Error("toggle on must take the lengths-only path")
	}
	n := int(fr.RowGroupNumRows(0))
	for i := 0; i < n; i++ {
		if got, want := on.Columns[2].BytesData.LengthAt(i), len(off.Columns[2].BytesData.Value(i)); got != want {
			t.Fatalf("row %d: lengths-only %d, full decode %d", i, got, want)
		}
	}
}

// TestLengthsDecodeNonEligibleColumns: a column the scan cannot decode as
// lengths (non-byte-array leaf) must fall through to the full decode even
// when named in the shape-only set.
func TestLengthsDecodeNonEligibleColumns(t *testing.T) {
	rd, schema := lengthsFixture(t, 4096, 4096)
	fr := rd.FileReader()
	b, err := readRowGroupNative(fr, 0, schema.Columns, nil, nil, nil, map[string]bool{"id": true})
	if err != nil {
		t.Fatal(err)
	}
	full, err := ReadRowGroupNative(fr, 0, schema.Columns, nil)
	if err != nil {
		t.Fatal(err)
	}
	n := int(fr.RowGroupNumRows(0))
	for i := 0; i < n; i++ {
		if b.Columns[0].Int64Data[i] != full.Columns[0].Int64Data[i] {
			t.Fatalf("row %d: int column diverged under a shape-only mark", i)
		}
	}
}

// BenchmarkLengthsDecode measures the ClickBench Q28 column profile
// (100k rows x ~90B URLs): the full decode's dictionary gather + arena
// growth + page memcpy against the offsets-only walk.
func BenchmarkLengthsDecode(b *testing.B) {
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
	shapeOnly := map[string]bool{"u": true}
	b.Run("full", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := readRowGroupNative(fr, 0, schema.Columns, nil, nil, nil, nil); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("lengths-only", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := readRowGroupNative(fr, 0, schema.Columns, nil, nil, nil, shapeOnly); err != nil {
				b.Fatal(err)
			}
		}
	})
}
