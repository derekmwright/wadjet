package scan

import (
	"bytes"
	"strings"
	"testing"

	pqt "github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A column chunk's extent is [offset, offset+total_compressed_size), and the
// reader used to CLAMP that end to the file rather than refuse it, on the
// belief that writers round the figure up. They do not. What clamping bought
// was a silent wrong answer: an overstated size reaches into the NEXT
// column's pages, the page loop decodes them as this column's, and a 128-row
// chunk comes back holding 64 of its own values and 64 belonging to a
// neighbour — err == nil, and the row path hands back
// [..., 63, 1000, 1001, ...] as if it were the column.

func twoColumnTwoGroupFile(t *testing.T) []byte {
	t.Helper()
	schema := pqt.Schema{Columns: []pqt.Column{
		{Name: "low", Type: pqt.TypeInt64},
		{Name: "high", Type: pqt.TypeInt64},
	}}
	cfg := pqt.DefaultWriterConfig()
	cfg.RowGroupSize = 64
	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, schema, cfg)
	if err != nil {
		t.Fatalf("writer: %v", err)
	}
	rows := make([]map[string]any, 128)
	for i := range rows {
		rows[i] = map[string]any{"low": int64(i), "high": int64(1000 + i)}
	}
	if err := w.WriteRows(rows); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return buf.Bytes()
}

func twoColumnSchema() []pqt.Column {
	return []pqt.Column{
		{Name: "low", Type: pqt.TypeInt64},
		{Name: "high", Type: pqt.TypeInt64},
	}
}

// TestOverstatedChunkSizeIsRefusedOnBothPaths is the no-foreign-bytes
// property. Each shape must be an ERROR — not a short read, not a read that
// happens to look right, and above all not values from the next chunk.
func TestOverstatedChunkSizeIsRefusedOnBothPaths(t *testing.T) {
	good := twoColumnTwoGroupFile(t)

	// The honest file reads, and reads its OWN values: the precondition
	// without which the negative cases prove nothing.
	fr, err := pqt.OpenFileReaderFromBytes(good)
	if err != nil {
		t.Fatalf("opening the fixture: %v", err)
	}
	if fr.NumRowGroups() != 2 {
		t.Fatalf("the fixture has %d row groups, want 2", fr.NumRowGroups())
	}
	batches, err := ReadFileBatchesNative(fr, twoColumnSchema(), nil)
	if err != nil {
		t.Fatalf("reading the fixture: %v", err)
	}
	row := 0
	for _, b := range batches {
		for i := 0; i < b.Len; i++ {
			if got := b.Columns[0].Int64Data[i]; got != int64(row) {
				t.Fatalf("row %d of \"low\" = %d", row, got)
			}
			if got := b.Columns[1].Int64Data[i]; got != int64(1000+row) {
				t.Fatalf("row %d of \"high\" = %d", row, got)
			}
			row++
		}
	}
	if row != 128 {
		t.Fatalf("the fixture read %d rows, want 128", row)
	}

	// Where the last chunk in the file lives, so the "into the footer" and
	// "past the file" shapes can be aimed.
	lastRG, lastCol := 1, 1

	cases := []struct {
		name string
		edit func(*pqt.FileMetaData)
		want string
	}{
		{
			// (a) the first column of the first row group swallows the
			// second column's chunk whole.
			name: "chunk reaches into the next column's chunk",
			edit: func(md *pqt.FileMetaData) {
				cm := md.RowGroups[0].Columns[0].MetaData
				cm.TotalCompressedSize = md.RowGroups[0].Columns[1].MetaData.TotalCompressedSize +
					cm.TotalCompressedSize
			},
			want: "overlaps",
		},
		{
			// (a') one byte over. The page loop would not even notice; the
			// layout would.
			name: "chunk overstated by a single byte",
			edit: func(md *pqt.FileMetaData) {
				md.RowGroups[0].Columns[0].MetaData.TotalCompressedSize++
			},
			want: "overlaps",
		},
		{
			// (b) the last chunk reaches into the footer.
			name: "last chunk reaches into the footer",
			edit: func(md *pqt.FileMetaData) {
				md.RowGroups[lastRG].Columns[lastCol].MetaData.TotalCompressedSize += 16
			},
			want: "before its footer",
		},
		{
			// (c) and past the end of the file altogether — the shape that
			// used to clamp.
			name: "last chunk reaches past the end of the file",
			edit: func(md *pqt.FileMetaData) {
				md.RowGroups[lastRG].Columns[lastCol].MetaData.TotalCompressedSize += 1 << 20
			},
			want: "before its footer",
		},
		{
			// (d) an offset moved back so two honest sizes overlap.
			name: "two chunks overlap through a moved offset",
			edit: func(md *pqt.FileMetaData) {
				md.RowGroups[0].Columns[1].MetaData.DataPageOffset =
					md.RowGroups[0].Columns[0].MetaData.DataPageOffset
			},
			want: "overlaps",
		},
		{
			// A chunk in the middle of the file reaching across a row group
			// boundary rather than a column one.
			name: "chunk reaches into the next row group",
			edit: func(md *pqt.FileMetaData) {
				md.RowGroups[0].Columns[1].MetaData.TotalCompressedSize += 32
			},
			want: "overlaps",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := rewriteFooter(t, good, tc.edit)
			nativeErr, rowErr := readSchemaBothPaths(t, raw, twoColumnSchema())
			if nativeErr == nil {
				t.Error("the native path read the file")
			}
			if rowErr == nil {
				t.Error("the row path read the file")
			}
			for name, err := range map[string]error{"native": nativeErr, "row": rowErr} {
				if err != nil && !strings.Contains(err.Error(), tc.want) {
					t.Errorf("%s error %q does not mention %q", name, err, tc.want)
				}
			}
		})
	}
}
