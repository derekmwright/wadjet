package parquet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"testing"
)

// #974: the four-byte trailer bounds the footer, and the bound is checked
// before a single footer byte is written.
//
// The boundary is driven through footerTrailerLength rather than by building a
// 4 GB footer, which no gate should allocate. Measured at f415faba, the bare
// uint32() conversion writeFooter performed AFTER writing the footer:
//
//	len 4294967295 -> trailer 4294967295 (correct)
//	len 4294967296 -> trailer 0
//	len 4294967300 -> trailer 4
//
// so a reader seeks to byte 0 or byte 4 of the file and reads data as
// metadata, over a Close that returned nil.
func TestFooterTrailerLengthBoundary(t *testing.T) {
	cases := []struct {
		n       int64
		want    uint32
		wantErr string // substring, empty = no error
	}{
		{n: 1, want: 1},
		{n: footerMaxSize, want: footerMaxSize},
		{n: footerMaxSize + 1, wantErr: "ceiling this package will read back"},
		{n: math.MaxUint32, wantErr: "ceiling this package will read back"},
		{n: math.MaxUint32 + 1, wantErr: "4-byte trailer cannot address"},
		{n: math.MaxUint32 + 5, wantErr: "4-byte trailer cannot address"},
		{n: 0, wantErr: "encoded to 0 bytes"},
		{n: -1, wantErr: "encoded to -1 bytes"},
	}
	for _, c := range cases {
		got, err := footerTrailerLength(c.n)
		if c.wantErr == "" {
			if err != nil {
				t.Errorf("footerTrailerLength(%d) = error %v, want %d", c.n, err, c.want)
				continue
			}
			if got != c.want {
				t.Errorf("footerTrailerLength(%d) = %d, want %d", c.n, got, c.want)
			}
			continue
		}
		if err == nil {
			t.Errorf("footerTrailerLength(%d) = %d with no error — the length was narrowed instead of "+
				"refused (#974)", c.n, got)
			continue
		}
		if !strings.Contains(err.Error(), c.wantErr) {
			t.Errorf("footerTrailerLength(%d) said %q, want it to name %q", c.n, err, c.wantErr)
		}
		if got != 0 {
			t.Errorf("footerTrailerLength(%d) returned %d beside its error; a refused length must not "+
				"hand back a trailer value at all", c.n, got)
		}
	}
}

// #974 end to end: a file whose footer crosses the ceiling is NOT finalized —
// Close fails loudly, the trailer is never appended, and the writer latches.
//
// The footer is grown the way the issue says it is reachable: by row-group
// metadata, not by huge values. One RowGroup plus one ColumnChunk per column
// accumulates at every flush and is all retained to Close, so 50 INT64 columns
// at RowGroupSize 1 add ~2.8 KB of footer per row.
func TestAnOversizeFooterIsRefusedBeforeItIsWritten(t *testing.T) {
	if testing.Short() {
		t.Skip("builds a >64 MiB footer")
	}
	const cols = 50
	schema := Schema{}
	row := make(map[string]any, cols)
	for i := 0; i < cols; i++ {
		name := fmt.Sprintf("c%02d", i)
		schema.Columns = append(schema.Columns, Column{Name: name, Type: TypeInt64, Nullable: true})
		row[name] = int64(i)
	}

	var out bytes.Buffer
	w := NewNativeWriter(&out, schema, WriterConfig{RowGroupSize: 1, Compression: CompressionNone})
	// ~2.8 KB of footer per row group; 30000 clears 64 MiB with margin.
	const rows = 30000
	batch := make([]map[string]any, rows)
	for i := range batch {
		batch[i] = row
	}
	if err := w.WriteMapRows(batch); err != nil {
		t.Fatalf("writing the rows: %v", err)
	}
	beforeClose := out.Len()

	err := w.Close()
	if err == nil {
		t.Fatal("Close returned nil over a footer past the ceiling — at f415faba it wrote the footer " +
			"first and narrowed the length afterwards (#974)")
	}
	if !strings.Contains(err.Error(), "refusing to finalize the file") {
		t.Fatalf("Close said %q, want a refusal naming the footer bound", err)
	}

	// Nothing was appended: not the footer, not the length, not the magic.
	if out.Len() != beforeClose {
		t.Fatalf("the refused Close wrote %d bytes (file %d -> %d); a footer that cannot be sized must "+
			"leave the stream alone", out.Len()-beforeClose, beforeClose, out.Len())
	}
	if bytes.HasSuffix(out.Bytes(), []byte("PAR1")) {
		t.Fatal("the refused Close still appended the magic trailer")
	}

	// And the writer is latched: a later write or Close cannot resurrect it.
	if err2 := w.WriteMapRows([]map[string]any{row}); err2 == nil {
		t.Fatal("WriteMapRows after a failed Close returned nil")
	}
	if err2 := w.Close(); err2 == nil {
		t.Fatal("a second Close after a failed Close returned nil")
	}
	if out.Len() != beforeClose {
		t.Fatalf("the calls after the failed Close wrote %d bytes", out.Len()-beforeClose)
	}
}

// The trailer a finalized file carries IS the length of the footer in front of
// it — the property the bound above exists to keep true, asserted on a real
// file so the two cannot drift apart.
func TestTheTrailerLengthMatchesTheFooterItPointsAt(t *testing.T) {
	var out bytes.Buffer
	w, err := NewWriter(&out, Schema{Columns: []Column{
		{Name: "a", Type: TypeInt64, Nullable: true},
		{Name: "s", Type: TypeString, Nullable: true},
	}}, DefaultWriterConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteRows([]map[string]any{{"a": int64(1), "s": "x"}}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	data := out.Bytes()
	if len(data) < minFileSize {
		t.Fatalf("file is %d bytes", len(data))
	}
	trailer := data[len(data)-trailerSize:]
	if string(trailer[4:]) != "PAR1" {
		t.Fatalf("the file does not end with the magic: %q", trailer[4:])
	}
	footerLen := int64(binary.LittleEndian.Uint32(trailer[:4]))
	// The footer starts after the header magic and ends at the trailer.
	if footerLen <= 0 || footerLen > int64(len(data))-int64(trailerSize)-4 {
		t.Fatalf("trailer says the footer is %d bytes in a %d-byte file", footerLen, len(data))
	}
	if _, err := footerTrailerLength(footerLen); err != nil {
		t.Fatalf("the trailer this writer emitted does not survive its own bound: %v", err)
	}
}
