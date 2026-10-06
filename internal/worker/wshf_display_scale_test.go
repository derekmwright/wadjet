// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// displayScaleFixture: three DECIMAL(38,4) columns — one whose values carry
// different display scales, one uniform at display scale 1, one with none —
// and a NULL row.
func displayScaleFixture(t *testing.T) *batch.RecordBatch {
	t.Helper()
	col := func(name string) parquet.Column {
		return parquet.Column{Name: name, Type: parquet.TypeDecimal, Precision: 38, Scale: 4, Nullable: true}
	}
	b := batch.NewRecordBatch([]parquet.Column{col("varying"), col("uniform"), col("carrier")}, 4)
	vals := [][3]any{{"2.50", "1.5", "1.2500"}, {"2.5", "2.0", "3"}, {nil, nil, nil}, {"7", "-0.5", "0.0001"}}
	for i, r := range vals {
		for c, v := range r {
			if v == nil {
				b.Columns[c].Nulls.SetNull(i)
				continue
			}
			if err := b.Columns[c].SetValueChecked(i, v); err != nil {
				t.Fatal(err)
			}
		}
	}
	b.Columns[1].DecimalData.SetUniformDScale(1)
	b.Columns[2].DecimalData.ResetDScale()
	return b
}

func displayTexts(b *batch.RecordBatch) string {
	var rows []string
	for i := 0; i < b.Len; i++ {
		var cells []string
		for _, c := range b.Columns {
			cells = append(cells, fmt.Sprint(c.GetValueOf(i, false)))
		}
		rows = append(rows, strings.Join(cells, "|"))
	}
	return strings.Join(rows, " ; ")
}

// TestWSHFDisplayScaleRoundTrip: a DECIMAL column's display scales survive a
// .wshf exchange through every reader, with and without a row selection;
// a column without them carries no section, so its bytes are the carriers
// alone (TestWSHFUnmarkedColumnsEncodeAsBase holds the whole file to the
// base's bytes); a corrupt section — a code past the column's scale, an
// unknown mode — is refused.
func TestWSHFDisplayScaleRoundTrip(t *testing.T) {
	b := displayScaleFixture(t)
	want := displayTexts(b)
	if want != "2.50|1.5|1.2500 ; 2.5|2.0|3.0000 ; <nil>|<nil>|<nil> ; 7|-0.5|0.0001" {
		t.Fatalf("fixture prints %s", want)
	}
	encode := func(sel []uint32, n int) []byte {
		var buf bytes.Buffer
		sw := newShuffleWriter(&buf, b.Schema)
		if err := sw.writeHeader(); err != nil {
			t.Fatal(err)
		}
		if err := sw.writeChunk(b.Columns, sel, n); err != nil {
			t.Fatal(err)
		}
		data := buf.Bytes()
		binary.LittleEndian.PutUint32(data[4:], sw.numChunks)
		return data
	}
	for _, tc := range []struct {
		name string
		sel  []uint32
		n    int
		want string
	}{
		{"all", nil, 4, want},
		{"sel", []uint32{3, 0}, 2, "7|-0.5|0.0001 ; 2.50|1.5|1.2500"},
	} {
		data := encode(tc.sel, tc.n)
		for how, bs := range decodeEveryWay(t, data) {
			if len(bs) != 1 {
				t.Fatalf("%s/%s: %d batches", tc.name, how, len(bs))
			}
			if got := displayTexts(bs[0]); got != tc.want {
				t.Errorf("%s/%s: decodes as %s, want %s", tc.name, how, got, tc.want)
			}
		}
	}
	// The carrier-only column writes 16 bytes a row and nothing else.
	cb := batch.NewRecordBatch(b.Schema[2:], 4)
	cb.Columns[0] = b.Columns[2]
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, cb.Schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatal(err)
	}
	hdr := buf.Len()
	if err := sw.writeChunk(cb.Columns, nil, 4); err != nil {
		t.Fatal(err)
	}
	chunk := buf.Bytes()[hdr:]
	// rows u32, bitmap words u32 + one word, data length u32
	if dl := binary.LittleEndian.Uint32(chunk[4+4+8:]); dl != 4*16 {
		t.Errorf("a column without display scales announces %d data bytes, want %d", dl, 4*16)
	}
	// A corrupt section is refused by every reader.
	good := encode(nil, 4)
	off := firstDecimalSection(t, good)
	for name, patch := range map[string]func([]byte){
		"code past the scale": func(d []byte) { d[off+1] = 9 },
		"unknown mode":        func(d []byte) { d[off] = 7 },
	} {
		data := bytes.Clone(good)
		patch(data)
		for how, err := range openEveryWay(data) {
			if err == nil {
				t.Errorf("%s: %s decoded the file", name, how)
			}
		}
	}
}

// firstDecimalSection is the offset of the first column's display-scale
// section in a one-chunk file of the fixture's schema.
func firstDecimalSection(t *testing.T, data []byte) int {
	t.Helper()
	off := 10
	for i := 0; i < 3; i++ { // three DECIMAL columns: name, type, scale, precision
		nameLen := int(binary.LittleEndian.Uint16(data[off:]))
		off += 2 + nameLen + 1 + 2
	}
	off += 4                                             // rows
	words := int(binary.LittleEndian.Uint32(data[off:])) // bitmap words
	off += 4 + words*8                                   // bitmap
	dl := int(binary.LittleEndian.Uint32(data[off:]))    // data length
	off += 4
	if dl != 4*16+1+4 {
		t.Fatalf("the varying column announces %d data bytes, want %d", dl, 4*16+1+4)
	}
	return off + 4*16
}
