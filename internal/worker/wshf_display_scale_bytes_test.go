// SPDX-License-Identifier: AGPL-3.0-only

package worker

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/wshf"
)

// baseRawWSHF is eb76eb97's .wshf of three DECIMAL(38,4) carriers 2.5000,
// 7.0000, 1.2500 in one chunk (the PS1 closure review's base_raw.wshf).
const baseRawWSHF = "575348460100000001000100761104260300000001000000070000000000000030000000" +
	"a861000000000000000000000000000070110100000000000000000000000000d4300000000000000000000000000000"

// displayScaleBytesFixture is one DECIMAL(38,4) column of three rows: raw
// carriers (no display scale written), text at the carrier's scale, text
// uniform at display scale 2, and text of three different display scales.
func displayScaleBytesFixture(t *testing.T, kind string) *batch.RecordBatch {
	t.Helper()
	schema := []parquet.Column{{Name: "v", Type: parquet.TypeDecimal, Precision: 38, Scale: 4, Nullable: true}}
	b := batch.NewRecordBatch(schema, 3)
	texts := map[string][]string{
		"uniform4": {"2.5000", "7.0000", "1.2500"},
		"uniform2": {"2.50", "7.00", "1.25"},
		"varying":  {"2.50", "7", "1.250"},
	}[kind]
	for i := 0; i < 3; i++ {
		if kind == "raw" {
			b.Columns[0].DecimalData.Data[i] = batch.Int128From([]int64{25000, 70000, 12500}[i])
			continue
		}
		if err := b.Columns[0].SetValueChecked(i, texts[i]); err != nil {
			t.Fatal(err)
		}
	}
	return b
}

func writeDisplayScaleBytesFixture(t *testing.T, b *batch.RecordBatch) []byte {
	t.Helper()
	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, b.Schema)
	if err := sw.writeHeader(); err != nil {
		t.Fatal(err)
	}
	if err := sw.writeChunk(b.Columns, nil, 3); err != nil {
		t.Fatal(err)
	}
	d := buf.Bytes()
	binary.LittleEndian.PutUint32(d[4:], sw.numChunks)
	return d
}

// TestWSHFAUniformDisplayScaleCostsTwoBytes: a column whose values all share
// one display scale holds no per-row array (DecimalColumn.DAll), and its
// exchange chunk carries the two-byte uniform section — or none, with bytes
// identical to eb76eb97's, when that display scale is the carrier's. At
// 60f9f2d5 the uniform-2 column held DScale=[2 2 2] after its fill (PS1
// closure review B4).
func TestWSHFAUniformDisplayScaleCostsTwoBytes(t *testing.T) {
	base, _ := hex.DecodeString(baseRawWSHF)
	for _, tc := range []struct {
		kind      string
		size      int
		baseBytes bool
		perRow    bool
	}{
		{"raw", 84, true, false},
		{"uniform4", 84, true, false},
		{"uniform2", 86, false, false},
		{"varying", 88, false, true},
	} {
		b := displayScaleBytesFixture(t, tc.kind)
		if got := b.Columns[0].DecimalData.DScale != nil; got != tc.perRow {
			t.Errorf("%s: per-row display-scale array present = %v, want %v (codes %v)", tc.kind, got, tc.perRow, b.Columns[0].DecimalData.DScale)
		}
		d := writeDisplayScaleBytesFixture(t, b)
		if len(d) != tc.size {
			t.Errorf("%s: %d bytes, want %d", tc.kind, len(d), tc.size)
		}
		if tc.baseBytes && !bytes.Equal(d, base) {
			t.Errorf("%s: bytes differ from eb76eb97's\n got %x\nwant %x", tc.kind, d, base)
		}
		if tc.kind == "uniform2" && (len(d) < 2 || d[len(d)-2] != wshf.DecimalDScaleUniform || d[len(d)-1] != 2) {
			t.Errorf("uniform2: section %x, want the uniform form 0002", d[len(d)-2:])
		}
		if tc.kind == "varying" && d[len(d)-4] != wshf.DecimalDScalePerRow {
			t.Errorf("varying: section %x, want the per-row form", d[len(d)-4:])
		}
	}
}
