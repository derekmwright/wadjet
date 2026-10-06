// SPDX-License-Identifier: MIT

package exec

import (
	"bytes"
	"encoding/hex"
	"os"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// baseRawSpill is eb76eb97's spill run of three DECIMAL(38,4) carriers
// 2.5000, 7.0000, 1.2500 (the PS1 closure review's base_raw.spill).
const baseRawSpill = "0100000003000000010000001101007604260100" +
	"a861000000000000000000000000000070110100000000000000000000000000d4300000000000000000000000000000"

// TestSpillRunAUniformDisplayScaleCostsTwoBytes: a column whose values all
// share one display scale holds no per-row array and its spill run carries
// the two-byte uniform section — none, with eb76eb97's bytes, when that
// display scale is the carrier's; a varying column carries the per-row
// section. At 60f9f2d5 the uniform-2 column wrote a per-row section (72
// bytes; PS1 closure review B4).
func TestSpillRunAUniformDisplayScaleCostsTwoBytes(t *testing.T) {
	base, _ := hex.DecodeString(baseRawSpill)
	uniform2 := append(bytes.Clone(base), 0, 2)
	uniform2[18] |= 4 // the column's display-scale flag bit
	for _, tc := range []struct {
		kind   string
		texts  []string
		want   []byte
		size   int
		perRow bool
	}{
		{"raw", nil, base, 68, false},
		{"uniform4", []string{"2.5000", "7.0000", "1.2500"}, base, 68, false},
		{"uniform2", []string{"2.50", "7.00", "1.25"}, uniform2, 70, false},
		{"varying", []string{"2.50", "7", "1.250"}, nil, 72, true},
	} {
		b := batch.NewRecordBatch([]parquet.Column{{Name: "v", Type: parquet.TypeDecimal, Precision: 38, Scale: 4, Nullable: true}}, 3)
		for i := 0; i < 3; i++ {
			if tc.texts == nil {
				b.Columns[0].DecimalData.Data[i] = batch.Int128From([]int64{25000, 70000, 12500}[i])
			} else if err := b.Columns[0].SetValueChecked(i, tc.texts[i]); err != nil {
				t.Fatal(err)
			}
		}
		if got := b.Columns[0].DecimalData.DScale != nil; got != tc.perRow {
			t.Errorf("%s: per-row display-scale array present = %v, want %v", tc.kind, got, tc.perRow)
		}
		p, err := writeSpillBatches(t.TempDir(), []*batch.RecordBatch{b})
		if err != nil {
			t.Fatal(err)
		}
		d, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if len(d) != tc.size {
			t.Errorf("%s: %d bytes, want %d", tc.kind, len(d), tc.size)
		}
		if tc.want != nil && !bytes.Equal(d, tc.want) {
			t.Errorf("%s: bytes\n got %x\nwant %x", tc.kind, d, tc.want)
		}
		if tc.perRow && (len(d) < 4 || d[len(d)-4] != 1) {
			t.Errorf("%s: section %x, want the per-row form", tc.kind, d[len(d)-4:])
		}
	}
}
