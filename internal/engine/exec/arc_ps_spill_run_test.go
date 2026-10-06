// SPDX-License-Identifier: MIT

package exec

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestSpillRunKeepsTheDisplayScale: the columnar spill run (grace join
// partitions, the CTE collector, the external sort) reads back each DECIMAL
// value's display scale (ADR-0024 §1 as amended) — per row, uniform, and none
// — beside the §10 mark, and a column without display scales writes exactly
// the bytes it wrote before: no flag bit, no section, so a run of such
// columns is the base's byte for byte. A uniform display scale costs two
// bytes per column per batch.
func TestSpillRunKeepsTheDisplayScale(t *testing.T) {
	col := func(name string, marked bool) parquet.Column {
		return parquet.Column{Name: name, Type: parquet.TypeDecimal, Precision: 38, Scale: 4, Nullable: true, Unconstrained: marked}
	}
	schema := []parquet.Column{col("varying", false), col("uniform", false), col("carrier", true)}
	mk := func() *batch.RecordBatch {
		b := batch.NewRecordBatch(schema, 3)
		for i, r := range [][3]string{{"2.50", "1.5", "1.2500"}, {"7", "2.0", "3"}, {"0.125", "-0.5", "0.0001"}} {
			for c, v := range r {
				if err := b.Columns[c].SetValueChecked(i, v); err != nil {
					t.Fatal(err)
				}
			}
		}
		b.Columns[1].DecimalData.SetUniformDScale(1)
		b.Columns[2].DecimalData.ResetDScale()
		return b
	}
	texts := func(b *batch.RecordBatch) string {
		var rows []string
		for i := 0; i < b.Len; i++ {
			var cells []string
			for c := range b.Columns {
				cells = append(cells, fmt.Sprint(b.Columns[c].GetValueOf(i, b.Schema[c].Unconstrained)))
			}
			rows = append(rows, strings.Join(cells, "|"))
		}
		return strings.Join(rows, " ; ")
	}
	in := mk()
	want := texts(in)
	if want != "2.50|1.5|1.25 ; 7|2.0|3 ; 0.125|-0.5|0.0001" {
		t.Fatalf("fixture prints %s", want)
	}
	path, err := writeSpillBatches(t.TempDir(), []*batch.RecordBatch{in})
	if err != nil {
		t.Fatal(err)
	}
	got, err := readSpillBatches(path)
	if err != nil {
		t.Fatal(err)
	}
	if g := texts(got[0]); g != want {
		t.Errorf("read back %s, want %s", g, want)
	}
	if !got[0].Schema[2].Unconstrained || got[0].Columns[2].DecimalData.HasDisplayScale() {
		t.Errorf("the carrier column came back with mark %v, display scales %v",
			got[0].Schema[2].Unconstrained, got[0].Columns[2].DecimalData.HasDisplayScale())
	}

	size := func(b *batch.RecordBatch) int64 {
		p, err := writeSpillBatches(t.TempDir(), []*batch.RecordBatch{b})
		if err != nil {
			t.Fatal(err)
		}
		st, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		return st.Size()
	}
	plain := mk()
	for _, c := range plain.Columns {
		c.DecimalData.ResetDScale()
	}
	uniform := mk()
	uniform.Columns[0].DecimalData.ResetDScale()
	uniform.Columns[1].DecimalData.SetUniformDScale(1)
	base := size(plain)
	if d := size(uniform) - base; d != 2 {
		t.Errorf("a uniform display scale costs %d bytes in the run, want 2 ([0][code])", d)
	}
	if d := size(in) - base; d != 2+1+3 {
		t.Errorf("a uniform column and a per-row column cost %d bytes, want %d", d, 2+1+3)
	}

	// The base's bytes: eb76eb97's writeSpillBatches over the same carriers
	// (a constrained column and a marked one with a NULL).
	golden := mkGolden(t)
	p, err := writeSpillBatches(t.TempDir(), []*batch.RecordBatch{golden})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if h := hex.EncodeToString(raw); h != spillRunGoldenAtBase {
		t.Errorf("a run without display scales no longer writes the base's bytes:\n got  %s\n want %s", h, spillRunGoldenAtBase)
	}
}

// spillRunGoldenAtBase is eb76eb97's run for mkGolden's batch.
const spillRunGoldenAtBase = "0100000003000000020000001101006104260100a8610000000000000000000000000000701101000000000000000000000000001efbffffffffffffffffffffffffffff110100620426030104f8240100000000000000000000000000503403000000000000000000000000005af1ffffffffffffffffffffffffffff"

func mkGolden(t *testing.T) *batch.RecordBatch {
	t.Helper()
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeDecimal, Precision: 38, Scale: 4, Nullable: true},
		{Name: "b", Type: parquet.TypeDecimal, Precision: 38, Scale: 4, Nullable: true, Unconstrained: true},
	}
	b := batch.NewRecordBatch(schema, 3)
	for i, v := range []int64{25000, 70000, -1250} {
		b.Columns[0].DecimalData.Data[i] = batch.Int128From(v)
		b.Columns[1].DecimalData.Data[i] = batch.Int128From(v * 3)
	}
	b.Columns[1].Nulls.SetNull(2)
	return b
}

// TestDisplayScaleIsNeverAKey is invariant I5: equal numbers of different
// display scales (`2.5`, `2.50`, `2.5000` in one DECIMAL(38,4) column) are
// one value to every comparison and key — the vector comparator, the
// in-memory group / join key, the spill run's merge key built from the
// printed boxes, the boxed sort / coordinator key — while each prints its
// own text.
func TestDisplayScaleIsNeverAKey(t *testing.T) {
	col := parquet.Column{Name: "d", Type: parquet.TypeDecimal, Precision: 38, Scale: 4, Nullable: true}
	v := batch.NewRecordBatch([]parquet.Column{col}, 3).Columns[0]
	for i, s := range []string{"2.5", "2.50", "2.5000"} {
		if err := v.SetValueChecked(i, s); err != nil {
			t.Fatal(err)
		}
	}
	if a, b, c := v.GetValueOf(0, false), v.GetValueOf(1, false), v.GetValueOf(2, false); a != "2.5" || b != "2.50" || c != "2.5000" {
		t.Fatalf("the fixture prints %v %v %v", a, b, c)
	}
	var keys, boxKeys, sortKeys []string
	for i := 0; i < 3; i++ {
		if c := kernel.CompareValuesAt(v, 0, v, i); c != 0 {
			t.Errorf("row 0 and row %d compare %d", i, c)
		}
		keys = append(keys, string(appendColumnValue(nil, v, i, batch.TypeDecimal)))
		boxKeys = append(boxKeys, string(appendGroupKeyColumn(nil, v.GetValueOf(i, false), batch.TypeDecimal, &col)))
		sortKeys = append(sortKeys, string(AppendBoxedGroupKey(nil, v.GetValueOf(i, false), &col)))
	}
	for name, ks := range map[string][]string{"in-memory key": keys, "spill merge key": boxKeys, "boxed key": sortKeys} {
		if ks[0] != ks[1] || ks[1] != ks[2] {
			t.Errorf("%s differs between equal values: %q", name, ks)
		}
	}
}
