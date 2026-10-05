// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"math"
	"slices"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// selSource yields one batch of n physical rows whose selection is sel (nil:
// every row), the shape a scan hands on after a DELETE's markers.
type selSource struct {
	n    int
	sel  []uint32
	done bool
}

func (s *selSource) Init(context.Context) error { return nil }
func (s *selSource) Close() error               { return nil }
func (s *selSource) Next(context.Context) (*batch.RecordBatch, error) {
	if s.done || s.n == 0 {
		return nil, nil
	}
	s.done = true
	return &batch.RecordBatch{Len: s.n, Sel: slices.Clone(s.sel)}, nil
}

func drainSampled(t *testing.T, src Source) ([]uint32, error) {
	t.Helper()
	ctx := context.Background()
	var rows []uint32
	for {
		b, err := src.Next(ctx)
		if err != nil {
			return nil, err
		}
		if b == nil {
			return rows, nil
		}
		if b.Sel == nil {
			for i := range b.Len {
				rows = append(rows, uint32(i))
			}
			continue
		}
		rows = append(rows, b.Sel...)
	}
}

// THE SAMPLE IS DRAWN FROM THE ROWS THE SCAN SELECTS (#1411 measured case B1).
//
// A DELETE keeps the batch's Len physical and drops the deleted rows from
// Sel. The sampler redrew from 0..Len-1, so BERNOULLI (100) returned a
// deleted row; SYSTEM (0) emptied Len but left Sel, so it returned the
// selected rows. Both now narrow the batch's own selection.
func TestSampledSourceHonoursTheScansSelection(t *testing.T) {
	sel := []uint32{1, 3, 5}
	for _, c := range []struct {
		method string
		pct    float64
		want   []uint32
	}{
		{"BERNOULLI", 100, []uint32{1, 3, 5}},
		{"BERNOULLI", 0, nil},
		{"SYSTEM", 100, []uint32{1, 3, 5}},
		{"SYSTEM", 0, nil},
	} {
		src := NewSampledSource(&selSource{n: 8, sel: sel}, TableSample{Method: c.method, Percent: c.pct})
		got, err := drainSampled(t, src)
		if err != nil {
			t.Fatalf("%s (%v): %v", c.method, c.pct, err)
		}
		if !slices.Equal(got, c.want) {
			t.Errorf("%s (%v) over Sel %v: rows %v, want %v", c.method, c.pct, sel, got, c.want)
		}
	}
	// A sample at 50 % never returns a row the selection dropped.
	for range 20 {
		got, err := drainSampled(t, NewSampledSource(&selSource{n: 8, sel: sel},
			TableSample{Method: "BERNOULLI", Percent: 50}))
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range got {
			if !slices.Contains(sel, r) {
				t.Fatalf("BERNOULLI (50) returned row %d, which the scan's selection %v dropped", r, sel)
			}
		}
	}
}

// SYSTEM keeps or drops whole blocks of SampleBlockRows physical rows: a
// batch longer than one block is sampled per block, and only the rows its
// selection keeps inside a kept block come back.
func TestSampledSourceSystemSamplesBlocks(t *testing.T) {
	n := 3*SampleBlockRows + 7
	sel := make([]uint32, 0, n)
	for i := 0; i < n; i += 2 {
		sel = append(sel, uint32(i))
	}
	sawPartial := false
	for range 40 {
		got, err := drainSampled(t, NewSampledSource(&selSource{n: n, sel: sel},
			TableSample{Method: "SYSTEM", Percent: 50}))
		if err != nil {
			t.Fatal(err)
		}
		perBlock := map[int]int{}
		for _, r := range got {
			if r%2 != 0 {
				t.Fatalf("SYSTEM (50) returned row %d, which the selection dropped", r)
			}
			perBlock[int(r)/SampleBlockRows]++
		}
		for blk, k := range perBlock {
			full := 0
			for _, r := range sel {
				if int(r)/SampleBlockRows == blk {
					full++
				}
			}
			if k != full {
				t.Fatalf("block %d: %d of its %d selected rows kept; SYSTEM keeps a block whole", blk, k, full)
			}
		}
		if len(perBlock) > 0 && len(perBlock) < 4 {
			sawPartial = true
		}
	}
	if !sawPartial {
		t.Error("SYSTEM (50) never kept some blocks and dropped others in 40 draws")
	}
}

// The range is checked when the scan begins — its first Next — over a source
// with no rows too, as PostgreSQL checks it when a sample scan begins.
func TestSampledSourceChecksTheRangeWhenTheScanBegins(t *testing.T) {
	for _, c := range []struct {
		pct  float64
		null bool
		want string
	}{
		{101, false, "2202H"},
		{-1, false, "2202H"},
		{math.NaN(), false, "2202H"},
		{0, true, "2202H"},
		{100, false, ""},
		{0, false, ""},
	} {
		for _, n := range []int{0, 3} {
			src := NewSampledSource(&selSource{n: n}, TableSample{Method: "BERNOULLI", Percent: c.pct, Null: c.null})
			_, err := drainSampled(t, src)
			if got := sqlerr.StateOf(err); got != c.want {
				t.Errorf("(%v, null=%v) over %d rows: SQLSTATE %q, want %q (%v)", c.pct, c.null, n, got, c.want, err)
			}
		}
	}
}
