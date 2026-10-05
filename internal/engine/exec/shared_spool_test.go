// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// spoolBodySource yields n batches of rows rows each (column g = 1, 2, …) and
// counts how many it produced; at batch failAt (1-based, 0 = never) it fails.
type spoolBodySource struct {
	n, rows, failAt int
	produced        atomic.Int64
}

var errBody = errors.New("body error")

func (s *spoolBodySource) Init(context.Context) error { return nil }
func (s *spoolBodySource) Close() error               { return nil }
func (s *spoolBodySource) Next(context.Context) (*batch.RecordBatch, error) {
	i := int(s.produced.Load())
	if i >= s.n {
		return nil, nil
	}
	if s.failAt > 0 && i+1 == s.failAt {
		return nil, errBody
	}
	s.produced.Add(1)
	b := batch.NewRecordBatch([]parquet.Column{{Name: "g", Type: parquet.TypeInt64}}, s.rows)
	for r := 0; r < s.rows; r++ {
		b.Columns[0].Int64Data[r] = int64(i*s.rows + r + 1)
	}
	return b, nil
}

func readFirst(t *testing.T, r Source, k int) []int64 {
	t.Helper()
	var out []int64
	for i := 0; i < k; i++ {
		b, err := r.Next(context.Background())
		if err != nil {
			t.Fatalf("next: %v", err)
		}
		if b == nil {
			break
		}
		out = append(out, b.Columns[0].Int64Data[0])
	}
	return out
}

// THE BODY ADVANCES ONLY ON DEMAND: nothing runs until a reader asks; a reader
// that read k batches has made the body produce k (one more may be in flight
// only after a reader asked for it); a second reader reads the stored batches
// without moving the body; every reader reads the same first values.
func TestSharedSpoolAdvancesOnlyOnDemand(t *testing.T) {
	src := &spoolBodySource{n: 50, rows: 4}
	s := &SharedSpool{Source: src}
	defer s.Close()
	a, b := s.NewReader(), s.NewReader()
	if s.Started() || src.produced.Load() != 0 {
		t.Fatal("the body ran before any reader asked")
	}
	got := readFirst(t, a, 3)
	if want := []int64{1, 5, 9}; len(got) != 3 || got[0] != want[0] || got[2] != want[2] {
		t.Fatalf("reader a read %v, want %v", got, want)
	}
	if p := src.produced.Load(); p != 3 {
		t.Fatalf("three batches asked for, the body produced %d", p)
	}
	if got := readFirst(t, b, 2); got[0] != 1 || got[1] != 5 {
		t.Fatalf("reader b read %v", got)
	}
	if p := src.produced.Load(); p != 3 {
		t.Fatalf("reader b read stored batches, but the body produced %d", p)
	}
}

// THE BODY'S ERROR REACHES THE READER WHOSE PULL HITS IT, WHEN IT HITS IT: the
// batches before it are read first, and a reader that stops before it never
// sees it.
func TestSharedSpoolRaisesWhereTheReaderReachesTheError(t *testing.T) {
	src := &spoolBodySource{n: 10, rows: 2, failAt: 4}
	s := &SharedSpool{Source: src}
	defer s.Close()
	early, full := s.NewReader(), s.NewReader()
	if got := readFirst(t, early, 2); len(got) != 2 {
		t.Fatalf("early reader read %v", got)
	}
	for i := 0; i < 3; i++ {
		if b, err := full.Next(context.Background()); err != nil || b == nil {
			t.Fatalf("batch %d: %v %v", i, b, err)
		}
	}
	if _, err := full.Next(context.Background()); !errors.Is(err, errBody) {
		t.Fatalf("the full reader's fourth pull: %v, want the body's error", err)
	}
}

// A READER WHOSE NEXT BATCH WAS WRITTEN TO A RUN WHILE IT READ FROM MEMORY
// reads on from the run at its position: under a tight budget every batch
// drains, and two readers interleaved read every value once, in order.
func TestSharedSpoolReaderFollowsItsBatchesIntoARun(t *testing.T) {
	tracker := memory.NewTracker("test", 1_000)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	tracker.ForceReserve(900)
	src := &spoolBodySource{n: 40, rows: 8}
	s := &SharedSpool{Source: src, Spill: sm, RunBytes: 1}
	a, b := s.NewReader(), s.NewReader()
	var gotA, gotB []int64
	for i := 0; i < 40; i++ {
		for _, rd := range []struct {
			r   *SpoolReader
			out *[]int64
		}{{a, &gotA}, {b, &gotB}} {
			if i%3 == 0 && rd.r == b {
				continue // b lags behind a
			}
			bt, err := rd.r.Next(context.Background())
			if err != nil {
				t.Fatalf("next: %v", err)
			}
			if bt != nil {
				*rd.out = append(*rd.out, bt.Columns[0].Int64Data[0])
			}
		}
	}
	for {
		bt, err := b.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if bt == nil {
			break
		}
		gotB = append(gotB, bt.Columns[0].Int64Data[0])
	}
	if s.SpillRuns() == 0 {
		t.Fatal("nothing drained: this test reads from memory only")
	}
	for name, got := range map[string][]int64{"a": gotA, "b": gotB} {
		if len(got) != 40 {
			t.Fatalf("reader %s read %d batches, want 40", name, len(got))
		}
		for i, v := range got {
			if v != int64(i*8+1) {
				t.Fatalf("reader %s batch %d starts at %d, want %d", name, i, v, i*8+1)
			}
		}
	}
	s.Close()
	if n := len(s.runs); n != 0 {
		t.Fatalf("%d runs left after Close", n)
	}
}
