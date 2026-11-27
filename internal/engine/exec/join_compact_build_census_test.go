// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The compact builds (key-only semi/anti, serial and parallel, and the NE
// distinct-pair build) measured: what they hold, enumerated independently from
// the structures themselves, against what THIS join charged its tracker, at
// the builds' synchronized points (#1706, line 1706p).

// cbGenSource generates its build rows on demand, so the only large live heap
// during a build is the build's own state. Safe for concurrent Next.
type cbGenSource struct {
	mu     sync.Mutex
	schema []parquet.Column
	n      int  // rows
	str    bool // a ~40-byte string key instead of an int64 one
	ne     bool // add an int64 value column v (two distinct values per key)
	dup    int  // each key repeats dup times (1 = distinct)
	nulls  int  // every nulls-th key is NULL (0 = none)
	sel    bool // batches carry a selection vector over their even rows
	next   int
	// failAt, when > 0, makes the failAt-th Next call return errCBSource.
	failAt int
	calls  int
	// before runs on every Next under mu, with the call number.
	before func(call int)
}

var errCBSource = fmt.Errorf("injected compact build source failure")

func newCBGen(n int, str, ne bool) *cbGenSource {
	s := &cbGenSource{n: n, str: str, ne: ne, dup: 1}
	kt := parquet.TypeInt64
	if str {
		kt = parquet.TypeString
	}
	s.schema = []parquet.Column{{Name: "k", Type: kt}}
	if ne {
		s.schema = append(s.schema, parquet.Column{Name: "v", Type: parquet.TypeInt64})
	}
	return s
}

func (s *cbGenSource) Init(context.Context) error { s.next = 0; s.calls = 0; return nil }
func (s *cbGenSource) Close() error               { return nil }
func (s *cbGenSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.before != nil {
		s.before(s.calls)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.failAt > 0 && s.calls >= s.failAt {
		return nil, errCBSource
	}
	if s.next >= s.n {
		return nil, nil
	}
	m := min(batch.DefaultBatchSize, s.n-s.next)
	b := batch.NewRecordBatch(s.schema, m)
	for j := 0; j < m; j++ {
		r := s.row(s.next + j)
		switch {
		case r.keyNull:
			if s.str {
				b.Columns[0].BytesData.SetString(j, "") // keep the offsets dense
			}
			b.Columns[0].Nulls.SetNull(j)
		case s.str:
			b.Columns[0].BytesData.SetString(j, cbStrKey(r.key))
		default:
			b.Columns[0].Int64Data[j] = r.key
		}
		if s.ne {
			if r.valNull {
				b.Columns[1].Nulls.SetNull(j)
			} else {
				b.Columns[1].Int64Data[j] = r.val
			}
		}
	}
	b.Len = m
	s.next += m
	if s.sel {
		// A selected batch: only the even rows are live.
		for j := 0; j < m; j += 2 {
			b.Sel = append(b.Sel, uint32(j))
		}
	}
	return b, nil
}

type cbRow struct {
	key, val         int64
	keyNull, valNull bool
}

// row is generated row i: key i/dup (NULL every nulls-th row), and for NE a
// value i%3 (NULL on the row after each NULL key), so a key spans one, two or
// three distinct values.
func (s *cbGenSource) row(i int) cbRow {
	r := cbRow{key: int64(i / max(s.dup, 1)), val: int64(i % 3)}
	if s.nulls > 0 {
		r.keyNull = i%s.nulls == 0
		r.valNull = i%s.nulls == 1
	}
	return r
}

// live reports whether row i is live (not deselected).
func (s *cbGenSource) live(i int) bool { return !s.sel || (i%batch.DefaultBatchSize)%2 == 0 }

func cbStrKey(k int64) string { return fmt.Sprintf("key-%036d", k) }

// cbModel enumerates the bytes a compact build holds at a hook from the
// structures themselves: every local table still referenced, the index part
// tables (an adopted local counted once), arenas, the part headers, the bloom,
// NE pairs and the key buffers. It shares no code with the join's accounting.
func cbModel(pt *CompactBuildPoint) int64 {
	var held int64
	seenInt := map[*intHashTable]bool{}
	seenStr := map[*strHashTable]bool{}
	intBytes := func(t *intHashTable) int64 {
		if t == nil || seenInt[t] {
			return 0
		}
		seenInt[t] = true
		return int64(cap(t.entries)) * 16
	}
	strBytes := func(t *strHashTable) int64 {
		if t == nil || seenStr[t] {
			return 0
		}
		seenStr[t] = true
		n := int64(cap(t.entries)) * 16
		for _, c := range t.chunks {
			n += int64(cap(c))
		}
		return n + int64(cap(t.chunks))*24
	}
	for _, lb := range pt.Locals {
		if lb == nil {
			continue
		}
		held += intBytes(lb.intIndex) + strBytes(lb.strIndex) + int64(cap(lb.keyBuf))
	}
	h := pt.Join
	held += int64(cap(h.parts)) * sizeofJoinIndexPart
	for i := range h.parts {
		p := &h.parts[i]
		held += intBytes(p.ints) + strBytes(p.strs)
		held += int64(cap(p.arena))*8 + int64(cap(p.next))*4 + int64(cap(p.matched))
	}
	held += int64(cap(h.bloom))*8 + int64(cap(h.nePairs))*24 + int64(cap(h.keyBuf))
	return held
}

// cbHeap is the live heap after two collections.
func cbHeap() int64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return int64(ms.HeapAlloc)
}

type cbSample struct {
	route                  string
	maxHeld, chargedAtMax  int64
	maxCharged             int64
	doneHeld, doneCharged  int64
	maxHeapDelta, heapDone int64
	usedDone               int64
	points                 int
}

// cbCharged is THIS join's charge: what it reserved and forced, both.
func cbCharged(h *HashJoin) int64 { return h.trackedMem + h.compactCharged.Load() }

// runCompactCensus builds one compact join over src and samples model,
// charge and heap at every hook. heapEvery samples the heap at every
// heapEvery-th arrival (0 = only at the done/locals/merged points).
func runCompactCensus(t *testing.T, jt JoinType, src *cbGenSource, budget int64, heapEvery int) (cbSample, error) {
	t.Helper()
	var hj *HashJoin
	if src.ne {
		hj = NewHashJoin(jt, []string{"k"}, []string{"k"})
		hj.SemiAntiFilter = BuildNETestFilter("pv", "v")
		hj.SemiAntiNEProbeCol, hj.SemiAntiNEBuildCol = "pv", "v"
		hj.ProbeSchemaHint = []parquet.Column{{Name: "k", Type: parquet.TypeInt64}, {Name: "pv", Type: parquet.TypeInt64}}
		hj.BuildSchemaHint = src.schema
	} else {
		hj = NewHashJoin(jt, []string{"k"}, []string{"k"})
		hj.SemiAntiKeyOnly = true
	}
	hj.BuildRowHint = int64(src.n)
	tr := memory.NewTracker("census", budget)
	hj.MemTracker = tr
	sm, err := memory.NewSpillManager(t.TempDir(), tr)
	if err != nil {
		t.Fatal(err)
	}
	hj.Spill = sm
	defer hj.Close()

	var s cbSample
	base := cbHeap()
	arrivals := 0
	restore := SetPhaseHook(func(site PhaseSite, op any) {
		pt, ok := op.(*CompactBuildPoint)
		if !ok {
			return
		}
		s.route = pt.Route
		s.points++
		held := cbModel(pt)
		ch := cbCharged(pt.Join)
		if held > s.maxHeld {
			s.maxHeld, s.chargedAtMax = held, ch
		}
		if ch > s.maxCharged {
			s.maxCharged = ch
		}
		sample := site != PhaseCompactArrival
		if site == PhaseCompactArrival {
			arrivals++
			sample = heapEvery > 0 && arrivals%heapEvery == 0
		}
		if sample {
			if d := cbHeap() - base; d > s.maxHeapDelta {
				s.maxHeapDelta = d
			}
		}
		if site == PhaseCompactDone {
			s.doneHeld, s.doneCharged = held, ch
			s.heapDone = cbHeap() - base
		}
		// The heap samples above must see what the model counted: without
		// this the collector may treat pt (and the locals it names) as dead
		// once the model has read them.
		runtime.KeepAlive(pt)
	})
	defer restore()
	berr := hj.Build(context.Background(), src)
	s.usedDone = tr.Used()
	return s, berr
}

// TestCompactBuildCensus prints held against charged for the compact builds.
// It is a measurement, not a gate: WADJET_COMPACT_CENSUS=1 runs it.
func TestCompactBuildCensus(t *testing.T) {
	if os.Getenv("WADJET_COMPACT_CENSUS") == "" {
		t.Skip("measurement: set WADJET_COMPACT_CENSUS=1")
	}
	type shape struct {
		name string
		n    int
		str  bool
		ne   bool
	}
	shapes := []shape{
		{"int-1M", 1_000_000, false, false},
		{"int-5M", 5_000_000, false, false},
		{"str40-1M", 1_000_000, true, false},
		{"ne-int-1M", 1_000_000, false, true},
		{"ne-int-5M", 5_000_000, false, true},
	}
	if v := os.Getenv("WADJET_COMPACT_CENSUS_SHAPES"); v != "" {
		var keep []shape
		for _, sh := range shapes {
			if v == sh.name {
				keep = append(keep, sh)
			}
		}
		shapes = keep
	}
	budgets := []int64{0, 64 << 20, 8 << 20}
	t.Logf("NumCPU=%d GOMAXPROCS=%d WADJET_SCAN_WORKERS=%q", runtime.NumCPU(), runtime.GOMAXPROCS(0), os.Getenv("WADJET_SCAN_WORKERS"))
	t.Logf("%-10s %-9s %-18s %12s %12s %12s %12s %12s %12s %12s  %s",
		"shape", "budget", "route", "maxHeld", "chgAtMax", "maxCharged", "doneHeld", "doneChg", "maxHeapΔ", "heapDone", "result")
	for _, sh := range shapes {
		for _, bud := range budgets {
			src := newCBGen(sh.n, sh.str, sh.ne)
			s, err := runCompactCensus(t, AntiJoin, src, bud, 64)
			res := "completed"
			if err != nil {
				res = "refused: " + err.Error()
			}
			bs := "none"
			if bud > 0 {
				bs = strconv.FormatInt(bud>>20, 10) + "MiB"
			}
			t.Logf("%-10s %-9s %-18s %12d %12d %12d %12d %12d %12d %12d  %s",
				sh.name, bs, s.route, s.maxHeld, s.chargedAtMax, s.maxCharged, s.doneHeld, s.doneCharged, s.maxHeapDelta, s.heapDone, res)
		}
	}
}
