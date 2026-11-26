// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// #1706, line 1706o: the census of a grace join's spill buffers, kept as a
// test. It asserts the LIFECYCLE the build-end close established — no build
// writer survives the build, so build and probe writers are never open
// together; the probe writers are gone once the replay starts; the replay
// holds one probe-run reader at a time; nothing is open or left after Close —
// and it LOGS the bytes those buffers hold beside what is charged for them.
// Nothing charges them (the accounting step of 1706o is not in this tree), so
// the logged gap is the whole held figure: the census is the measurement, not
// a claim that the join is within its budget.
//
// "Held" is enumerated from the operator at synchronized points — the build's
// arrival boundaries (h.mu free), the probe's batch boundaries, and each
// output batch of the replay — as the reachable bufio buffers: every writer
// in the build and probe writer maps that still holds its buffer, and the
// replay's probe-run reader. The build-run readers live only inside the
// prefetch goroutine's readSpillBatches (one run at a time, opened, read and
// closed there) and are not counted here.
//
// The shape is the join-envelope proof's uniform one: 200,000 build rows,
// 400,000 probe rows, every build key matched twice.

type censusPhase struct {
	samples                 int
	buildW, probeW, readers int // largest counts seen
	held                    int64
}

func (c *censusPhase) note(buildW, probeW, readers int, held int64) {
	c.samples++
	c.buildW = max(c.buildW, buildW)
	c.probeW = max(c.probeW, probeW)
	c.readers = max(c.readers, readers)
	c.held = max(c.held, held)
}

func censusWriters(m map[int]*spillBatchWriter) (n int, held int64) {
	for _, w := range m {
		if w != nil && w.w != nil && w.f != nil {
			n++
			held += int64(w.w.Size())
		}
	}
	return n, held
}

type censusSink struct {
	CollectSink
	onConsume func()
}

func (s *censusSink) Consume(ctx context.Context, b *batch.RecordBatch) error {
	if s.onConsume != nil {
		s.onConsume()
	}
	return s.CollectSink.Consume(ctx, b)
}

func TestJoinSpillBufferCensus(t *testing.T) {
	kinds := []struct {
		name string
		jt   JoinType
		rows int
	}{
		{"inner", InnerJoin, 400_000},
		{"left", LeftJoin, 400_000},
		{"full", FullOuterJoin, 400_000},
		{"right_semi", RightSemiJoin, 200_000},
	}
	for _, mib := range []int64{2, 8, 32} {
		for _, k := range kinds {
			t.Run(fmt.Sprintf("%dMiB/%s", mib, k.name), func(t *testing.T) {
				// Fresh batches per cell: the build and the pipeline release
				// what they consume.
				build := lcGen("b", 200_000, 16, func(i int) int64 { return int64(i) })
				probeIn := lcGen("p", 400_000, 16, func(i int) int64 { return int64(i / 2) })
				ctx := context.Background()
				tr := memory.NewTracker("census", mib<<20)
				sm, err := memory.NewSpillManager(t.TempDir(), tr)
				if err != nil {
					t.Fatal(err)
				}
				defer sm.Cleanup()
				hj := NewHashJoin(k.jt, []string{"pk"}, []string{"bk"})
				hj.Spill, hj.MemTracker = sm, tr
				phases := map[string]*censusPhase{}
				ph := func(name string) *censusPhase {
					if phases[name] == nil {
						phases[name] = &censusPhase{}
					}
					return phases[name]
				}
				lastSpilled := 0
				src := &gatedSource{inner: NewBatchSource(build), before: func(int) {
					hj.mu.Lock()
					defer hj.mu.Unlock()
					ss := hj.spillState
					if ss == nil {
						return
					}
					n, held := censusWriters(ss.partBuildWriters)
					name := "arrival"
					if len(ss.spilledParts) > lastSpilled {
						name = "eviction"
						lastSpilled = len(ss.spilledParts)
					}
					ph(name).note(n, 0, 0, held)
				}}
				if err := hj.Build(ctx, src); err != nil {
					t.Fatalf("build: %v", err)
				}
				ss := hj.spillState
				if ss == nil || len(ss.spilledParts) == 0 {
					t.Fatalf("the build did not spill at %d MiB: this cell measures nothing", mib)
				}
				spilled := len(ss.spilledParts)
				{
					n, held := censusWriters(ss.partBuildWriters)
					ph("build-done").note(n, 0, 0, held)
					if ss.partBuildWriters != nil || n != 0 {
						t.Errorf("after Build, %d build writers are still open (map nil=%v)", n, ss.partBuildWriters == nil)
					}
				}
				probe := hj.Probe()
				replaying := false
				sample := func() {
					bn, bheld := censusWriters(ss.partBuildWriters)
					ss.probeMu.Lock()
					pn, pheld := censusWriters(ss.partProbeWriters)
					ss.probeMu.Unlock()
					readers := 0
					var rheld int64
					if r := probe.spillFlushReader; r != nil && r.r != nil && r.f != nil {
						readers = 1
						rheld = int64(r.r.Size())
					}
					name := "probe"
					if replaying {
						name = "replay"
					}
					ph(name).note(bn, pn, readers, bheld+pheld+rheld)
					if bn > 0 {
						t.Errorf("%s: %d build writers open", name, bn)
					}
					if pn > spilled {
						t.Errorf("%s: %d probe writers for %d spilled partitions", name, pn, spilled)
					}
					if replaying && pn > 0 {
						t.Errorf("replay: %d probe writers still open", pn)
					}
					if readers > 1 {
						t.Errorf("%s: %d replay readers", name, readers)
					}
				}
				nProbe := len(probeIn)
				psrc := &gatedSource{inner: NewBatchSource(probeIn), before: func(i int) {
					if i == nProbe {
						sample()
						replaying = true
						return
					}
					sample()
				}}
				sink := &censusSink{onConsume: func() {
					if replaying {
						sample()
					}
				}}
				pipe := &Pipeline{Source: psrc, Ops: []UnaryOperator{probe}, Sink: sink}
				if err := pipe.Run(ctx); err != nil {
					t.Fatalf("probe: %v", err)
				}
				rows := len(sink.Rows)
				if k.jt == RightSemiJoin {
					if tail := probe.FlushMatched(); tail != nil {
						rows += tail.ActiveLen()
					}
				}
				if rows != k.rows {
					t.Errorf("answered %d rows, want %d", rows, k.rows)
				}
				usedBeforeClose := tr.Used()
				probe.Close()
				hj.Close()
				if fds := lcOpenFDs(sm.SpillDir()); len(fds) != 0 {
					t.Errorf("after Close, %d spill descriptors are open: %v", len(fds), fds)
				}
				if n := spillDirEntries(t, sm.SpillDir()); n != 0 {
					t.Errorf("after Close, %d entries are left in the spill directory", n)
				}
				if u := tr.Used(); u != 0 {
					t.Errorf("after Close, the tracker holds %d bytes", u)
				}
				for _, name := range []string{"arrival", "eviction", "build-done", "probe", "replay"} {
					c := phases[name]
					if c == nil {
						continue
					}
					// charged: nothing charges a spill writer's or reader's
					// buffer in this tree, so the uncharged gap is the held
					// figure.
					t.Logf("CENSUS %dMiB %s %-10s samples=%d build_writers=%d probe_writers=%d readers=%d held=%d charged=0 uncharged_gap=%d spilled=%d tracker_used_before_close=%d",
						mib, k.name, name, c.samples, c.buildW, c.probeW, c.readers, c.held, c.held, spilled, usedBeforeClose)
				}
			})
		}
	}
}
