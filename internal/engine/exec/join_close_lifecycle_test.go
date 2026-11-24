// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A spilled hash join leaves nothing behind on ANY exit path (#1706 line
// "Spill files left after cancel or I/O error", #1605, #1685 line
// "HashJoin.Close"): after the route ends and the owner closes the probe and
// the join, with both objects still referenced,
//
//	(a) no spill file of this join remains on disk,
//	(b) no file descriptor of this join is still open,
//	(c) neither object can reach build batches, index parts, bloom, matched
//	    bitmaps, the grace spill state (open accumulators, per-partition
//	    batches, writers) or probe scratch,
//	(d) the tracker's used and forced census are zero.
//
// Before the fix, Close removed only the files of writers that had already
// been CLOSED, which happens in the first NextFlush. Every route that ends
// before it (cancel or error in the build or the probe, Close before the
// probe) left each open writer's file, descriptor and 64 KiB buffer behind,
// and the grace state stayed reachable from the join even on the normal end.
//
// The I/O failures are real ones from the kernel: a writer's file name is
// predictable (prefix, pid, spillFileSeq), so the test places a symlink to
// /dev/full (ENOSPC on every write) or a directory (EISDIR on create) at the
// names the join will open next, and truncates closed run files to make a
// replay read hit a short read.

// ---- fixtures ----

type lcFixture struct {
	build, probe []*batch.RecordBatch
}

var (
	lcFixtureOnce sync.Once
	lcBig         lcFixture // partitions larger than a writer's 64 KiB buffer
	lcSmall       lcFixture // partitions far smaller than it
)

func lcSchema(side string) []parquet.Column {
	return []parquet.Column{
		{Name: side + "k", Type: parquet.TypeInt64},
		{Name: side + "v", Type: parquet.TypeInt64},
		{Name: side + "pad", Type: parquet.TypeString},
	}
}

func lcGen(side string, n, pad int, key func(i int) int64) []*batch.RecordBatch {
	schema := lcSchema(side)
	padSrc := strings.Repeat("abcdefghijklmnopqrstuvwxyz0123456789", 4)
	var out []*batch.RecordBatch
	for start := 0; start < n; start += batch.DefaultBatchSize {
		m := min(batch.DefaultBatchSize, n-start)
		b := batch.NewRecordBatch(schema, m)
		for j := 0; j < m; j++ {
			i := start + j
			b.Columns[0].Int64Data[j] = key(i)
			b.Columns[1].Int64Data[j] = int64(i)
			b.Columns[2].BytesData.SetString(j, padSrc[i%32:i%32+pad])
		}
		b.Len = m
		out = append(out, b)
	}
	return out
}

func lcFixtures() {
	lcFixtureOnce.Do(func() {
		lcBig = lcFixture{
			build: lcGen("b", 100_000, 48, func(i int) int64 { return int64(i) }),
			probe: lcGen("p", 200_000, 32, func(i int) int64 { return int64((i * 7919) % 200_000) }),
		}
		lcSmall = lcFixture{
			build: lcGen("b", 12_000, 4, func(i int) int64 { return int64(i) }),
			probe: lcGen("p", 12_000, 4, func(i int) int64 { return int64((i * 7919) % 24_000) }),
		}
	})
}

// lcCopy hands out a fresh copy, so nothing the join retains is a fixture
// batch the test itself keeps alive.
func lcCopy(b *batch.RecordBatch) *batch.RecordBatch {
	rows := make([]int, b.Len)
	for j := range rows {
		rows[j] = j
	}
	out := compactBatchForRows(b, rows)
	out.Len = b.Len
	return out
}

// lcSource is safe for the concurrent Next calls of a parallel pipeline's
// workers, so before sees every index exactly once.
type lcSource struct {
	mu      sync.Mutex
	batches []*batch.RecordBatch
	i       int
	before  func(i int)
}

func (s *lcSource) Init(context.Context) error { s.i = 0; return nil }
func (s *lcSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.i >= len(s.batches) {
		return nil, nil
	}
	if s.before != nil {
		s.before(s.i)
	}
	// A real scan stops on cancellation; without this a parallel pipeline
	// whose workers drain the rest of the input before they look at ctx
	// finishes cleanly and the cancel route proves nothing.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b := lcCopy(s.batches[s.i])
	s.i++
	return b, nil
}
func (s *lcSource) Close() error { return nil }

// ---- fault injection through the real file system ----

type lcFaults struct {
	dir   string
	links map[int64][]string // claimed sequence number → symlinks placed for it
	dirs  []string
	// sealedAt, when set, is the last sequence number a writer with the
	// injected prefixes could claim: numbers claimed after it belong to
	// writers of another prefix (the probe's, after a build injection).
	sealedAt int64
}

// seal ends the phase whose writers the injection targets.
func (f *lcFaults) seal() { f.sealedAt = spillFileSeq.Load() }

// inject places, for each of the next n sequence numbers after skip, an
// obstacle at every name a writer with one of prefixes would open: a symlink
// to /dev/full (asDir false) or a directory (asDir true).
func (f *lcFaults) inject(t *testing.T, prefixes []string, skip, n int, asDir bool) {
	t.Helper()
	start := spillFileSeq.Load()
	for s := start + int64(skip) + 1; s <= start+int64(skip+n); s++ {
		for _, pre := range prefixes {
			p := filepath.Join(f.dir, fmt.Sprintf("%s-%d.%d.bin", pre, os.Getpid(), s))
			if asDir {
				if err := os.Mkdir(p, 0o700); err != nil {
					t.Fatal(err)
				}
				f.dirs = append(f.dirs, p)
				continue
			}
			if err := os.Symlink("/dev/full", p); err != nil {
				t.Fatal(err)
			}
			f.links[s] = append(f.links[s], p)
		}
	}
}

func probePrefixes(parts map[int]bool) []string {
	var out []string
	for p := range parts {
		out = append(out, fmt.Sprintf("probe-part-%d", p))
	}
	return out
}

// filesLeft counts what this join left in its spill directory: regular files,
// plus, for every sequence number the join claimed inside an injected range,
// the one symlink it opened there if it is still present (exactly one name
// per claimed number is opened; every placed link of that number still being
// there means the opened one was never removed).
func (f *lcFaults) filesLeft(t *testing.T) (int, []string) {
	t.Helper()
	for _, d := range f.dirs {
		os.Remove(d)
	}
	var names []string
	ents, err := os.ReadDir(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if e.Type().IsRegular() {
			names = append(names, e.Name())
		}
	}
	claimed := spillFileSeq.Load()
	if f.sealedAt > 0 {
		claimed = f.sealedAt
	}
	for s, links := range f.links {
		if s > claimed {
			continue
		}
		remaining := 0
		for _, l := range links {
			if _, err := os.Lstat(l); err == nil {
				remaining++
			}
		}
		if remaining == len(links) {
			names = append(names, fmt.Sprintf("<opened link at seq %d>", s))
		}
	}
	sort.Strings(names)
	return len(names), names
}

// lcOpenFDs lists this process's descriptors that point into dir or at
// /dev/full.
func lcOpenFDs(dir string) []string {
	ents, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range ents {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name()))
		if err != nil {
			continue
		}
		if strings.HasPrefix(target, dir) || target == "/dev/full" {
			out = append(out, target)
		}
	}
	return out
}

// ---- what the operator holds ----

func watchJoinState(w *closeWatch, prefix string, h *HashJoin) {
	watchSlice(w, prefix+"buildBatches", h.buildBatches)
	for i, b := range h.buildBatches {
		if b != nil {
			watchPtr(w, fmt.Sprintf("%sbuildBatches[%d]", prefix, i), b)
		}
	}
	watchSlice(w, prefix+"parts", h.parts)
	for i := range h.parts {
		pt := &h.parts[i]
		watchPtr(w, fmt.Sprintf("%sparts[%d].ints", prefix, i), pt.ints)
		watchPtr(w, fmt.Sprintf("%sparts[%d].strs", prefix, i), pt.strs)
		watchSlice(w, fmt.Sprintf("%sparts[%d].arena", prefix, i), pt.arena)
		watchSlice(w, fmt.Sprintf("%sparts[%d].next", prefix, i), pt.next)
		watchSlice(w, fmt.Sprintf("%sparts[%d].matched", prefix, i), pt.matched)
	}
	watchSlice(w, prefix+"rowMatched", h.rowMatched)
	watchSlice(w, prefix+"bloom", h.bloom)
	watchSlice(w, prefix+"nePairs", h.nePairs)
	watchSlice(w, prefix+"keyBuf", h.keyBuf)
	ss := h.spillState
	if ss == nil {
		return
	}
	watchPtr(w, prefix+"spillState", ss)
	for p, b := range ss.openAccum {
		watchPtr(w, fmt.Sprintf("%sspillState.openAccum[%d]", prefix, p), b)
	}
	for p, bs := range ss.partBuildBatches {
		for i, b := range bs {
			watchPtr(w, fmt.Sprintf("%sspillState.partBuildBatches[%d][%d]", prefix, p, i), b)
		}
	}
	for p, sw := range ss.partBuildWriters {
		watchPtr(w, fmt.Sprintf("%sspillState.partBuildWriters[%d]", prefix, p), sw)
		watchPtr(w, fmt.Sprintf("%sspillState.partBuildWriters[%d].buf", prefix, p), sw.w)
	}
	for p, sw := range ss.partProbeWriters {
		watchPtr(w, fmt.Sprintf("%sspillState.partProbeWriters[%d]", prefix, p), sw)
		watchPtr(w, fmt.Sprintf("%sspillState.partProbeWriters[%d].buf", prefix, p), sw.w)
	}
	watchSlice(w, prefix+"spillState.batchPartID", ss.batchPartID)
	for p, s := range ss.partRowScratch {
		watchSlice(w, fmt.Sprintf("%sspillState.partRowScratch[%d]", prefix, p), s)
	}
}

func watchProbeState(w *closeWatch, prefix string, p *HashJoinProbe) {
	if p == nil {
		return
	}
	watchSlice(w, prefix+"pairsBuf", p.pairsBuf)
	watchSlice(w, prefix+"sortBuf", p.sortBuf)
	watchSlice(w, prefix+"semiSelBuf", p.semiSelBuf)
	watchSlice(w, prefix+"lookupBuf", p.lookupBuf)
	watchSlice(w, prefix+"indexBuf", p.indexBuf)
	watchSlice(w, prefix+"buildIndexBuf", p.buildIndexBuf)
	watchSlice(w, prefix+"keyBuf", p.keyBuf)
	watchPtr(w, prefix+"outPool", p.outPool)
	watchPtr(w, prefix+"largeOutPool", p.largeOutPool)
	watchPtr(w, prefix+"emitBuf.out", p.emitBuf.out)
	watchPtr(w, prefix+"res.in", p.res.in)
	watchPtr(w, prefix+"spillFlushReader", p.spillFlushReader)
	if p.spillFlushTmpJoin != nil {
		watchPtr(w, prefix+"spillFlushTmpJoin", p.spillFlushTmpJoin)
		watchJoinState(w, prefix+"spillFlushTmpJoin.", p.spillFlushTmpJoin)
	}
	watchPtr(w, prefix+"spillFlushTmpProbe", p.spillFlushTmpProbe)
}

// requireUnreachable collects until every watched structure is gone (bounded
// at about two seconds) while keep stays referenced, and fails naming the
// survivors.
func (w *closeWatch) requireUnreachable(t *testing.T, what string, keep ...any) {
	t.Helper()
	w.mu.Lock()
	total := len(w.names)
	w.mu.Unlock()
	if total == 0 {
		t.Fatalf("watched nothing: the route built no %s state", what)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		runtime.GC()
		w.mu.Lock()
		done := len(w.collected) == total
		w.mu.Unlock()
		if done || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	runtime.KeepAlive(keep)
	w.mu.Lock()
	defer w.mu.Unlock()
	var alive []string
	for i, n := range w.names {
		if !w.collected[i] {
			alive = append(alive, n)
		}
	}
	if n := len(alive); n > 0 {
		sort.Strings(alive)
		if n > 12 {
			alive = append(alive[:12:12], fmt.Sprintf("... %d more", n-12))
		}
		t.Errorf("after Close, %d of %d %s structures are still reachable from the operator: %v",
			n, total, what, alive)
	}
}

func lcHeap() int64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return int64(ms.HeapAlloc)
}

// ---- drivers ----

var errLCStopped = errors.New("route stopped the probe")

func lcProbe(ctx context.Context, p *HashJoinProbe, batches []*batch.RecordBatch, stop func(i int) bool) error {
	for i, b := range batches {
		if stop != nil && stop(i) {
			return errLCStopped
		}
		if _, err := p.Execute(ctx, lcCopy(b)); err != nil {
			return err
		}
		for p.HasPendingOutput() {
			if _, err := p.NextOutput(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

func lcFlush(ctx context.Context, p *HashJoinProbe, before func(call int)) (int, error) {
	n := 0
	for p.HasPendingFlush() {
		if before != nil {
			before(n)
		}
		out, err := p.NextFlush(ctx)
		n++
		if err != nil {
			return n, err
		}
		if out == nil {
			break
		}
	}
	return n, nil
}

type lcEnv struct {
	t       *testing.T
	hj      *HashJoin
	tracker *memory.Tracker
	faults  *lcFaults
	fix     lcFixture
	ctx     context.Context
	cancel  context.CancelFunc
}

func (e *lcEnv) build(before func(i int)) error {
	return e.hj.Build(e.ctx, &lcSource{batches: e.fix.build, before: before})
}

func (e *lcEnv) newProbe() *HashJoinProbe {
	p := e.hj.Probe()
	p.EnableBoundedOutput()
	if err := p.Init(e.ctx); err != nil {
		e.t.Fatalf("probe init: %v", err)
	}
	return p
}

func (e *lcEnv) spilled() map[int]bool {
	if e.hj.spillState == nil || len(e.hj.spillState.spilledParts) == 0 {
		e.t.Fatal("the build spilled nothing: this route never reaches a spill writer")
	}
	return e.hj.spillState.spilledParts
}

// truncateReplayFiles shortens the run files of every spilled partition the
// replay has not reached yet (the third onward: the first is open and the
// second may be prefetching), so the replay meets a short read.
func (e *lcEnv) truncateReplayFiles(p *HashJoinProbe, build bool) {
	ids := p.spillFlushPartIDs
	if len(ids) < 3 {
		e.t.Fatalf("only %d spilled partitions; the read-error route needs 3", len(ids))
	}
	n := 0
	for _, id := range ids[2:] {
		files := e.hj.spillState.partProbeFiles[id]
		if build {
			files = e.hj.spillState.partBuildFiles[id]
		}
		for _, f := range files {
			if err := os.Truncate(f, 6); err != nil {
				e.t.Fatal(err)
			}
			n++
		}
	}
	if n == 0 {
		e.t.Fatal("no run file to truncate")
	}
}

type lcRoute struct {
	name    string
	small   bool
	jt      JoinType
	budget  int64
	wantErr string // substring the route's error must carry; "" = must succeed
	run     func(e *lcEnv) (*HashJoinProbe, error)
	// joinFirst closes the join before the probe: an owner whose cleanup
	// runs before its pipeline's Close, with the replay's build prefetch
	// possibly still reading.
	joinFirst bool
}

func lcRoutes() []lcRoute {
	const big = 2 << 20
	const small = 256 << 10
	return []lcRoute{
		{name: "normal-end", jt: FullOuterJoin, budget: big, run: func(e *lcEnv) (*HashJoinProbe, error) {
			if err := e.build(nil); err != nil {
				return nil, err
			}
			e.spilled()
			p := e.newProbe()
			if err := lcProbe(e.ctx, p, e.fix.probe, nil); err != nil {
				return p, err
			}
			_, err := lcFlush(e.ctx, p, nil)
			return p, err
		}},
		{name: "close-before-probe", jt: FullOuterJoin, budget: big, run: func(e *lcEnv) (*HashJoinProbe, error) {
			err := e.build(nil)
			e.spilled()
			return nil, err
		}},
		{name: "cancel-mid-build", jt: InnerJoin, budget: big, wantErr: "cancel", run: func(e *lcEnv) (*HashJoinProbe, error) {
			return nil, e.build(func(i int) {
				if i == 30 {
					e.spilled()
					e.cancel()
				}
			})
		}},
		{name: "cancel-at-first-eviction", jt: InnerJoin, budget: big, wantErr: "cancel", run: func(e *lcEnv) (*HashJoinProbe, error) {
			before := JoinPartitionsEvicted.Load()
			return nil, e.build(func(int) {
				if JoinPartitionsEvicted.Load() > before {
					e.cancel()
				}
			})
		}},
		{name: "cancel-mid-probe", jt: FullOuterJoin, budget: big, wantErr: "cancel", run: func(e *lcEnv) (*HashJoinProbe, error) {
			if err := e.build(nil); err != nil {
				return nil, err
			}
			e.spilled()
			p := e.newProbe()
			err := lcProbe(e.ctx, p, e.fix.probe, func(i int) bool {
				if i == len(e.fix.probe)/2 {
					if len(e.hj.spillState.partProbeWriters) == 0 {
						e.t.Fatal("no probe writer open at the cancel point")
					}
					e.cancel()
					return true
				}
				return false
			})
			if errors.Is(err, errLCStopped) {
				err = e.ctx.Err()
			}
			return p, err
		}},
		{name: "cancel-mid-replay", jt: FullOuterJoin, budget: big, wantErr: "canceled", run: func(e *lcEnv) (*HashJoinProbe, error) {
			if err := e.build(nil); err != nil {
				return nil, err
			}
			e.spilled()
			p := e.newProbe()
			if err := lcProbe(e.ctx, p, e.fix.probe, nil); err != nil {
				return p, err
			}
			_, err := lcFlush(e.ctx, p, func(call int) {
				if call == 3 {
					e.cancel()
				}
			})
			return p, err
		}},
		// The same, with the join closed before the probe. Under -race this
		// is the route on which the replay's build prefetch, a goroutine the
		// probe does not join until its own Close, read the join's spill
		// bookkeeping while Close cleared it.
		{name: "cancel-mid-replay-join-closed-first", jt: FullOuterJoin, budget: big, wantErr: "canceled", joinFirst: true, run: func(e *lcEnv) (*HashJoinProbe, error) {
			if err := e.build(nil); err != nil {
				return nil, err
			}
			e.spilled()
			p := e.newProbe()
			if err := lcProbe(e.ctx, p, e.fix.probe, nil); err != nil {
				return p, err
			}
			_, err := lcFlush(e.ctx, p, func(call int) {
				if call == 3 {
					e.cancel()
				}
			})
			return p, err
		}},
		// A writer's batches go through a 64 KiB bufio.Writer whose write
		// errors are sticky and are returned by the Flush in close(): a
		// partition larger than the buffer meets ENOSPC DURING the build, and
		// the join learns of it when the first NextFlush closes the writers.
		{name: "build-write-enospc", jt: FullOuterJoin, budget: big, wantErr: "no space left on device", run: func(e *lcEnv) (*HashJoinProbe, error) {
			e.faults.inject(e.t, []string{"build-spill"}, 2, 128, false)
			if err := e.build(nil); err != nil {
				return nil, err
			}
			e.faults.seal()
			e.spilled()
			p := e.newProbe()
			if err := lcProbe(e.ctx, p, e.fix.probe, nil); err != nil {
				return p, err
			}
			_, err := lcFlush(e.ctx, p, nil)
			if err != nil && !strings.Contains(err.Error(), "closing build writers") {
				e.t.Errorf("expected the failure at the build writers' close, got %v", err)
			}
			return p, err
		}},
		{name: "build-flush-enospc", small: true, jt: InnerJoin, budget: small, wantErr: "no space left on device", run: func(e *lcEnv) (*HashJoinProbe, error) {
			e.faults.inject(e.t, []string{"build-spill"}, 2, 128, false)
			if err := e.build(nil); err != nil {
				return nil, fmt.Errorf("the build itself failed (partitions must stay under the writer buffer): %w", err)
			}
			e.faults.seal()
			e.spilled()
			p := e.newProbe()
			if err := lcProbe(e.ctx, p, e.fix.probe, nil); err != nil {
				return p, err
			}
			_, err := lcFlush(e.ctx, p, nil)
			if err != nil && !strings.Contains(err.Error(), "closing build writers") {
				e.t.Errorf("expected the failure at the build writers' close, got %v", err)
			}
			return p, err
		}},
		{name: "probe-write-enospc", jt: FullOuterJoin, budget: big, wantErr: "no space left on device", run: func(e *lcEnv) (*HashJoinProbe, error) {
			if err := e.build(nil); err != nil {
				return nil, err
			}
			e.faults.inject(e.t, probePrefixes(e.spilled()), 2, 128, false)
			p := e.newProbe()
			if err := lcProbe(e.ctx, p, e.fix.probe, nil); err != nil {
				return p, err
			}
			_, err := lcFlush(e.ctx, p, nil)
			if err != nil && !strings.Contains(err.Error(), "closing probe writers") {
				e.t.Errorf("expected the failure at the probe writers' close, got %v", err)
			}
			return p, err
		}},
		{name: "probe-flush-enospc", small: true, jt: FullOuterJoin, budget: small, wantErr: "no space left on device", run: func(e *lcEnv) (*HashJoinProbe, error) {
			if err := e.build(nil); err != nil {
				return nil, err
			}
			e.faults.inject(e.t, probePrefixes(e.spilled()), 2, 128, false)
			p := e.newProbe()
			if err := lcProbe(e.ctx, p, e.fix.probe, nil); err != nil {
				return p, fmt.Errorf("the probe itself failed (partitions must stay under the writer buffer): %w", err)
			}
			_, err := lcFlush(e.ctx, p, nil)
			if err != nil && !strings.Contains(err.Error(), "closing probe writers") {
				e.t.Errorf("expected the failure at the probe writers' close, got %v", err)
			}
			return p, err
		}},
		{name: "build-read-error", jt: FullOuterJoin, budget: big, wantErr: "reading build spill", run: func(e *lcEnv) (*HashJoinProbe, error) {
			if err := e.build(nil); err != nil {
				return nil, err
			}
			e.spilled()
			p := e.newProbe()
			if err := lcProbe(e.ctx, p, e.fix.probe, nil); err != nil {
				return p, err
			}
			_, err := lcFlush(e.ctx, p, func(call int) {
				if call == 1 {
					e.truncateReplayFiles(p, true)
				}
			})
			return p, err
		}},
		{name: "probe-read-error", jt: FullOuterJoin, budget: big, wantErr: "reading probe spill", run: func(e *lcEnv) (*HashJoinProbe, error) {
			if err := e.build(nil); err != nil {
				return nil, err
			}
			e.spilled()
			p := e.newProbe()
			if err := lcProbe(e.ctx, p, e.fix.probe, nil); err != nil {
				return p, err
			}
			_, err := lcFlush(e.ctx, p, func(call int) {
				if call == 1 {
					e.truncateReplayFiles(p, false)
				}
			})
			return p, err
		}},
		{name: "scratch-create-fails", jt: InnerJoin, budget: big, wantErr: "creating spill file", run: func(e *lcEnv) (*HashJoinProbe, error) {
			e.faults.inject(e.t, []string{"build-spill"}, 3, 128, true)
			return nil, e.build(nil)
		}},
		{name: "panic-at-query-boundary", jt: FullOuterJoin, budget: big, wantErr: "lc sink panic", run: func(e *lcEnv) (*HashJoinProbe, error) {
			if err := e.build(nil); err != nil {
				return nil, err
			}
			e.spilled()
			p := e.hj.Probe()
			pipe := &Pipeline{
				Source: &lcSource{batches: e.fix.probe},
				Ops:    []UnaryOperator{p},
				Sink:   &lcPanicSink{after: 20},
			}
			err := pipe.Run(e.ctx)
			pipe.Close()
			return p, err
		}},
	}
}

// lcPanicSink panics on its after-th batch: an unexpected panic the
// pipeline's query boundary turns into an error (#511).
type lcPanicSink struct {
	after, n int
}

func (s *lcPanicSink) Init(context.Context) error { return nil }
func (s *lcPanicSink) Consume(context.Context, *batch.RecordBatch) error {
	s.n++
	if s.n == s.after {
		panic("lc sink panic")
	}
	return nil
}
func (s *lcPanicSink) Finalize(context.Context) error { return nil }
func (s *lcPanicSink) Close() error                   { return nil }

func runLCRoute(t *testing.T, r lcRoute) {
	lcFixtures()
	fix := lcBig
	if r.small {
		fix = lcSmall
	}
	tracker := memory.NewTracker("lc", r.budget)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{"pk"}
	bkeys := []string{"bk"}
	hj := NewHashJoin(r.jt, keys, bkeys)
	hj.MemTracker, hj.Spill = tracker, sm
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e := &lcEnv{t: t, hj: hj, tracker: tracker, fix: fix, ctx: ctx, cancel: cancel,
		faults: &lcFaults{dir: sm.SpillDir(), links: map[int64][]string{}}}

	heapBefore := lcHeap()
	probe, runErr := r.run(e)
	switch {
	case r.wantErr == "" && runErr != nil:
		t.Fatalf("route failed: %v", runErr)
	case r.wantErr != "" && runErr == nil:
		t.Fatalf("route did not fail; it was meant to end on %q and proves nothing", r.wantErr)
	case r.wantErr != "" && !strings.Contains(runErr.Error(), r.wantErr):
		t.Fatalf("route failed for another reason than the one it tests (%q): %v", r.wantErr, runErr)
	}

	w := newCloseWatch()
	watchJoinState(w, "join.", hj)
	watchProbeState(w, "probe.", probe)

	// The production order: the pipeline closes its operators, then the
	// plan's Cleanup closes the join (wadjet.go defers them that way).
	if r.joinFirst {
		if err := hj.Close(); err != nil {
			t.Errorf("join Close: %v", err)
		}
	}
	if probe != nil {
		if err := probe.Close(); err != nil {
			t.Errorf("probe Close: %v", err)
		}
	}
	if err := hj.Close(); err != nil {
		t.Errorf("join Close: %v", err)
	}
	if err := hj.Close(); err != nil {
		t.Errorf("second join Close: %v", err)
	}

	nFiles, names := e.faults.filesLeft(t)
	fds := lcOpenFDs(sm.SpillDir())
	retained := lcHeap() - heapBefore
	t.Logf("route=%s err=%v filesLeft=%d openFDs=%d retainedAfterClose=%d", r.name, runErr, nFiles, len(fds), retained)

	if nFiles != 0 {
		if len(names) > 6 {
			names = append(names[:6:6], fmt.Sprintf("... %d more", len(names)-6))
		}
		t.Errorf("after Close, %d spill files of this join remain on disk: %v", nFiles, names)
	}
	if len(fds) != 0 {
		t.Errorf("after Close, %d descriptors of this join are still open: %v", len(fds), fds)
	}
	if used := tracker.Used(); used != 0 {
		t.Errorf("after Close the tracker holds %d bytes", used)
	}
	if total, top, n := tracker.ForcedBytes(); total != 0 {
		t.Errorf("after Close the forced census reports %d bytes (largest %d by %q)", total, n, top)
	}
	w.requireUnreachable(t, "join", hj, probe)
}

func TestHashJoinCloseLeavesNothingOnEveryRoute(t *testing.T) {
	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skip("needs /dev/full for the ENOSPC routes")
	}
	for _, r := range lcRoutes() {
		t.Run(r.name, func(t *testing.T) { runLCRoute(t, r) })
	}
}

// The same cancel route through a PARALLEL pipeline: every probe clone shares
// the join's spill state, and the pipeline's Close closes the clones before
// the plan closes the join.
func TestHashJoinCloseAfterCancelledParallelProbe(t *testing.T) {
	lcFixtures()
	tracker := memory.NewTracker("lc", 2<<20)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	hj := NewHashJoin(FullOuterJoin, []string{"pk"}, []string{"bk"})
	hj.MemTracker, hj.Spill = tracker, sm
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := hj.Build(ctx, &lcSource{batches: lcBig.build}); err != nil {
		t.Fatal(err)
	}
	if hj.spillState == nil || len(hj.spillState.spilledParts) == 0 {
		t.Fatal("the build spilled nothing")
	}
	p := hj.Probe()
	pipe := &Pipeline{
		Source: &lcSource{batches: lcBig.probe, before: func(i int) {
			if i == 40 {
				cancel()
			}
		}},
		Ops:     []UnaryOperator{p},
		Sink:    &CountSinkForTest{},
		Workers: 4,
	}
	runErr := pipe.Run(ctx)
	if runErr == nil {
		t.Fatal("the cancelled pipeline returned no error")
	}
	w := newCloseWatch()
	watchJoinState(w, "join.", hj)
	watchProbeState(w, "probe.", p)
	pipe.Close()
	hj.Close()
	faults := &lcFaults{dir: sm.SpillDir(), links: map[int64][]string{}}
	if n, names := faults.filesLeft(t); n != 0 {
		t.Errorf("after Close, %d spill files of this join remain on disk: %v", n, names)
	}
	if fds := lcOpenFDs(sm.SpillDir()); len(fds) != 0 {
		t.Errorf("after Close, %d descriptors are still open: %v", len(fds), fds)
	}
	if used := tracker.Used(); used != 0 {
		t.Errorf("after Close the tracker holds %d bytes", used)
	}
	w.requireUnreachable(t, "join", hj, p, pipe)
}

// CountSinkForTest counts rows and keeps nothing.
type CountSinkForTest struct {
	mu   sync.Mutex
	rows int
}

func (s *CountSinkForTest) Init(context.Context) error { return nil }
func (s *CountSinkForTest) Consume(_ context.Context, b *batch.RecordBatch) error {
	s.mu.Lock()
	s.rows += b.ActiveLen()
	s.mu.Unlock()
	return nil
}
func (s *CountSinkForTest) Finalize(context.Context) error { return nil }
func (s *CountSinkForTest) Close() error                   { return nil }

// Calls after Close return an explicit error instead of answering from state
// that is gone: before the fix a probe of a closed join returned no matches
// (a silent wrong answer) and a second Build re-registered and re-charged.
func TestHashJoinUseAfterClose(t *testing.T) {
	lcFixtures()
	for _, jt := range []JoinType{InnerJoin, FullOuterJoin, CrossJoin} {
		t.Run(fmt.Sprint(jt), func(t *testing.T) {
			tracker := memory.NewTracker("lc", 64<<20)
			sm, err := memory.NewSpillManager(t.TempDir(), tracker)
			if err != nil {
				t.Fatal(err)
			}
			var hj *HashJoin
			build := lcSmall.build[:2]
			if jt == CrossJoin {
				hj = NewHashJoin(CrossJoin, nil, nil)
				build = lcSmall.build[:1]
			} else {
				hj = NewHashJoin(jt, []string{"pk"}, []string{"bk"})
			}
			hj.MemTracker, hj.Spill = tracker, sm
			ctx := context.Background()
			if err := hj.Build(ctx, &lcSource{batches: build}); err != nil {
				t.Fatal(err)
			}
			p := hj.Probe()
			if err := p.Init(ctx); err != nil {
				t.Fatal(err)
			}
			in := lcCopy(lcSmall.probe[0])
			in.Len = 8
			out, err := p.Execute(ctx, in)
			if err != nil || out == nil || out.ActiveLen() == 0 {
				t.Fatalf("before Close the probe must answer: out=%v err=%v", out, err)
			}
			for p.HasPendingOutput() {
				if _, err := p.NextOutput(ctx); err != nil {
					t.Fatal(err)
				}
			}
			w := newCloseWatch()
			watchJoinState(w, "join.", hj)
			if err := hj.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := p.Execute(ctx, lcCopy(lcSmall.probe[0])); !errors.Is(err, errHashJoinClosed) {
				t.Errorf("Execute after the join's Close: err=%v, want errHashJoinClosed", err)
			}
			if _, err := p.NextFlush(ctx); !errors.Is(err, errHashJoinClosed) {
				t.Errorf("NextFlush after the join's Close: err=%v, want errHashJoinClosed", err)
			}
			if _, err := p.NextOutput(ctx); !errors.Is(err, errHashJoinClosed) {
				t.Errorf("NextOutput after the join's Close: err=%v, want errHashJoinClosed", err)
			}
			if p.HasPendingFlush() || p.HasPendingOutput() {
				t.Error("a probe of a closed join reports pending work")
			}
			if err := hj.Probe().Init(ctx); !errors.Is(err, errHashJoinClosed) {
				t.Errorf("Probe().Init after Close: err=%v, want errHashJoinClosed", err)
			}
			if err := hj.Build(ctx, &lcSource{batches: build}); !errors.Is(err, errHashJoinClosed) {
				t.Errorf("Build after Close: err=%v, want errHashJoinClosed", err)
			}
			if n, err := hj.SpillSome(1 << 30); n != 0 || err != nil {
				t.Errorf("SpillSome after Close = (%d, %v), want (0, nil)", n, err)
			}
			if err := hj.Close(); err != nil {
				t.Errorf("second Close: %v", err)
			}
			if used := tracker.Used(); used != 0 {
				t.Errorf("after Close the tracker holds %d bytes", used)
			}
			p.Close()
			w.requireUnreachable(t, "join", hj, p)
		})
	}
}

// The distinct-pair semi/anti build keeps its pairs outside buildBatches.
func TestHashJoinCloseDropsDistinctPairState(t *testing.T) {
	buildSchema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}, {Name: "v", Type: parquet.TypeInt64}}
	probeSchema := []parquet.Column{{Name: "k", Type: parquet.TypeInt64}, {Name: "pv", Type: parquet.TypeInt64}}
	var rows []map[string]any
	for i := 0; i < 5000; i++ {
		rows = append(rows, map[string]any{"k": int64(i % 700), "v": int64(i % 13)})
	}
	hj := NewHashJoin(SemiJoin, []string{"k"}, []string{"k"})
	hj.SemiAntiFilter = BuildNETestFilter("pv", "v")
	hj.SemiAntiNEProbeCol, hj.SemiAntiNEBuildCol = "pv", "v"
	hj.ProbeSchemaHint, hj.BuildSchemaHint = probeSchema, buildSchema
	if err := hj.Build(context.Background(), NewSliceSource(buildSchema, rows)); err != nil {
		t.Fatal(err)
	}
	if !hj.neActive || len(hj.nePairs) == 0 {
		t.Fatal("the distinct-pair path did not activate")
	}
	w := newCloseWatch()
	watchJoinState(w, "join.", hj)
	if err := hj.Close(); err != nil {
		t.Fatal(err)
	}
	w.requireUnreachable(t, "join", hj)
}
