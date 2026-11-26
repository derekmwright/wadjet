// SPDX-License-Identifier: MIT

package exec

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// #1706, line 1706o: the failures of a grace join's OWN spill I/O — its own
// evictions (at an arrival under pressure, on an arrival reservation refusal,
// an arrival into an already-spilled partition, the build tail's close of its
// runs) and its probe side (a probe run that cannot be created or fails
// mid-write, a replay reader that cannot open or fails mid-read, the prefetch
// of the next partition failing, the replay cancelled) — on every join kind.
// Each cell must end LOUD (an error from Build or from the probe) or RIGHT
// (PostgreSQL 17.11's answer); never WRONG.
//
// The fixture is staleWide's data (staleMain with a 400-byte pad, so a
// partition's run overflows the writer's 64 KiB buffer and an eviction's own
// writes reach the file), joined alone under a 3 MiB budget: the build evicts
// both at arrivals under pressure and on reservation refusals, and probe rows
// reach the spilled partitions. PostgreSQL's answer is staleReliefPG.

const soloBudget = 3 << 20

// soloFault is one fault of the join's own spill I/O.
type soloFault struct {
	name string
	// The writer seam: fail stage on runs whose name has prefix, when the
	// failing call's stack contains at and not notAt. once: the first such
	// call only (a transient fault).
	prefix    string
	stage     spillFaultStage
	at, notAt string
	once      bool
	// fs, instead of the seam: a file-system fault on the closed runs the
	// replay reads (see runSoloFault).
	fs string
}

// soloEntries records which eviction entry created each build run.
type soloEntries struct {
	mu   sync.Mutex
	seen map[string]int
}

func evictionEntry(stack string) string {
	switch {
	case strings.Contains(stack, "spillUntilCanReserve"):
		return "reserve"
	case strings.Contains(stack, ".SpillSome("):
		return "relief"
	case strings.Contains(stack, "partitionAndIndexBatch"):
		return "spilled-arrival"
	case strings.Contains(stack, "absorbArrivalBatch"):
		return "pressure"
	}
	return "other"
}

func callStack() string {
	buf := make([]byte, 16<<10)
	return string(buf[:runtime.Stack(buf, false)])
}

type soloOutcome struct {
	digest  staleDigest
	err     error
	used    int64 // tracker after Close
	files   int   // spill directory entries after Close
	fired   bool  // the fault was taken
	entries map[string]int
	spilled int // partitions spilled at the end of the build
}

// runSoloFault builds, probes and drains one join of kind k with fault f.
func runSoloFault(t *testing.T, k staleKind, f soloFault) soloOutcome {
	t.Helper()
	var out soloOutcome
	dir := t.TempDir()
	tr := memory.NewTracker("solo", soloBudget)
	sm, err := memory.NewSpillManager(dir, tr)
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Cleanup()

	ent := &soloEntries{seen: map[string]int{}}
	var fired bool
	var mu sync.Mutex
	hook := func(stage spillFaultStage, path string) error {
		base := filepath.Base(path)
		if stage == spillFaultCreate && strings.HasPrefix(base, "build-spill-") {
			ent.mu.Lock()
			ent.seen[evictionEntry(callStack())]++
			ent.mu.Unlock()
		}
		if f.prefix == "" || stage != f.stage || !strings.HasPrefix(base, f.prefix) {
			return nil
		}
		if f.at != "" || f.notAt != "" {
			st := callStack()
			if (f.at != "" && !strings.Contains(st, f.at)) || (f.notAt != "" && strings.Contains(st, f.notAt)) {
				return nil
			}
		}
		mu.Lock()
		defer mu.Unlock()
		if f.once && fired {
			return nil
		}
		fired = true
		return fmt.Errorf("%w (%s)", errInjectedSpill, f.name)
	}
	spillWriterFault.Store(&hook)
	defer spillWriterFault.Store(nil)

	hj := newStaleJoin(k)
	hj.Spill, hj.MemTracker = sm, tr
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finish := func() soloOutcome {
		hj.Close()
		out.used = tr.Used()
		out.files = spillDirEntries(t, sm.SpillDir())
		mu.Lock()
		out.fired = out.fired || fired
		mu.Unlock()
		ent.mu.Lock()
		out.entries = ent.seen
		ent.mu.Unlock()
		return out
	}

	if err := hj.Build(ctx, NewSliceSource(staleBuildSchema, staleWide().buildRows())); err != nil {
		out.err = err
		return finish()
	}
	hj.mu.Lock()
	if hj.spillState != nil {
		out.spilled = len(hj.spillState.spilledParts)
	}
	hj.mu.Unlock()

	probe := hj.Probe()
	ss := hj.spillState
	allRuns := func(m map[int][]string, skip int) []string {
		var ps []string
		for p, files := range m {
			if p != skip {
				ps = append(ps, files...)
			}
		}
		return ps
	}
	breakRuns := func(paths []string, truncate bool) {
		for _, p := range paths {
			if truncate {
				st, err := os.Stat(p)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Truncate(p, st.Size()/2); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Remove(p); err != nil {
				t.Fatal(err)
			}
			out.fired = true
		}
	}
	nProbe := (staleProbeN + batch.DefaultBatchSize - 1) / batch.DefaultBatchSize
	src := &gatedSource{inner: NewSliceSource(staleProbeSchema, staleProbeRows()), before: func(i int) {
		// The probe source's last Next: the build runs are complete and
		// closed and nothing has read them yet (the first prefetch starts in
		// the first NextFlush).
		if i != nProbe {
			return
		}
		switch f.fs {
		case "build-run-missing":
			breakRuns(allRuns(ss.partBuildFiles, -1), false)
		case "build-run-truncated":
			breakRuns(allRuns(ss.partBuildFiles, -1), true)
		}
	}}
	replays := 0
	restoreReplay := SetSpillReplayHook(func() {
		replays++
		if replays != 1 {
			return
		}
		// The first partition's build is prefetched (or in flight); the
		// next one's prefetch starts after this hook, and no probe run has
		// been opened.
		first := probe.spillFlushPartIDs[0]
		switch f.fs {
		case "next-prefetch-fails":
			breakRuns(allRuns(ss.partBuildFiles, first), true)
		case "probe-run-missing":
			breakRuns(allRuns(ss.partProbeFiles, -1), false)
		case "probe-run-truncated":
			breakRuns(allRuns(ss.partProbeFiles, -1), true)
		case "replay-cancel":
			cancel()
			out.fired = true
		}
	})
	defer restoreReplay()

	sink := &CollectSink{}
	pipe := &Pipeline{Source: src, Ops: []UnaryOperator{probe}, Sink: sink}
	if err := pipe.Run(ctx); err != nil {
		out.err = err
		probe.Close()
		return finish()
	}
	rows := sink.Rows
	var tail *batch.RecordBatch
	switch hj.JoinType {
	case RightSemiJoin:
		tail = probe.FlushMatched()
	case RightAntiJoin:
		tail = probe.FlushAntiMatched()
	}
	if tail != nil && tail.ActiveLen() > 0 {
		ts := &CollectSink{}
		_ = ts.Init(ctx)
		_ = ts.Consume(ctx, tail)
		_ = ts.Finalize(ctx)
		rows = append(rows, ts.Rows...)
	}
	probe.Close()
	out.digest = digestRows(rows)
	return finish()
}

var soloFaults = []soloFault{
	// The build's own evictions.
	{name: "pressure-create", prefix: "build-spill-", stage: spillFaultCreate, at: "absorbArrivalBatch", notAt: "spillUntilCanReserve", once: true},
	{name: "pressure-write", prefix: "build-spill-", stage: spillFaultWrite, at: "spillOneInMemoryPartition", notAt: "spillUntilCanReserve", once: true},
	{name: "pressure-batch", prefix: "build-spill-", stage: spillFaultBatch, at: "spillOneInMemoryPartition", notAt: "spillUntilCanReserve", once: true},
	{name: "reserve-create", prefix: "build-spill-", stage: spillFaultCreate, at: "spillUntilCanReserve", once: true},
	{name: "reserve-write", prefix: "build-spill-", stage: spillFaultWrite, at: "spillUntilCanReserve", once: true},
	{name: "reserve-batch", prefix: "build-spill-", stage: spillFaultBatch, at: "spillUntilCanReserve", once: true},
	{name: "spilled-arrival-write", prefix: "build-spill-", stage: spillFaultWrite, at: "partitionAndIndexBatch", once: true},
	{name: "spilled-arrival-batch", prefix: "build-spill-", stage: spillFaultBatch, at: "partitionAndIndexBatch", once: true},
	// The build tail's close of its runs.
	{name: "tail-flush", prefix: "build-spill-", stage: spillFaultFlush, once: true},
	{name: "tail-seek", prefix: "build-spill-", stage: spillFaultSeek, once: true},
	{name: "tail-count", prefix: "build-spill-", stage: spillFaultCount, once: true},
	{name: "tail-close", prefix: "build-spill-", stage: spillFaultClose, once: true},
	// The probe's runs.
	{name: "probe-create", prefix: "probe-part-", stage: spillFaultCreate, once: true},
	{name: "probe-batch", prefix: "probe-part-", stage: spillFaultBatch, once: true},
	{name: "probe-flush", prefix: "probe-part-", stage: spillFaultFlush, once: true},
	{name: "probe-count", prefix: "probe-part-", stage: spillFaultCount, once: true},
	// The replay's readers.
	{name: "build-run-missing", fs: "build-run-missing"},
	{name: "build-run-truncated", fs: "build-run-truncated"},
	{name: "next-prefetch-fails", fs: "next-prefetch-fails"},
	{name: "probe-run-missing", fs: "probe-run-missing"},
	{name: "probe-run-truncated", fs: "probe-run-truncated"},
	{name: "replay-cancel", fs: "replay-cancel"},
}

// TestJoinOwnSpillFailureIsLoud runs every fault on every join kind.
func TestJoinOwnSpillFailureIsLoud(t *testing.T) {
	// The fixture must engage what the faults target: without a fault the
	// build evicts on both entries, spills partitions, and answers
	// PostgreSQL's answer.
	for _, k := range staleKinds {
		out := runSoloFault(t, k, soloFault{name: "none"})
		if out.err != nil || out.digest != staleReliefPG[k.name] {
			t.Fatalf("fixture %s without a fault: %v err=%v; PostgreSQL 17.11: %v", k.name, out.digest, out.err, staleReliefPG[k.name])
		}
		if out.entries["pressure"] == 0 || out.entries["reserve"] == 0 || out.spilled == 0 {
			t.Fatalf("fixture %s does not engage both eviction entries: runs created by %v, %d partitions spilled", k.name, out.entries, out.spilled)
		}
	}
	for _, f := range soloFaults {
		for _, k := range staleKinds {
			t.Run(f.name+"/"+k.name, func(t *testing.T) {
				out := runSoloFault(t, k, f)
				verdict := "RIGHT"
				switch {
				case out.err != nil:
					verdict = "LOUD"
				case out.digest != staleReliefPG[k.name]:
					verdict = "WRONG"
				}
				staleLogMu.Lock()
				t.Logf("SOLO %s/%s verdict=%s fired=%v answer{%v} err=%v runs_by=%v used_after_close=%d files_after_close=%d",
					f.name, k.name, verdict, out.fired, out.digest, out.err, out.entries, out.used, out.files)
				staleLogMu.Unlock()
				if !out.fired {
					t.Fatalf("schedule not reached: the %s fault was never taken", f.name)
				}
				if verdict == "WRONG" {
					t.Errorf("answered %v after %s; PostgreSQL 17.11: %v", out.digest, f.name, staleReliefPG[k.name])
				}
				if out.used != 0 {
					t.Errorf("tracker holds %d bytes after Close", out.used)
				}
				if out.files != 0 {
					t.Errorf("%d entries left in the spill directory after Close", out.files)
				}
			})
		}
	}
}
