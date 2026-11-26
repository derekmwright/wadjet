// SPDX-License-Identifier: MIT

package exec

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// #1706, line 1706o: a relief eviction that ANOTHER join drives into a victim
// (RequestRelief or the drift backstop calling the victim's SpillSome while
// the victim is still building) can fail. Its error goes to the caller of
// SpillSome: RequestRelief returns it to the requester, the drift backstop
// discards it. Before the fix, spillOneInMemoryPartition marked the partition
// spilled before its run existed, so a run that could not be created left a
// partition marked spilled with its batches resident and never written, and
// the victim answered without them on every join kind (inner 31516 rows
// against PostgreSQL's 32000).
//
// The two rules the fix holds:
//   - nothing is marked before the fallible step that makes it true: the run
//     is created first, so a failed create changes nothing and the victim
//     still answers PostgreSQL's answer;
//   - from the mark on, a failure is the victim's sticky error (the writer's
//     and the join's), so a write, flush, seek, count-write or close failure
//     ends the victim's statement loudly, never with an answer.

// staleFault is one fault placed on the run the held relief call's eviction
// opens.
type staleFault struct {
	name string
	// How the fault is made:
	//   asDir:   a directory at the run's name (os.Create fails, EISDIR);
	//   devFull: a symlink to /dev/full at the run's name (every write that
	//            reaches the file fails with ENOSPC);
	//   stage:   otherwise, the writer's test-only fault seam fails this
	//            step of the first build run created while the fault is
	//            armed, for as long as the test runs.
	asDir, devFull bool
	stage          spillFaultStage
	// nth, for a seam write fault: the first nth-1 writes that reach the
	// file succeed, the nth and every later one fail.
	nth int
	// once: the step fails the first time only (a transient failure: what
	// follows depends only on what the writer and the join remember).
	once   bool
	fired  bool
	placed []string
	writes int

	mu      sync.Mutex
	arming  bool
	target  string
	restore func()
}

var errInjectedSpill = errors.New("injected spill fault")

func (f *staleFault) place(t *testing.T, dir string) {
	t.Helper()
	f.placed = nil
	if f.asDir || f.devFull {
		seq := spillFileSeq.Load() + 1
		p := filepath.Join(dir, fmt.Sprintf("build-spill-%d.%d.bin", os.Getpid(), seq))
		var err error
		if f.asDir {
			err = os.Mkdir(p, 0o700)
		} else {
			err = os.Symlink("/dev/full", p)
		}
		if err != nil {
			t.Fatal(err)
		}
		f.placed = append(f.placed, p)
		return
	}
	f.mu.Lock()
	f.arming = true
	f.mu.Unlock()
	hook := func(stage spillFaultStage, path string) error {
		f.mu.Lock()
		defer f.mu.Unlock()
		if stage == spillFaultCreate && f.arming && f.target == "" &&
			strings.HasPrefix(filepath.Base(path), "build-spill-") {
			f.target = path
		}
		if path == f.target && stage == f.stage {
			if f.once {
				if f.fired {
					return nil
				}
				f.fired = true
			}
			if stage == spillFaultWrite {
				f.writes++
				if f.writes < f.nth {
					return nil
				}
			}
			return fmt.Errorf("%w at %s", errInjectedSpill, filepath.Base(path))
		}
		return nil
	}
	spillWriterFault.Store(&hook)
	f.restore = func() { spillWriterFault.Store(nil) }
	t.Cleanup(f.restore)
}

// lift ends the fault for every run created from now on, once the held call
// returned, so the victim's own later writers (and its own evictions) succeed:
// what the victim does next is then the consequence of the failed relief
// eviction alone. A seam fault stays on the run it already took (the close
// steps run at the victim's build end).
func (f *staleFault) lift() {
	for _, p := range f.placed {
		os.Remove(p)
	}
	f.mu.Lock()
	f.arming = false
	f.mu.Unlock()
}

// staleWide is staleMain with a 400-byte build pad: every partition is then
// larger than the writer's 64 KiB buffer (about 312 rows of 450 bytes), so an
// eviction's own writes reach the file and a failing file fails the eviction
// part-way through the partition, after earlier batches were written and
// dropped. The pad is not in the digest, so PostgreSQL's answer is
// staleReliefPG. The requester's 3,000-byte rows keep the registry selecting
// the victim at this calibration through both entry points (measured: pads
// 120 and 250 and requester pads 400 and 1500 leave one site unreached).
func staleWide() staleFixture {
	fx := staleMain
	fx.buildRows = func() []map[string]any {
		rows := staleBuildRows()
		pad := strings.Repeat("x", 400)
		for _, r := range rows {
			r["pad"] = pad
		}
		return rows
	}
	fx.reqPad = 3000
	return fx
}

// staleSkew is the key-side repair fixture's DATA joined the ordinary way
// (probe pk = i/2; build key 0 for i < 3000, else i): partition 0 holds 3,000
// rows in TWO frozen batches (the accumulator freezes at 2,048 rows), so an
// eviction of it writes a first batch, drops it from memory and only then
// writes the second. tailRoom keeps the victim from evicting anything itself,
// so the held call's eviction takes partition 0, the largest. The tables are
// those of staleFixKeyPG's comment, so PostgreSQL 17.11's answer is
// staleFixKeyPG. RequestRelief only: through the drift backstop the force
// schedule's first round evicts partition 0 before the backstop call lands.
func staleSkew() staleFixture {
	fx := staleMain
	fx.buildRows = staleFixKey.buildRows
	fx.probeRows = func() []map[string]any {
		rows := staleFixKey.probeRows()
		for _, r := range rows {
			r["pk"] = r["bk"]
			delete(r, "bk")
		}
		return rows
	}
	fx.pg = staleFixKeyPG
	fx.reqPad = 3000
	fx.tailRoom = true
	return fx
}

// TestReliefEvictionFailureIsAnsweredOrLoud is the gate that a failed relief
// eviction never becomes a silent wrong answer in its victim, on all ten join
// kinds and through both registry entry points:
//
//   - create: the run cannot be created. Nothing is marked or dropped; the
//     victim answers PostgreSQL 17.11's answer; right after the call its
//     bookkeeping, what it published and the tracker agree; no file is left.
//   - write: the partition overflows the buffer and the file fails (ENOSPC)
//     part-way through the eviction: some batches are written and dropped.
//     The victim fails loudly with the eviction's sticky error.
//   - write-after-drop: the same, with the partition's first batch written
//     and dropped from memory before the second one's write fails.
//   - batch: the batch encoder fails once, after writing part of a batch
//     into the buffer, which bufio does not see (its own error stays clear)
//     and which later writes do not repeat: only what the writer and the
//     join remember can carry it to the victim.
//   - flush, seek, count, close: the eviction itself succeeds (the partition
//     fits the buffer) and the run's close fails at the victim's build end.
//     The victim fails loudly.
func TestReliefEvictionFailureIsAnsweredOrLoud(t *testing.T) {
	cases := []struct {
		name  string
		fx    func() staleFixture
		fault func() *staleFault
		loud  string // "" = must answer PostgreSQL's answer; else the victim's error must contain it
		sites []memory.ReliefSite
		// dropped: the torn partition must have had at least one batch
		// written and dropped when the call returned.
		dropped bool
	}{
		{"create", func() staleFixture { return staleMain }, func() *staleFault { return &staleFault{asDir: true} }, "", nil, false},
		{"write", staleWide, func() *staleFault { return &staleFault{devFull: true} }, "spilling partition", nil, false},
		{name: "write-after-drop", fx: staleSkew, fault: func() *staleFault { return &staleFault{stage: spillFaultWrite, nth: 2} },
			loud: "spilling partition 0", sites: []memory.ReliefSite{memory.ReliefSiteRequest}, dropped: true},
		{"batch", func() staleFixture { return staleMain }, func() *staleFault { return &staleFault{stage: spillFaultBatch, once: true} }, "spilling partition", nil, false},
		{"flush", func() staleFixture { return staleMain }, func() *staleFault { return &staleFault{devFull: true} }, "flushing spill", nil, false},
		{"seek", func() staleFixture { return staleMain }, func() *staleFault { return &staleFault{stage: spillFaultSeek} }, "seeking spill batch count", nil, false},
		{"count", func() staleFixture { return staleMain }, func() *staleFault { return &staleFault{stage: spillFaultCount} }, "writing spill batch count", nil, false},
		{"close", func() staleFixture { return staleMain }, func() *staleFault { return &staleFault{stage: spillFaultClose} }, "closing spill file", nil, false},
	}
	for _, c := range cases {
		sites := c.sites
		if sites == nil {
			sites = []memory.ReliefSite{memory.ReliefSiteRequest, memory.ReliefSiteForceLargest}
		}
		for _, site := range sites {
			for _, k := range staleKinds {
				t.Run(c.name+"/"+string(site)+"/"+k.name, func(t *testing.T) {
					fx := c.fx()
					fx.fault = c.fault()
					fx.fault.name = c.name
					out := runStaleRelief(t, fx, k, site, landDuringBuild)
					want := fx.pg[k.name]
					verdict := "RIGHT"
					switch {
					case out.victimErr != nil:
						verdict = "LOUD"
					case out.victim != want:
						verdict = "WRONG"
					}
					staleLogMu.Lock()
					t.Logf("FAULT %s/%s/%s verdict=%s victim{%v err=%v} requester{err=%v panic=%v} evicted=%d landed{%s} used_after_close=%d files_after_close=%d",
						c.name, site, k.name, verdict, out.victim, out.victimErr, out.reqErr, out.reqPanic, out.evicted, out.landed, out.usedAtClose, out.filesAtClose)
					staleLogMu.Unlock()
					if out.reqPanic != nil {
						t.Errorf("the relief call panicked in the requester: %v", out.reqPanic)
					}
					if verdict == "WRONG" {
						t.Errorf("the victim answered %v after a failed relief eviction; PostgreSQL 17.11: %v", out.victim, want)
					}
					if c.loud == "" {
						if out.victimErr != nil {
							t.Errorf("the victim failed (%v) after a relief eviction that changed nothing; it must answer", out.victimErr)
						}
						if out.evicted != 0 {
							t.Errorf("the failed eviction marked %d partition(s) spilled", out.evicted)
						}
						if out.landed != "" {
							t.Errorf("after the failed relief call: %s", out.landed)
						}
					} else {
						if out.victimErr == nil {
							t.Errorf("the victim answered (%v) after a relief eviction failed with its run torn; it must fail", out.victim)
						} else if !strings.Contains(out.victimErr.Error(), c.loud) {
							t.Errorf("the victim failed with %q, want the %s failure (%q)", out.victimErr, c.name, c.loud)
						}
						if c.dropped && !strings.Contains(out.landed, "resident, 1 written and dropped") {
							t.Errorf("schedule not reached: the eviction did not fail after dropping a written batch (%s)", out.landed)
						}
					}
					if out.usedAtClose != 0 {
						t.Errorf("tracker holds %d bytes after both joins closed", out.usedAtClose)
					}
					if out.filesAtClose != 0 {
						t.Errorf("%d entries left in the spill directory after both joins closed", out.filesAtClose)
					}
				})
			}
		}
	}
}
