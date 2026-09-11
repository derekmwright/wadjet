// Package diskio reduces dirty-page pressure from large sequential worker writes
// using asynchronous sync_file_range WRITE per window, with NO per-window wait.
// Strict writeback serialization regresses downloads; see Flusher.wrote and
// #112/project_mmap_selfrelief_postmortem_2026-06-10.
// Spill advises DONTNEED behind writes and waits/drops on Finish; KeepResident
// retains clean pages for imminent mmap/upload consumers. Advice is best-effort,
// not a guarantee dirty bytes are reclaimable within a fixed window.
// --bounded-dirty-writes gates writeback; drop-behind independently keeps Spill
// active. Fully disabled NewWriter returns the file/nil Flusher after one load.
// See docs/internals/diskio-windowed-writeback.md for the design.
package diskio

import (
	"os"
	"sync/atomic"
)

// enabled gates the whole mechanism. Set once at startup via SetEnabled.
var enabled atomic.Bool

// SetEnabled activates or deactivates windowed writeback for writers
// created afterwards. Called once from worker startup (the
// --bounded-dirty-writes flag); tests may toggle it.
func SetEnabled(on bool) { enabled.Store(on) }

// Enabled reports whether windowed writeback is active.
func Enabled() bool { return enabled.Load() }

// dropBehind gates the single-pass drop-behind treatment independently of
// --bounded-dirty-writes: Spill-class writers and DropBehindReader stay
// active by default so single-pass bytes (spill runs, shuffle scratch,
// base-table populate copies) never accumulate in the page cache and
// evict the reusable pages a steady-state suite re-reads
// (docs/benchmarks/steady-slower-than-cold-2026-08-08.md). Kill switch:
// WADJET_DROP_BEHIND=0.
var dropBehind atomic.Bool

func init() { dropBehind.Store(os.Getenv("WADJET_DROP_BEHIND") != "0") }

// SetDropBehindEnabled toggles single-pass drop-behind; tests only.
func SetDropBehindEnabled(on bool) { dropBehind.Store(on) }

// DropBehindEnabled reports whether single-pass drop-behind is active.
func DropBehindEnabled() bool { return dropBehind.Load() }

// Drop-behind engagement counters, exposed as wlog rollout markers so an
// SF100 run shows the mechanism working without a kernel-side probe.
var (
	writeDropBytes atomic.Int64 // Spill-class windows dropped after writeback
	readDropBytes  atomic.Int64 // bytes dropped behind single-pass readers (fd + mmap walks)
)

// AddReadDropBytes records n bytes dropped behind a single-pass read by a
// caller that issues its own advise calls (the worker's shuffle mmap walk).
func AddReadDropBytes(n int64) { readDropBytes.Add(n) }

// DropBehindStats returns cumulative dropped bytes on the write and read
// sides since process start.
func DropBehindStats() (write, read int64) {
	return writeDropBytes.Load(), readDropBytes.Load()
}

// Class describes how a file's pages are treated once written back.
type Class int

const (
	// Spill marks files written now and read back at most once, later:
	// sort/window runs, join build/probe partitions, aggregate partial
	// state, raw-row spill.
	Spill Class = iota
	// KeepResident marks files whose pages are wanted immediately after
	// the write: local cache downloads that are mmap'd and walked, and
	// stage outputs that are uploaded (and possibly adopted into the
	// LocalStageCache and mmap'd by a same-worker consumer) right after
	// Finalize.
	KeepResident
)
