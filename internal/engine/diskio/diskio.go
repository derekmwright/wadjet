// Package diskio bounds the page-cache impact of the worker's large
// sequential file writes (operator spill files, local cache downloads,
// stage-output files).
//
// Mechanism: windowed asynchronous writeback (PostgreSQL's
// checkpoint_flush_after, RocksDB's bytes_per_sync — their DEFAULT,
// non-strict shapes). As each window of windowBytes completes,
// asynchronous writeback is started for it
// (sync_file_range(SYNC_FILE_RANGE_WRITE)), so dirty pages reach the
// device within one window of being written instead of waiting on the
// ~30s kernel flusher. Steady-state dirty footprint per writer ≈
// write-rate × device-latency. There is deliberately no per-window
// blocking wait — the strict variant (RocksDB's off-by-default
// strict_bytes_per_sync) serialized multi-GB cache downloads behind
// device writeback at SF100 and regressed exchange-heavy queries 50-110%
// (run 20260610-203304); see Flusher.wrote.
//
// Why dirty pages specifically: they cannot be reclaimed until written
// back, so a multi-GB write flood (cache downloads + spill) forces kernel
// reclaim to evict whatever IS cheaply reclaimable — the clean mmap'd
// cache-file pages concurrent tasks are still walking. That eviction is
// the major-fault mechanism behind the PR #112 regression
// (project_mmap_selfrelief_postmortem_2026-06-10); bounding the write
// side attacks the cause without any reader coordination.
//
// Class picks the fate of written-back (now clean) windows:
//
//   - Spill: read back at most once, much later — each clean window is
//     FADV_DONTNEED'd immediately, so spill bytes never compete with the
//     mmap'd cache pages at all. Read-back streams from NVMe.
//   - KeepResident: mmap-walked or uploaded right after the write — the
//     windows stay resident (only the dirty bound applies), so the
//     imminent readers take minor faults, not majors.
//
// The whole mechanism is gated on --bounded-dirty-writes (default false).
// When disabled, NewWriter returns the file itself and a nil Flusher, so
// the only cost is one atomic load at writer construction.
package diskio

import "sync/atomic"

// enabled gates the whole mechanism. Set once at startup via SetEnabled.
var enabled atomic.Bool

// SetEnabled activates or deactivates windowed writeback for writers
// created afterwards. Called once from worker startup (the
// --bounded-dirty-writes flag); tests may toggle it.
func SetEnabled(on bool) { enabled.Store(on) }

// Enabled reports whether windowed writeback is active.
func Enabled() bool { return enabled.Load() }

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
