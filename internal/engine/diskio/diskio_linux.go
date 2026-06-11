//go:build linux

package diskio

import (
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// windowBytes is the writeback window. 8 MiB sits in the range production
// engines converged on for fast SSD/NVMe (RocksDB bytes_per_sync 1-2 MiB,
// the canonical LKML example 8 MiB; PostgreSQL's 256 KiB is biased toward
// latency-sensitive OLTP), and being a multiple of the page size keeps
// every FADV_DONTNEED range page-aligned — the kernel ignores partial
// pages.
const windowBytes = 8 << 20

// Syscall seams so tests can record/inject without real I/O.
var (
	syncFileRange = unix.SyncFileRange
	fadvise       = unix.Fadvise
)

// Flusher applies the two-window writeback pattern to one sequentially
// written file. Not goroutine-safe: every write site already serializes
// writes per file (bufio behind a mutex, or a single goroutine).
type Flusher struct {
	fd       int
	class    Class
	off      int64 // sequential bytes observed so far
	boundary int64 // start of the window currently being filled
	disabled bool  // set on first syscall error (e.g. unsupported fs)
}

// NewWriter wraps f so windowed writeback tracks every Write. All
// sequential content writes must go through the returned writer;
// non-sequential header patches may go to f directly (Finish covers
// them with a whole-file pass). When the mechanism is disabled it
// returns f itself and a nil Flusher (whose Finish is a no-op), so call
// sites need no branches.
func NewWriter(f *os.File, class Class) (io.Writer, *Flusher) {
	if !enabled.Load() {
		return f, nil
	}
	fl := &Flusher{fd: int(f.Fd()), class: class}
	return &flushWriter{f: f, fl: fl}, fl
}

type flushWriter struct {
	f  *os.File
	fl *Flusher
}

func (w *flushWriter) Write(p []byte) (int, error) {
	n, err := w.f.Write(p)
	if n > 0 {
		w.fl.wrote(n)
	}
	return n, err
}

// wrote advances the sequential write cursor. For every window that
// completes it starts ASYNC writeback on that window and, for Spill,
// best-effort drops the window before it (whose writeback was queued a
// full window earlier and has usually completed by now).
//
// Deliberately NO blocking wait here. The first version used the strict
// pattern (WAIT_BEFORE|WRITE|WAIT_AFTER on window N-1, RocksDB's
// off-by-default strict_bytes_per_sync) and it throttled multi-GB cache
// downloads to ~96 MiB/s at SF100 — workers sat CPU-idle behind
// serialized stage inputs, Q18 +53%, Q21 ENOSPC from the stretched
// lifetimes (run 20260610-203304). Async-only matches what PostgreSQL
// (checkpoint_flush_after) and RocksDB (bytes_per_sync) actually default
// to: writeback starts within one window of the data being written —
// instead of the ~30s dirty-expire flusher — which bounds steady-state
// dirty pages at write-rate × device-latency without ever blocking the
// writer. vm.dirty_* sysctls remain the backstop if the device falls
// behind.
func (fl *Flusher) wrote(n int) {
	if fl.disabled {
		return
	}
	fl.off += int64(n)
	for fl.off-fl.boundary >= windowBytes {
		cur := fl.boundary
		fl.boundary += windowBytes
		if err := syncFileRange(fl.fd, cur, windowBytes, unix.SYNC_FILE_RANGE_WRITE); err != nil {
			fl.disabled = true
			return
		}
		if fl.class == Spill && cur > 0 {
			// Best-effort: a no-op on any pages still dirty (the kernel
			// skips them); Finish()'s whole-file pass is the guaranteed
			// drop.
			_ = fadvise(fl.fd, cur-windowBytes, windowBytes, unix.FADV_DONTNEED)
		}
	}
}

// Finish completes the page-cache treatment for the file. Call it after
// the final content write — including any header patch made directly on
// the file — and before the file is closed. For Spill files it writes
// back whatever is still dirty (the tail plus any patched header page)
// and drops the entire file from cache; KeepResident files keep their
// pages, so it is a no-op. Errors are swallowed: the machinery is
// advisory and never affects file contents. nil-safe.
func (fl *Flusher) Finish() {
	if fl == nil || fl.disabled || fl.class != Spill {
		return
	}
	// offset 0 / nbytes 0 = the whole file, for both calls. Window
	// writeback was started asynchronously throughout the write, so this
	// wait covers only whatever the device hasn't absorbed yet. It is the
	// one blocking call in the mechanism: once per spill file, at close.
	if err := syncFileRange(fl.fd, 0, 0,
		unix.SYNC_FILE_RANGE_WAIT_BEFORE|unix.SYNC_FILE_RANGE_WRITE|unix.SYNC_FILE_RANGE_WAIT_AFTER); err != nil {
		return
	}
	_ = fadvise(fl.fd, 0, 0, unix.FADV_DONTNEED)
}
