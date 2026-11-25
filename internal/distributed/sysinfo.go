// SPDX-License-Identifier: AGPL-3.0-only

package distributed

import (
	"os"
	"path/filepath"
	"runtime"
	runtimemetrics "runtime/metrics"
	"strconv"
	"strings"
)

// pageSize is the OS page size in bytes, resolved once. /proc/self/statm
// reports counts in pages; multiply by this to get bytes.
var pageSize = int64(os.Getpagesize())

// ProcessRSS returns the current process resident set size in bytes.
//
// On Linux it reads /proc/self/statm (field 2 = resident pages) × pagesize —
// cheaper than scanning /proc/self/status for VmRSS. This is TRUE resident
// memory: the Go heap AND mmap'd file pages resident in core (parquet/shuffle
// page cache) that runtime.MemStats.HeapInuse cannot see.
//
// On non-Linux platforms or any read/parse error it falls back to the Go
// heap-in-use figure so dev/CI on macOS/Windows and tests get a sane non-zero
// number rather than 0 (the previous behavior). HeapInuse is a strict
// under-estimate (it misses mmap) — the safe direction; it never over-reports.
func ProcessRSS() int64 {
	if data, err := os.ReadFile("/proc/self/statm"); err == nil {
		// Format: "size resident shared text lib data dt" (counts in pages).
		if rss, ok := processRSSFromStatm(data); ok {
			return rss
		}
	}
	// STW-free fallback (never taken on Linux): HeapInuse == heap
	// objects + unused span bytes in runtime/metrics terms.
	s := []runtimemetrics.Sample{
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/heap/unused:bytes"},
	}
	runtimemetrics.Read(s)
	return int64(s[0].Value.Uint64() + s[1].Value.Uint64())
}

// processRSSFromStatm parses the resident-pages field (2) of /proc/self/statm
// and returns bytes. ok is false on a malformed line. Split out for testing.
func processRSSFromStatm(data []byte) (int64, bool) {
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0, false
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return pages * pageSize, true
}

// DirDiskUsage returns the total size of the files directly in dir, plus every
// file in the whole subtree of dir/wadjet-spill: the pool's files directly in
// it and each statement scope's (wadjet-spill/stmt-<pid>-<n>/,
// memory.SpillManager.StatementScope), which is where a worker task's operator
// spill goes, at whatever depth an operator writes. Other subdirectories of dir
// (stage-cache/, heap-profiles/, shuffle output) are not spill and are not
// counted. Symbolic links below wadjet-spill are not followed: a link counts
// its own size and is never descended. Returns 0 on error.
func DirDiskUsage(dir string) int64 {
	if dir == "" {
		return 0
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	return sumFileSizes(entries) + treeFileSizes(filepath.Join(dir, "wadjet-spill"))
}

// treeFileSizes totals the files in dir and, recursively, in its real
// subdirectories. A DirEntry reports a symbolic link's own type (IsDir is
// false for a link to a directory), so links are counted, never followed, and
// a link cycle cannot loop. An unreadable directory counts 0.
func treeFileSizes(dir string) int64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	total := sumFileSizes(entries)
	for _, e := range entries {
		if e.IsDir() {
			total += treeFileSizes(filepath.Join(dir, e.Name()))
		}
	}
	return total
}

// sumFileSizes totals the sizes of the non-directory entries; an entry that
// vanished between the listing and its stat counts 0.
func sumFileSizes(entries []os.DirEntry) int64 {
	var total int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if info, err := e.Info(); err == nil {
			total += info.Size()
		}
	}
	return total
}

// NumGoroutines returns the current number of goroutines.
func NumGoroutines() int {
	return runtime.NumGoroutine()
}
