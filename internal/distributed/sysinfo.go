package distributed

import (
	"os"
	"path/filepath"
	"runtime"
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
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return int64(ms.HeapInuse)
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

// DirDiskUsage returns the total size of files in a directory (non-recursive).
// Returns 0 on error.
func DirDiskUsage(dir string) int64 {
	if dir == "" {
		return 0
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var total int64
	for _, e := range entries {
		if !e.IsDir() {
			if info, err := e.Info(); err == nil {
				total += info.Size()
			}
		}
	}
	// Also check wadjet-spill subdirectory
	spillDir := filepath.Join(dir, "wadjet-spill")
	subEntries, err := os.ReadDir(spillDir)
	if err == nil {
		for _, e := range subEntries {
			if !e.IsDir() {
				if info, err := e.Info(); err == nil {
					total += info.Size()
				}
			}
		}
	}
	return total
}

// NumGoroutines returns the current number of goroutines.
func NumGoroutines() int {
	return runtime.NumGoroutine()
}
