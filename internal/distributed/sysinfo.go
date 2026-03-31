package distributed

import (
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// ProcessRSS returns the current process RSS in bytes by reading /proc/self/status.
// Returns 0 on non-Linux platforms or on error.
func ProcessRSS() int64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, _ := strconv.ParseInt(fields[1], 10, 64)
				return kb * 1024
			}
		}
	}
	return 0
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
