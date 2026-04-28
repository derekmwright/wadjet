package memory

import (
	"os"
	"strconv"
	"strings"
)

const (
	// cgroupV2MemMax is the cgroups v2 memory limit file.
	cgroupV2MemMax = "/sys/fs/cgroup/memory.max"
	// cgroupV1MemLimit is the cgroups v1 memory limit file.
	cgroupV1MemLimit = "/sys/fs/cgroup/memory/memory.limit_in_bytes"

	// headroomFactor is the fraction of the cgroup limit to use as budget.
	// Leaves 25% for Go runtime, goroutine stacks, and non-tracked allocations.
	headroomFactor = 0.75

	// cgroupV1Unlimited is the sentinel value for "no limit" in cgroups v1.
	// It's the max int64 page-aligned value the kernel returns.
	cgroupV1Unlimited = int64(9223372036854771712)
)

// DetectCgroupLimit reads the raw container memory limit from cgroup files.
// Returns 0 if no container limit is detected.
//
// Detection order:
//  1. process's cgroup v2 scope (/sys/fs/cgroup/<scope>/memory.max), walking
//     up the tree to find the tightest non-"max" limit. This catches
//     systemd-run --scope MemoryMax=N for processes launched inside a
//     transient unit on hosts whose root cgroup has unlimited memory.
//  2. cgroups v2 root: /sys/fs/cgroup/memory.max
//  3. cgroups v1: /sys/fs/cgroup/memory/memory.limit_in_bytes
func DetectCgroupLimit() int64 {
	if limit := detectV2ProcessLimit(); limit > 0 {
		return limit
	}
	if limit := readLimit(cgroupV2MemMax, false); limit > 0 {
		return limit
	}
	if limit := readLimit(cgroupV1MemLimit, true); limit > 0 {
		return limit
	}
	return 0
}

// detectV2ProcessLimit walks the cgroup v2 hierarchy starting at the
// process's own cgroup (read from /proc/self/cgroup) and returns the
// tightest memory.max found. Each ancestor in the hierarchy enforces its
// own memory.max independently — the kernel applies the minimum — so the
// effective limit for the process is the smallest "non-max" value
// encountered while walking from leaf to root.
//
// Returns 0 when the process is in the root cgroup, /proc/self/cgroup
// can't be read, or every ancestor has memory.max set to "max".
func detectV2ProcessLimit() int64 {
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return 0
	}
	// cgroup v2 line format: "0::/system.slice/wadjet-worker-3.scope".
	// Pre-AL2023 hybrid hosts may show v1 lines too; we only consume the
	// "0::" entry which is the unified v2 hierarchy.
	var path string
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "0::") {
			path = strings.TrimPrefix(line, "0::")
			break
		}
	}
	if path == "" || path == "/" {
		return 0
	}
	const cgroupV2Root = "/sys/fs/cgroup"
	var best int64
	for path != "" && path != "/" {
		full := cgroupV2Root + path + "/memory.max"
		if v := readLimit(full, false); v > 0 && (best == 0 || v < best) {
			best = v
		}
		// Walk up: drop the trailing path segment.
		idx := strings.LastIndexByte(path, '/')
		if idx <= 0 {
			break
		}
		path = path[:idx]
	}
	return best
}

// DetectBudget returns a recommended per-task memory budget (75% of the
// container cgroup limit). Returns 0 if no container limit is detected.
func DetectBudget() int64 {
	if limit := DetectCgroupLimit(); limit > 0 {
		return int64(float64(limit) * headroomFactor)
	}
	return 0
}

// DetectPhysicalMemory reads total physical memory from /proc/meminfo.
// Returns 0 if detection fails (non-Linux).
func DetectPhysicalMemory() int64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				kb, err := strconv.ParseInt(fields[1], 10, 64)
				if err == nil && kb > 0 {
					return kb * 1024 // convert kB to bytes
				}
			}
		}
	}
	return 0
}

// DetectMemoryLimit returns the effective memory limit: cgroup limit if
// available, otherwise physical memory. Returns 0 if neither is detected.
func DetectMemoryLimit() int64 {
	if limit := DetectCgroupLimit(); limit > 0 {
		return limit
	}
	return DetectPhysicalMemory()
}

// readLimit parses a cgroup memory limit file. If filterV1Unlimited is true,
// the cgroups v1 "unlimited" sentinel value is treated as no limit.
func readLimit(path string, filterV1Unlimited bool) int64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	s := strings.TrimSpace(string(data))
	if s == "" || s == "max" {
		return 0
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 {
		return 0
	}
	if filterV1Unlimited && v >= cgroupV1Unlimited {
		return 0
	}
	return v
}
