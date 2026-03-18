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
//  1. cgroups v2: /sys/fs/cgroup/memory.max
//  2. cgroups v1: /sys/fs/cgroup/memory/memory.limit_in_bytes
func DetectCgroupLimit() int64 {
	if limit := readLimit(cgroupV2MemMax, false); limit > 0 {
		return limit
	}
	if limit := readLimit(cgroupV1MemLimit, true); limit > 0 {
		return limit
	}
	return 0
}

// DetectBudget returns a recommended per-task memory budget (75% of the
// container cgroup limit). Returns 0 if no container limit is detected.
func DetectBudget() int64 {
	if limit := DetectCgroupLimit(); limit > 0 {
		return int64(float64(limit) * headroomFactor)
	}
	return 0
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
