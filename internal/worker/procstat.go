package worker

import (
	"os"
	"runtime/metrics"
	"strconv"
	"strings"
)

// Process/device I-O counters for the steady-regime marker lines
// (docs/design/rowgroup-readahead.md, residual diagnosis). The decode
// span ns/byte ratio says decode is stretched; these say WHY: majflt
// climbing during steady scans means decode workers still block on
// hard faults (readahead losing the race — latency problem, fixable),
// while nvme read throughput pinned at the device ceiling with few
// majflt means the bandwidth floor. All readers return zeros on any
// parse or open failure — the markers are advisory, never load-bearing.

// procSelfFaults returns the process's cumulative minor and major page
// fault counts (/proc/self/stat fields 10 and 12). Major faults block
// on device I/O; minor faults map an already-resident page — a WILLNEED
// that keeps ahead of decode turns the former into the latter.
func procSelfFaults() (minflt, majflt int64) {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0, 0
	}
	// comm (field 2) may contain spaces and parens; fields resume after
	// the LAST ')'. state is field 3, so minflt/majflt (fields 10/12)
	// are tokens 7 and 9 of the remainder.
	rest := string(data)
	if i := strings.LastIndexByte(rest, ')'); i >= 0 {
		rest = rest[i+1:]
	}
	f := strings.Fields(rest)
	if len(f) < 10 {
		return 0, 0
	}
	minflt, _ = strconv.ParseInt(f[7], 10, 64)
	majflt, _ = strconv.ParseInt(f[9], 10, 64)
	return minflt, majflt
}

// procSelfIO returns the process's cumulative storage-layer read and
// write bytes (/proc/self/io read_bytes/write_bytes). mmap fault reads
// and WILLNEED readahead are both attributed to the faulting/advising
// process here.
func procSelfIO() (readBytes, writeBytes int64) {
	data, err := os.ReadFile("/proc/self/io")
	if err != nil {
		return 0, 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if v, ok := strings.CutPrefix(line, "read_bytes: "); ok {
			readBytes, _ = strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		} else if v, ok := strings.CutPrefix(line, "write_bytes: "); ok {
			writeBytes, _ = strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		}
	}
	return readBytes, writeBytes
}

// procSelfCPU returns the process's cumulative user and system CPU time
// in milliseconds (/proc/self/stat fields 14 and 15, USER_HZ=100 ticks).
// Diffed per run, these split a stretched decode span into "doing more
// CPU work" (utime up), "in the kernel" (stime up — reclaim and fault
// paths bill here), or "off-CPU" (neither moves while span wall does).
func procSelfCPU() (utimeMs, stimeMs int64) {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0, 0
	}
	rest := string(data)
	if i := strings.LastIndexByte(rest, ')'); i >= 0 {
		rest = rest[i+1:]
	}
	f := strings.Fields(rest)
	if len(f) < 13 {
		return 0, 0
	}
	utime, _ := strconv.ParseInt(f[11], 10, 64)
	stime, _ := strconv.ParseInt(f[12], 10, 64)
	return utime * 10, stime * 10
}

// hostCPUTimes returns the host-wide cumulative CPU accounting in
// milliseconds from the aggregate "cpu" line of /proc/stat: busy
// (user+nice+system+irq+softirq), idle, iowait, and steal. Idle climbing
// while decode spans stretch means blocked-not-queued; busy pinned at
// cores×wall means the box is CPU-saturated and spans stretch by
// preemption.
func hostCPUTimes() (busyMs, idleMs, iowaitMs, stealMs int64) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, 0, 0, 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) < 9 || f[0] != "cpu" {
			continue
		}
		v := make([]int64, 8)
		for i := range v {
			v[i], _ = strconv.ParseInt(f[i+1], 10, 64)
		}
		// user nice system idle iowait irq softirq steal
		busyMs = (v[0] + v[1] + v[2] + v[5] + v[6]) * 10
		idleMs = v[3] * 10
		iowaitMs = v[4] * 10
		stealMs = v[7] * 10
		return busyMs, idleMs, iowaitMs, stealMs
	}
	return 0, 0, 0, 0
}

// parsePSITotals extracts the cumulative stall totals (µs) from a PSI
// file body ("some avg10=... total=N" / "full ... total=N" lines).
func parsePSITotals(data string) (someUs, fullUs int64) {
	for _, line := range strings.Split(data, "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		var total int64
		for _, tok := range f[1:] {
			if v, ok := strings.CutPrefix(tok, "total="); ok {
				total, _ = strconv.ParseInt(v, 10, 64)
			}
		}
		switch f[0] {
		case "some":
			someUs = total
		case "full":
			fullUs = total
		}
	}
	return someUs, fullUs
}

// psiTotals returns cumulative pressure-stall totals (µs) for cpu,
// memory, and io. Inside a cgroup-capped container the host-wide
// /proc/pressure files miss the cap-induced pressure, so the cgroup2
// interface files are preferred when readable. PSI is the kernel's own
// verdict on the stretched-span question: memory full climbing during
// run 2 says reclaim, cpu some says runqueue, io says device.
func psiTotals() (cpuSomeUs, memSomeUs, memFullUs, ioSomeUs, ioFullUs int64) {
	read := func(cg, proc string) (int64, int64) {
		data, err := os.ReadFile(cg)
		if err != nil {
			if data, err = os.ReadFile(proc); err != nil {
				return 0, 0
			}
		}
		return parsePSITotals(string(data))
	}
	cpuSomeUs, _ = read("/sys/fs/cgroup/cpu.pressure", "/proc/pressure/cpu")
	memSomeUs, memFullUs = read("/sys/fs/cgroup/memory.pressure", "/proc/pressure/memory")
	ioSomeUs, ioFullUs = read("/sys/fs/cgroup/io.pressure", "/proc/pressure/io")
	return cpuSomeUs, memSomeUs, memFullUs, ioSomeUs, ioFullUs
}

// schedWaitTotalMs returns an approximate cumulative goroutine
// scheduling latency (ms) from the runtime's /sched/latencies:seconds
// histogram, using bucket-midpoint weighting. Runnable-but-waiting time
// inside the process: high here with host idle available means
// GOMAXPROCS-level contention, not kernel pressure.
func schedWaitTotalMs() int64 {
	sample := []metrics.Sample{{Name: "/sched/latencies:seconds"}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindFloat64Histogram {
		return 0
	}
	h := sample[0].Value.Float64Histogram()
	var totalSec float64
	for i, count := range h.Counts {
		if count == 0 {
			continue
		}
		lo, hi := h.Buckets[i], h.Buckets[i+1]
		mid := lo
		if !isInf(lo) && !isInf(hi) {
			mid = (lo + hi) / 2
		} else if isInf(lo) {
			mid = hi
		}
		totalSec += mid * float64(count)
	}
	return int64(totalSec * 1e3)
}

func isInf(f float64) bool { return f > 1e308 || f < -1e308 }

// nvmeDiskstats returns cumulative read and write bytes summed across
// whole nvme devices (/proc/diskstats sectors ×512, partitions
// excluded so bytes aren't double-counted) — the device-level truth the
// bandwidth-floor verdict needs: unlike procSelfIO it includes page
// cache writeback and every other process.
func nvmeDiskstats() (readBytes, writeBytes int64) {
	data, err := os.ReadFile("/proc/diskstats")
	if err != nil {
		return 0, 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		f := strings.Fields(line)
		if len(f) < 10 {
			continue
		}
		name := f[2]
		if !strings.HasPrefix(name, "nvme") || strings.Contains(name, "p") {
			continue
		}
		if sr, err := strconv.ParseInt(f[5], 10, 64); err == nil {
			readBytes += sr * 512
		}
		if sw, err := strconv.ParseInt(f[9], 10, 64); err == nil {
			writeBytes += sw * 512
		}
	}
	return readBytes, writeBytes
}
