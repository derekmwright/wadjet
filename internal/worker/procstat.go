package worker

import (
	"os"
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
