package scan

import "golang.org/x/sys/unix"

// threadCPUNs returns the calling OS thread's cumulative user and system
// CPU time (ns) via getrusage(RUSAGE_THREAD). Sampled before/after a
// decode span it splits the span's wall into on-CPU user, on-CPU sys,
// and (by subtraction) off-CPU — the discriminator between "decode got
// slower" and "decode got descheduled/blocked". Deltas are only valid
// within one OS thread: callers must LockOSThread around the sampled
// span (an unpinned goroutine can migrate mid-span and diff two
// unrelated threads' lifetime counters).
func threadCPUNs() (userNs, sysNs int64) {
	var ru unix.Rusage
	if err := unix.Getrusage(unix.RUSAGE_THREAD, &ru); err != nil {
		return 0, 0
	}
	return ru.Utime.Nano(), ru.Stime.Nano()
}
