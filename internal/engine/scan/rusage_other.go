//go:build !linux

package scan

// threadCPUNs is the non-linux stub: RUSAGE_THREAD is linux-only, so
// decode-span CPU attribution reads zero elsewhere (the markers are
// advisory; zeros mean "unmeasured", matching the procfs readers).
func threadCPUNs() (userNs, sysNs int64) { return 0, 0 }
