package worker

import "os"

// stageFsyncEnabled restores the historical fsync of stage-output files
// (.wshf shuffle partitions, unpartitioned stage sinks) at Finalize.
//
// Default OFF: these files are scratch, not a durability boundary. Local
// and peer readers are served by the page cache whether or not the data
// reached the platter; S3 upload (--shuffle-durability) is the recovery
// path for worker death, and node death loses the instance-store NVMe
// regardless of fsync. write() errors (ENOSPC) still surface
// synchronously at write time. What the fsync did do is force every
// worker to flush multi-GB of freshly written dirty pages at the same
// stage barrier, GOMAXPROCS-wide — a synchronized writeback storm that
// stalled concurrent mmap faults (WSHF readers) in the kernel and
// stretched GC stop-the-world into multi-second process freezes
// (2026-08-11 SF100 Q21: frozen-spin watchdog fired on 2/3 workers in
// the same second, both inside shuffle Finalize).
//
// WADJET_SHUFFLE_FSYNC=1 restores the old behavior.
var stageFsyncEnabled = os.Getenv("WADJET_SHUFFLE_FSYNC") == "1"

// syncStageFile fsyncs a stage-output file when the restore switch is on;
// otherwise it is a no-op (see stageFsyncEnabled).
func syncStageFile(f *os.File) error {
	if !stageFsyncEnabled {
		return nil
	}
	return f.Sync()
}
