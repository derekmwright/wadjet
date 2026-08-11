package worker

import "os"

// Pread-staged parquet scan reads (docs/design/scan-pread-reads.md):
// local-tier parquet opens (base-table cache hits, prefetched downloads,
// S3 streams staged to spill) decode from pread-staged pooled buffers
// instead of an mmap of the file. This removes the page-fault class from
// decode goroutines entirely: a goroutine blocked in a read syscall
// parks at a GC-safe point, while one faulting on an mmap'd page inside
// a decode span stretches every stop-the-world pause in the process —
// the R2 steady-drift mechanism (progressive 0.3 → 10-38 ms/cycle STW
// degradation) established by the 2026-08-11 drift diagnosis.
//
// WADJET_SCAN_PREAD=0 is the kill switch, restoring the mmap +
// touch-ahead path unchanged.
var scanPreadEnabled = os.Getenv("WADJET_SCAN_PREAD") != "0"
