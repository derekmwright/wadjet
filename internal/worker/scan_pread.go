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

// scanPreadHotEnabled extends pread staging to the just-written (page-hot)
// tier: S3 bodies staged to spill and prefetched downloads, which the
// original refinement deliberately kept on mmap after full-pread priced
// that tier at +15.9% cold — a cost dominated by pool-overflow allocs the
// 128 MiB pool classes have since absorbed. The 2026-08-12 SF100 pair
// (docs/design/shuffle-pread-reads.md §Validation) put the frozen-spin
// holdout M inside a ReadRowGroupNative decode worker with WSHF fully
// converted and parquet ~96% pread by bytes — the just-written mmaps are
// the prime surviving fault class, so the exemption goes.
//
// WADJET_SCAN_PREAD_HOT=0 restores the mmap exemption for just-written
// temps (tonight's shipped behavior). Inert when WADJET_SCAN_PREAD=0.
var scanPreadHotEnabled = os.Getenv("WADJET_SCAN_PREAD_HOT") != "0"
