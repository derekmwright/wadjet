# Worker parquet local file read modes

Source: internal/worker/stream_source.go — var data []byte, moved 2026-09-11 (#1026)
Superseded: The default local-temp path can be staged pread rather than mmap; the initial mmap-only description below predates scanPreadHotEnabled.

Parquet path: when a spill dir is available, stream the body to a
local NVMe temp file, mmap it PROT_READ, and hand the mmap'd byte
slice to parquet.NewReaderFromBytes (zero-copy). Heap is bounded
by the kernel's page-cache footprint for the active mmap region
instead of the full file size. Mirrors the WSHF path's streaming
pattern (openShuffleFile above).

Pre-2026-05-22 this used io.ReadAll(rc) + parquet.NewReader
(bytes.NewReader(data), len), which kept TWO full-file buffers
alive per open file: the io.ReadAll slice AND OpenFileReader's
internal make([]byte, size). Q21 SF1 alloc-profile attributed
1110 MB to io.ReadAll + 204 MB to OpenFileReader's make([]byte,
size) — the dominant heap source during join-6 (peak 3.9 GB).

Fallback: when spillDir is empty (tests, MemStore-only setups)
keep the in-memory path but drop the double-buffer by using
NewReaderFromBytes (zero-copy) instead of NewReader+bytes.Reader.
Just-written temp: pread-staged like every other tier once
scanPreadHotEnabled (scan_pread.go — the 2026-08-12 pair put the
frozen-spin holdout in a decode worker with these mmaps as the
prime surviving fault class); under WADJET_SCAN_PREAD_HOT=0 the
original zero-copy mmap of the page-hot temp.
