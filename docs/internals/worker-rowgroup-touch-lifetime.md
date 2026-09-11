# Worker rowgroup touch lifetime

Source: internal/worker/rowgroup_touch.go — var rowGroupTouchEnabled = os.Getenv("WADJET_ROWGROUP_TOUCH") != "0", moved 2026-09-11 (#1026)
Superseded: The toucher now prefers bounded MADV_POPULATE_READ calls, falling back to the byte walk; touching does not pin pages against later eviction.

Row-group touch-ahead: the residency guarantee MADV_WILLNEED cannot
make. The 2026-08-08 SF10 capped repro measured the steady regime
re-faulting synchronously inside token-holding decode spans DESPITE
the I/O-ahead advises (run-2 decode +23% ns/byte, token stalls 14x,
majflt climbing all run) at read rates far below the device ceiling:
under a saturated page-cache LRU the kernel throttles or skips
advisory readahead, and advised pages can be evicted again before
decode reaches them. The toucher is a per-mmap goroutine that
consumes the same advise ranges and physically faults the pages in
(one byte read per page) — it cannot be throttled away, and it is
deliberately outside the CPU-token budget because page-fault wait is
I/O, not compute. Decode workers then hold tokens for decode alone.
WILLNEED is still issued first for I/O overlap; the toucher rides
behind it and usually finds the pages already arriving.

Lifecycle contract: enqueue only from decode workers (joined by
iter.Close), stop() before munmap — same ordering the Advise seam
documents. stop() abandons queued ranges immediately; a dying scan
must not wait out a fault backlog.

WADJET_ROWGROUP_TOUCH=0 is the kill switch (cap-wrapper forwards it);
WADJET_ROWGROUP_READAHEAD=0 disables the whole advise seam including
this.
