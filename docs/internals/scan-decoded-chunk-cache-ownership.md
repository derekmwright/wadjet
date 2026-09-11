# Scan decoded chunk cache ownership

Source: internal/engine/scan/decoded_cache.go — type DecodedChunkCache struct {, moved 2026-09-11 (#1026)
Superseded: Cache keys now include DECIMAL scale and precision; admission also requires a frequency margin over victims with periodic aging, so a second touch does not guarantee admission.

DecodedChunkCache is a worker-lifetime cache of decoded parquet column
chunks: one entry per (object identity, row group, leaf column, catalog
type) holding a cache-owned *batch.Vector clone. It attacks the zstd
decompress + decode-kernel bill (~24% + ~7% of SF100 worker CPU) for
re-reads of the same immutable base-table bytes — cross-query within a
run and across benchmark runs. See docs/design/decoded-rowgroup-cache.md.

Correctness model (v1, copy discipline): consumers NEVER share storage
with the cache. A hit copies the cached vector into the caller's batch
slot; an admit clones the freshly decoded vector into cache-owned
storage. Entry vectors are immutable once inserted, so Get may return
the entry pointer and the caller copies outside the cache lock (an
eviction during the copy is safe — the GC keeps the clone alive).

Ledger model (ADR-0006): the cache OWNS its bytes once, surfaced through
Size for a hard system reservoir (memory.NewReservoirFunc). Consumers'
copies are ordinary scan output charged exactly as today. The cache also
implements memory.AccountedOperator so RequestRelief can shed it —
eviction is the cheapest relief in the process — before any operator
pays a real spill.

Eviction is segmented LRU (probation/protected) with second-touch ghost
admission: a key's first decode registers a ghost; the clone is stored
on the second decode; hits promote probation entries to protected.
Sequential scan floods (a cold first pass over a table) fill probation
without displacing the protected hot set.
