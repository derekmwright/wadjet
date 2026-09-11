# Memory cooperative and heap spill signals

Source: internal/engine/memory/spill.go — func (sm *SpillManager) ShouldSpill() bool {, moved 2026-09-11 (#1026)

ShouldSpill returns true when the operator should spill to disk.

It checks two independent signals:

 1. **Per-tracker budget** — the original cooperative signal: each operator
    reports its tracked allocations and spills when its share of the budget
    is exhausted. Cheap (atomic load) and accurate when every allocation
    paths through the tracker.

 2. **Process-wide heap pressure** — checks runtime.MemStats.HeapAlloc
    against GOMEMLIMIT and triggers spill when the heap approaches the
    soft limit. This catches allocations that bypass the tracker (probe
    pipeline batches, gather buffers, scan source channel buffers, every
    non-build operator that doesn't currently report memory). Without
    this signal, the SF100 deploy would hit 31 GB anon-rss with a 1.4 GB
    tracker budget — the tracker's view of memory was 22× smaller than
    reality, and the per-tracker spill check stayed under threshold while
    the process climbed past physical RAM.

runtime.ReadMemStats is moderately expensive (sub-millisecond), so the
reading is rate-limited to once per 100 ms across all callers.
