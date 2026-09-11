# Memory offheap array ownership

Source: internal/engine/memory/offheap_linux.go — var offheapAggToggle = optswitch.Register("offheap-agg", "WADJET_OFFHEAP_AGG",, moved 2026-09-11 (#1026)
Superseded: In-place append growth is bounded by the reservation capacity, not literally forever.

Off-heap growable arrays for aggregate group state (ADR-0006 amendment,
2026-08-17). The typed-SoA aggregation paths accumulate multi-GB
pointer-free arrays (flat accumulators, key SoAs) whose growth used to
ride Go's append: every doubling holds old+new live simultaneously and
leaves the old array as garbage for the collector. At the 100M-group
scale (ClickBench Q33) that stacked ~10GB of transients on ~12GB of
live state between GC cycles — measured 22.3GB heap on 12GB live — and
whether a cold try fit under GOMEMLIMIT or spiraled through the
pressure-valve spill path was decided by GC timing (108s vs 21s cold
on identical binaries, attributed by a same-window binary control).

The fix: back these arrays with anonymous MAP_NORESERVE reservations.
A slice is created over the reservation with len=0 and cap=the full
reserve, so every existing append site grows it IN PLACE forever — no
reallocation, no copy, no garbage, and the Go heap never sees the
bytes (the engine's own tracker still accounts them exactly; that
accounting, not heap size, drives spill decisions). Pages commit
lazily on first touch and arrive zeroed, which append's zero-writes
then satisfy for free.

Scope guard: this is NOT the shelved BytesColumn decode arena
(2026-06-09) — no object lifetimes, no per-value layout, no sharing.
It manages whole pointer-free arrays with one owner each, registered
on an OffheapRegistry whose lifetime is the owning operator's.
