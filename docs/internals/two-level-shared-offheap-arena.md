# Two level shared offheap arena

Source: internal/engine/exec/two_level_hash.go — offheapSubMinBytes, moved 2026-09-11 (#1026)

offheapSubMinBytes is the size at which an entry array moves off-heap.
2 MiB, and the number is load-bearing: it is the huge-page size.

The flat table goes off-heap unconditionally, which is right for ONE array
of tens of MB. Applied per bucket it is a trap: a mapping smaller than a
huge page is faulted in 4 KiB at a time and stays 4 KiB-paged, so a 256 MB
index spread over 256 sub-mappings takes ~65k faults and blows the dTLB,
where the same bytes in one big mapping take ~128 huge-page faults.
Measured (BenchmarkIntIndexOffheapSubGate, fill 8M int keys, min of 5,
one interleaved window):

	flat                       306 ms
	buckets off-heap >= 2 MiB  281 ms   (-8%)
	buckets off-heap >= 64 KiB 427 ms   (+39%)
	buckets off-heap >= 4 KiB  407 ms   (+33%)

Which is right, and was applied to the wrong UNIT. A bucket reaches 2 MiB
only when the whole index is ~33M slots — past 23M groups in one sink,
which nothing in TPC-H or ClickBench reaches. So in practice the gate was
unreachable, and converting to the bucketed form silently moved the entire
group index off its MAP_NORESERVE huge-page reservation onto the Go heap:
470 MB of heap churn per 16M-group fill where the flat table allocates
11 KB (ADR-0006's amendment, undone by the structure meant to complement
it). With WADJET_OFFHEAP_AGG=0 — which puts BOTH forms on the heap — the
same 16M near-unique fill inverts from +14.8% to -4.6% against flat: the
backing, not the structure, was the loss.

The unit that wants a huge page is the TABLE, not the bucket. So the 256
buckets are carved out of ONE reservation whenever their total clears this
gate (allocIntArena / newIntTwoLevelTableSub) — one mapping, one
MADV_HUGEPAGE, zero Go heap, and the buckets still index and probe
independently because the arena is only their backing store, never their
addressing. A bucket that outgrows its slice allocates on its own (per
the per-bucket gate below, which is now the fallback rather than the
rule), and the arena is released as soon as the last bucket has left it.

A var so tests can force either side of the gate.
