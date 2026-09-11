# Worker capped shuffle partial aggregation

Source: internal/worker/shuffle_partial_agg.go — type cappedPartialAgg struct {, moved 2026-09-11 (#1026)

cappedPartialAgg pre-combines shuffle-task rows on the exchange's
partial-agg keys before they reach the partitioning sink (exchange
partial aggregation — the Trino-style reduce-before-ship mechanism).
Specs are name-preserving (OutputCol == InputCol) and restricted to
self-mergeable functions (SUM/MIN/MAX) by the planner's eligibility
pass, so consumers are untouched: a grouped final_aggregate merges
partials exactly as it would raw rows, and a join consumer probes the
same (key, value) column names.

Memory is bounded by capBytes: when the hash state exceeds the cap the
current groups are flushed downstream and a fresh epoch begins. Poorly
clustered inputs therefore degrade to shipping ~one row per input row
in aggregate form — never spilling, never OOMing. The output schema is
identical across epochs (same HashAggregate config), which the
partitioned sink requires: it locks the WSHF schema from the first
batch it consumes.

NOTE on types: the shipped column type may differ from the raw payload's
(SUM over an int32-class column widens to int64, for one), so consumers
resolve WSHF columns by name and type per batch; every chunk this operator
emits shares one schema. SUM over a DECIMAL ships a DECIMAL at the column's
own scale since #455 — it used to ship the float64 the accumulator
finalized through, which is where the digits went.
