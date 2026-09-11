# Aggregate drain productivity

Source: internal/engine/exec/aggregate_drain_gate.go — drainIsProductive / noteDrain, moved 2026-09-11 (#1026)

Drain productivity gate (#325).

SpillManager.ShouldSpillFor answers a question about the WHOLE tracker —
"is the shared budget (or the process heap) over threshold?" — not about
this operator. A HashAggregate that owns almost none of the pressured
bytes therefore sees the signal on every batch and, before this gate,
answered each one with a drain: whole-table for the packed/string/generic
key modes, a partition slice for the int-keyed one. Neither relieves
pressure it did not cause, so the signal is still true on the next batch
and the operator drains again.

The SF100 report in #325 is that loop at scale: `GROUP BY l_partkey,
l_suppkey` over three years of lineitem, 23,153 backpressure pauses
totalling 1,158 s, 24 GB of agg-spill-*.bin, and a stage watchdog that
eventually blamed a worker crash that never happened. Draining once per
batch also makes each run file batch-sized, so the k-way merge's fan-in
grows with the input rather than with the state.

The gate is a floor on NEW state since the last drain. Its two properties
are what break the loop:

  - A drain only runs when this operator has accumulated enough of its own
    state that writing it out actually returns something. Foreign pressure
    alone can no longer trigger one.
  - It doubles as hysteresis. A whole-table drain resets the footprint to
    ~0, so the next drain waits for a floor's worth of regrowth; a partial
    partition drain leaves array capacity in place (the SoA arrays keep
    their len/cap and reconcileGroupMemory only ratchets upward), so the
    floor is measured against that retained footprint and successive
    drains are spaced by real growth rather than by batch arrival.

In-memory state stays bounded by (post-drain footprint + floor), and the
run count by (total state / floor) instead of one run per batch.
