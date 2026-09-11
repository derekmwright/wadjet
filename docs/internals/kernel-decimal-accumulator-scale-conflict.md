# Kernel decimal accumulator scale conflict

Source: internal/engine/exec/kernel/types.go — DecScaleConflict bool, moved 2026-09-11 (#1026)
Superseded: File/catalog DECIMAL declaration reconciliation now exists in parquet.DecimalRescale and the scan paths; the claim that scans mix scales without checks is stale.

DecScaleConflict marks an accumulator handed two DECIMAL values at
DIFFERENT scales. The Int128s it carries are unscaled integers counted
in ONE scale, so 12.75 (1275 at scale 2) added to 0.1275 (1275 at scale
4) is 2550 under whichever scale wins — 25.50 or 0.2550 depending on
arrival order, never the 12.8775 that is the answer. It rides the
accumulator beside DecOverflow, for the same reason and through the same
emit-time channel (exec.aggEmitErr), because there is no other way for a
kernel with no error return to refuse.

It is one door of several, not the last one, and the difference matters
because the first draft of this comment claimed otherwise. The planner
reconciles a set operation's arms (#533), the shuffle writer refuses a
cross-scale chunk, and the shuffle reader refuses a cross-scale stage
input (#685) — those cover the producers that exist. This covers the
UNGROUPED accumulator; the GROUPED paths keep their state in the flat
SoA arrays, which hold one scale per aggregate and no per-group
accumulator to carry a flag on, so their latch is
exec.HashAggregate.decScaleConflict instead and reaches the same
aggEmitErr. What NEITHER covers is the SCAN: two base-table files whose
footers declare one column at two scales are read, mixed and answered
with no check anywhere, on every path including the fast one. That is a
scan-level schema-drift check and a pre-existing residual — recorded in
ADR-0010 rather than fixed here.
