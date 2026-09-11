# Exact window frame slide

Source: internal/engine/exec/window_decimal_agg.go — exactFrameAcc.slide, moved 2026-09-11 (#1026)

slide advances the accumulator from its current frame to [lo, hi).

The ORDER is the correctness-relevant part, and it is retract-then-add.
Adding first means the accumulator transiently holds
sum(previous frame + arriving rows) — a value that belongs to NEITHER
frame — and for an exact carrier that transient can leave the range and
refuse a query both spellings answer: three DECIMAL(38,0) rows of 9x10^37
under `ROWS BETWEEN CURRENT ROW AND CURRENT ROW` held 1.8x10^38 between two
frames that each hold 9x10^37. Retracting first bounds every intermediate
by a PREFIX of the target frame, so the only overflow left is one the
frame's own rows produce — which is exactly what the grouped SUM over those
rows reports (ADR-0012 item 9).

DISJOINT frames reset instead of retracting to empty. When lo has passed
the last row this accumulator added, nothing carries over, and walking the
subtraction chain down to zero would re-introduce intermediates unrelated to
either frame (removing a large negative row from a total near the ceiling
overflows on the way out). Resetting is exact, cheaper, and — because every
frame bound is non-decreasing in the row index — costs O(sum of frame
widths) over the partition, which is bounded by the partition's own length:
a frame disjoint from its predecessor advances lo by at least its own width.

Both directions stay CHECKED even so. A retract is a subtraction of a value
the accumulator already holds, and an unchecked one would let a wrapped
intermediate become a plausible-looking total; the flag it raises is not
final, since windowExactFrames recomputes the frame before refusing.
