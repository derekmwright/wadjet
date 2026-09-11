# Grouped decimal clone scale merge

Source: internal/engine/exec/agg_partial_merge.go — HashAggregate.MergeSink, moved 2026-09-11 (#1026)

The DECIMAL scale upgrades on EVERY merge, not only the first one.

It is the one piece of that metadata a clone can report WRONG rather
than not at all: a clone whose morsel held no non-NULL value observed a
vector but contributed nothing, so it reports scale 0 — and inheriting
from that clone alone declares the output at scale 0 while the merged
accumulator counts in the column's, which renders 4.00 as "4". This is
the "prefer the first NONZERO observation" rule Consume already applies
ACROSS BATCHES (#455) applied ACROSS CLONES: it costs a genuinely
scale-0 column nothing, because every clone of one then reports 0.

The inherit above copies rather than aliases so this loop cannot write
through into the clone it inherited from.

And the clone seam is where the GROUPED cross-scale pair is DETECTED, not
only where the flag travels. Consume's latch compares a batch against the
scale its own operator established, so two clones each fed one scale each
see one scale and neither latches anything; without the second arm below
this loop then took the primary's and DISCARDED the disagreement, and
`p.Consume@2 + c.Consume@4 + MergeSink` answered 25.50 where the
ungrouped form raises 22003 (#685 review, item A, second pass).

A known inconsistency, recorded rather than fixed because no producer in
the tree reaches it: a genuinely scale-0 batch followed by a scale-2 one
is an UPGRADE on this path (5 at scale 0 summed with 1.00 gives 1.05)
where the ungrouped adoptDecScale calls the same pair a conflict. The
"first NONZERO" rule is what #455 needs for the identity row, and it
cannot tell that row's absent scale from a real DECIMAL(p,0).
