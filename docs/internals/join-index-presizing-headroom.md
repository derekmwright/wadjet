# Join index presizing headroom

Source: internal/engine/exec/join.go — HashJoin.preSizeRowHint, moved 2026-09-11 (#1026)

preSizeRowHint is how many build rows the arena and hash index may be
pre-allocated for, which is NOT the same question as how many rows the build
expects (#823).

BuildRowHint is the planner's estimate of the WHOLE build. Pre-sizing to it
charges the tracker — through reconcileHashMemory's ForceReserve, which
cannot fail and has no ceiling — for capacity that holds nothing yet: a
5,000-row hint put 191,072 bytes on the ledger on a batch of 20 rows, 36% of
a 512 KiB budget, before the build had stored anything. Every later Reserve
in the query was then measured against a floor that described a build that
had not happened, and the query refused for want of room the join was only
holding a reservation on.

So the pre-size is bounded by the room that EXISTS when the build starts —
what the budget still has, less the arrival batch that is about to be
charged. Sizing it that way is what keeps the pre-allocation from being the
charge that crosses the line: it can only claim room that is free and that
nothing else is already committed to. The structures grow on demand past the
cap (both hash tables round up to a power of two on CheckGrow; the arena
appends), so it costs a few rehashes on a build genuinely bigger than its
budget's headroom, and costs nothing at all when there is room — on an
unbudgeted tracker, or any budget with headroom to spare, this returns the
hint unchanged and no pre-allocation changes.

The other half of #823 — the index for rows that HAVE arrived being
unreleasable — is fixed by per-partition index state (join_index_parts.go),
so a build that spills does now give its index back.
