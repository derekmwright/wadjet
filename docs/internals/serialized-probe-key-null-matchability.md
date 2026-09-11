# Serialized probe key null matchability

Source: internal/engine/exec/join.go — HashJoinProbe.buildProbeKey, moved 2026-09-11 (#1026)

buildProbeKey fills p.keyBuf with the serialized probe key for a row and
reports whether that key may MATCH. Uses the per-probe keyBuf to avoid races
when multiple cloned probes execute in parallel.

A row holding a NULL in any key column reports false: SQL's `=` is UNKNOWN
against a NULL, so an equi-join must not pair it with anything — not even
with another NULL. The key bytes are still filled in, because the partition
router (probePartition, join_spill.go) needs a deterministic partition for
every row including that one, exactly as the integer paths return partition
0 for a key they refuse to match.

Without the flag, a NULL serialized to a lone 0x01 flag byte with no
payload, so two NULL rows produced IDENTICAL key bytes and the string hash
table — which matches keys by byte equality — joined them. The integer fast
paths (intProbeKey, dualIntKeyFromVectors) have always refused a NULL key,
so which answer a query got depended on whether its key columns happened to
be integers (#459).

An UNRESOLVABLE key column (idx < 0) is deliberately NOT a NULL here: it
keeps its flag byte and its matchability, because folding it in would turn
a join whose key column is missing from the probe schema from "matches
everything" into "matches nothing" — a different bug, in a different place.
