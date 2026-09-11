# Raw row spill container values

Source: internal/engine/exec/aggregate_raw_spill_container.go — encodeContainerColsForSpill / decodeContainerColsFromSpill, moved 2026-09-11 (#1026)

The legacy raw-row aggregate spill (memory.SpillManager.SpillRows) writes
one boxed value per column and has typed arms only for bool/int/float/
string. A container box — []any (ARRAY, and a MAP as its list of entry
ROWs), map[string]any (ROW) or []float32 (VECTOR) — falls to its default
arm, which renders it with fmt.Sprintf and stores the DISPLAY text. On the
way back that text is a string, and batch.FromRows refuses to write a
string into a container vector (#361's silent-write guard), so a GROUP BY
over a container column plus any non-simple aggregate — the shapes
canUseExternalMerge returns false for — failed outright the moment the
spill buffer flushed to disk (#611).

#566/ADR-0023 already gave the PARTIAL-STATE drain a lossless container
VALUE codec (appendContainerKeyValue / decodeContainerKeyValue); this is
its sibling site. Rather than teach the memory layer about containers (it
imports neither batch nor this package), the raw-row path encodes a
container box to that codec's bytes BEFORE handing rows to SpillRows and
decodes it back AFTER ReadSpilledRows, so the box the row carried is
reconstructed EXACTLY — the same producer, one definition of a container
value. The encoded bytes ride as a string through SpillRows' existing
length-prefixed string tag, which round-trips arbitrary bytes; the emit
then reconstructs the value through the identical batch.FromRows the
un-spilled buffered drain uses, so spilled equals in-memory by
construction, exactly as ADR-0023 requires.
