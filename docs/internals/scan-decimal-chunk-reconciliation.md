# Scan decimal chunk reconciliation

Source: internal/engine/scan/columnar_native.go — func rescaleDecimalChunk(vec *batch.Vector, leaves []*pqt.SchemaNode, colIdx, offset, precision int) error {, moved 2026-09-11 (#1026)
Superseded: Timestamp unit conversion can truncate sub-millisecond values; it is not generally exact division. DECIMAL reconciliation also validates declared precision even at equal scale.

rescaleDecimalChunk moves a just-decoded DECIMAL chunk from the scale the
FILE declares to the scale the CATALOG does — the same shape as
rescaleTimestampChunk above, and for the same reason: the values a column
chunk carries mean what a DECLARATION says they mean, and the file's
declaration is input rather than fact (ADR-0018).

For a TIMESTAMP the two declarations can only differ by a power of ten that
always divides exactly, so that one cannot fail. A DECIMAL can: the catalog's
scale may need digits the carrier has no room for, and the answer then is
PostgreSQL's 22003 rather than a wrapped number (#707).

NULL slots are SKIPPED, and the per-row test that costs is the point.

The first version of this rescaled them, on the argument that a null cell's
carrier is not a value and that "zero is the only carrier the batch allocator
puts there". That argument is FALSE: the scan reuses batches through a
BatchPool, and a reused batch hands back the previous file's carriers in
slots the new file marks NULL. So a file whose every VISIBLE value is fine
was refused — one file holding the widest DECIMAL(15,2) carrier followed by
an all-NULL file declared at scale 0 raises 22003 on the 0-to-2 multiply of a
number no query can see. Round 0's review flagged the premise as ungated and
could not break it end to end; asserting it directly
(TestPooledBatchesHandBackZeroedDecimalSlots) showed it never held.

A null cell's carrier is unspecified, so it must not decide whether a file
reads. The branch runs only on the repair path, which by definition is a file
this writer did not produce.

Non-inlined for the frame-size reason columnDecodePlan's comment gives: this
sits on the stack of every per-column errgroup goroutine.

go:noinline
