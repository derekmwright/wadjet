# Worker partial agg schema intersection

Source: internal/worker/shuffle_partial_agg.go — func (p *cappedPartialAgg) resolveAgainst(b *batch.RecordBatch) {, moved 2026-09-11 (#1026)

resolveAgainst intersects the configured keys/specs with the actual
batch schema (see the type comment). Called once, on the first batch.

Presence is decided by batch.ResolveSchemaIndex, not by a byte-exact set
probe, and a name that resolves is REWRITTEN to the schema's spelling. The
declared keys and specs are plan-side names, so they can arrive in the
lexer's folded spelling (#731) while the stream carries the catalog's own —
`RegionID`. Byte-exact, a MIXED-case schema (`RegionID` beside an
already-folded `counterid`) is the case that hurts: the folded names are
present, the CamelCase ones are not, so the intersection drops PART of the
grouping and this operator pre-combines rows belonging to DIFFERENT groups
— a wrong number the consumer cannot detect, where a total miss merely
disables the pre-combine. Carrying the schema's spelling forward keeps the
flushed WSHF payload in the same spelling as the raw one it replaces, which
is the contract the type comment states (specs are name-preserving) and
what the downstream by-name resolution assumes.

The camel-case invariance battery does not yet distinguish this site:
measured with every other fix in place and this one reverted, it owns 0 of
its cells, because its aggregate shapes reach the exchange with keys the
scan already spelled the schema's way. The change is the hazard closed, not
a cell recovered.
