# Two level int index conversion

Source: internal/engine/exec/two_level_hash.go — convertIntHashTableToTwoLevel, moved 2026-09-11 (#1026)

convertIntHashTableToTwoLevel rebuilds a flat int index as a bucketed one
and releases the flat table's entry array. The flat table is left empty
and must not be used afterwards.

The destination has EXACTLY the flat table's slot count, split 256 ways.
Two things follow. The conversion is then a pure re-permutation into the
same number of slots — by the bit budget above, (bucket, slot) is
bit-for-bit the index the flat table of that size computes — and, called
where convertsToTwoLevel says (the flat table one batch from its load
factor), the doubling the flat table was about to perform as ONE
whole-table rehash instead happens as 256 per-bucket rehashes, each of
them cache-resident. That is the structure's whole claim, applied to the
one rehash that was already due.

Sizing the destination at the DOUBLED capacity instead was measured and
rejected: it removes the per-bucket doublings, but only by moving them
into the conversion's own scatter, which then works over twice the bytes
and is DRAM-bound. On the Q18 capped-epoch shape that cost +7 points of
wall against this sizing (BenchmarkAggIntCappedEpochs, near-unique 16M).

The insert loop is written out rather than calling GetOrInsertAt: the
source keys are unique by construction, so there is no duplicate to test
for.
