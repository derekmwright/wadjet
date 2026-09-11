# Two level group index

Source: internal/engine/exec/two_level_hash.go — twoLevelToggle / intTwoLevelTable / packedTwoLevelTable, moved 2026-09-11 (#1026)

Two-level (bucketed) group index — G6 in
docs/benchmarks/high-card-aggregation-gap-2026-08-17.md.

A flat open-addressing table grows by WHOLE-TABLE rehash: allocate 2x,
memset it to the empty marker, then scatter every live entry into random
slots of an array far larger than any cache. A 100M-group aggregate pays
~8 of those, the last few touching tens of millions of entries with old
and new tables both live. ClickHouse converts to a 256-bucket two-level
table past ~100K keys and DuckDB radix-partitions for the same reason:
afterwards a rehash touches 1/256 of the data, stays cache-resident, and
the buckets are independent — merge and emit can walk them in parallel,
and sizing no longer needs a cardinality estimate.

This file holds the int and packed key modes' bucketed indexes. The
string mode keeps its flat table for now (see "String mode" below).

# Bit budget (the one place it is written down)

Every group key yields ONE 64-bit hash — fibHash for single-int keys,
packedHash for composite keys (G5's hash-once, partitioned_agg.go). Three
independent consumers read DISJOINT windows of it:

	 63                        52 51                  8 7            0
	+----------------------------+----------------------+-------------+
	| partition owner            | sub-table slot       | bucket      |
	| top ceil(log2 parts) bits  | log2(subcap) bits    | 8 bits      |
	+----------------------------+----------------------+-------------+
	  partitionFor(h, parts)       (h >> 8) & (cap-1)     h & 255

PARTITION OWNER — unchanged from G5: the high half of h*parts (Lemire
multiply-shift), a function of the top ceil(log2(parts)) bits. parts is
one per worker, <= 4096 in any plausible deployment, so the window is at
most 12 bits.

BUCKET — the LOW 8 bits, and the sub-table's slot is what USED to be the
low bits, shifted up by 8. So (bucket, slot) together are exactly the low
8+log2(subcap) bits of the same hash: the identical index a FLAT table of
256*subcap slots would compute. Two keys collide in the two-level table
iff they would have collided in that flat table
(TestTwoLevelMatchesFlatCollisions). Everything the flat tables' spread
rests on carries over unchanged, including fibHash's collision-free
bijection on dense integer ids.

SLOT — bits (7+log2(subcap))..8. Disjointness with the partition window
holds while 8+log2(subcap) <= 52, i.e. up to 2^44 slots in a SINGLE
sub-table (16 PiB of entries). Enforced by construction: growSub refuses
past twoLevelMaxSubBits.

WHY THE BUCKET IS THE LOW WINDOW (measured, not assumed): the obvious
choice — a middle window like bits 39..32, which is what ClickHouse's
`hash >> (32 - 8)` amounts to — is correct only for an avalanching hash.
fibHash is not one: it folds the key's high bits down and then multiplies
by phi, deliberately KEEPING the multiply's collision-free bijection on the
low bits for dense integer ids (see fibHash, where avalanching was measured
and rejected at +47% geomean). So picking keys by any high-ish window
selects a near-arithmetic subsequence of a dense key range. Simulated on
33M dense int keys: bucket=bits 39..32 gave 6.46 average probes per insert
at 8M keys and degrades with scale, while bucket=low 8 bits gives exactly
1.00 at every size — the flat table's own number. Random and
packed-composite families measure 1.50 either way. The low window is the
only one that inherits the flat table's guarantees instead of replacing
them with new ones.

The low window is also why #306's stride collapse hit this table as hard as
the flat one — a key set of multiples of 2^s had constant low bits, so it
landed in ONE bucket on ONE chain. fibHash's fold fixed both at once.

# Layout is decided at construction where the sink's bounds allow it

Before any of the adaptive machinery below runs, a sink is born FLAT and
never converts when either construction-time bound says a conversion could
not be repaid. Both are properties the sink's owner knows before the first
row, and both are one comparison in HashAggregate.indexLayoutStaysFlat:

  - EPOCH BYTE CAP (twoLevelBoundedMinGroups). A sink whose owner finalizes
    and rebuilds it on a byte cap — the shuffle sender's exchange partial
    aggregation, worker.cappedPartialAgg — holds at most C/s groups and its
    index cannot outlive one epoch, so a conversion has nothing after it to
    amortize against.
  - INPUT ROW BOUND (twoLevelAmortizeMultiple). An UNBOUNDED sink whose
    owner knows exactly how many rows it will read — a DAG aggregate task
    reading a known set of upstream shuffle partitions — will pass fewer
    than R* rows through the index in total, so it cannot have R* − the
    conversion threshold left after the earliest conversion point. This is
    the Q18/Q20 `final_aggregate` shape: rows ≈ groups, one probe per
    group, the conversion firing at the last doubling.

Everything below applies to sinks with NEITHER bound — a standalone or
single-process aggregate that knows only its own live counters.

# Adaptive conversion, not construction

An unbounded sink starts FLAT, and converts AT THE POINT WHERE THE
FLAT TABLE WOULD HAVE REHASHED ITSELF ANYWAY — see convertsToTwoLevel
(aggregate.go) for the two tests and twoLevelConvertAt for the measured
curve behind the size one. The decision runs once per BATCH, never per row:
the consume loop hoists its table pointer for the whole batch and the
conversion lands at the batch's end. Aggregates whose NDV hint already
exceeds the threshold construct bucketed directly (resolveIndices) and never
pay a conversion at all. Below the threshold nothing changes: no bucket
indirection, no extra shift, byte-identical behavior to G5.

The "would have rehashed anyway" half is load-bearing and was NOT true in
the first version of this file, which converted on the first batch-end past
a live-entry threshold. That point falls, on average, halfway between two
doublings, so the conversion's scatter REPLACED NOTHING: the flat table
still owed its next doubling, the bucketed table paid it as per-bucket
growth, and the conversion was pure additional work. Measured on SF100
TPC-H (release v0.16.0-correctness, merged 3-worker CPU profile) that came
to ~79.6 CPU-s per suite run of conversion rehash against ~7.7 CPU-s of
two-level probe benefit — ≈10:1 — concentrated in the shape that can never
veto a growth-rate test: a NEAR-UNIQUE key, where every row mints a group
(Q18's GROUP BY l_orderkey over 150M lineitem rows, +87% in that release).

Converting at the load-factor crossing instead pays the conversion INSTEAD
OF that doubling rather than on top of it, and the destination is the flat
table's own slot count split 256 ways, so the doubling it displaced then
happens as 256 per-bucket, cache-resident rehashes rather than one more
whole-table scatter. A table that is not about to rehash is a table with
nothing to save, so it stays flat — which also retires the old growth-rate
heuristic: a saturated table cannot cross its load factor, so it can no
longer convert at all.

Sizing the destination at the DOUBLED capacity was tried and rejected on
measurement. It looks like the tidier "replace grow() exactly" — the
bucketed table is then born at 35% load and no bucket regrows — but it
only moves the per-bucket doublings into the conversion's own scatter,
which then works over twice the bytes and is DRAM-bound instead of
cache-resident. Same-window A/B on the Q18 capped-epoch shape
(BenchmarkAggIntCappedEpochs, near-unique 16M, n=5 medians): flat
2037 ms, doubled-capacity 2204 ms (+8.2%), flat-capacity 2058 ms (+1.0%).
The scatter is the expensive part of a rehash, and the bucketed form's
whole value is that its scatters are small.

What the conversion does NOT touch: group ids stay dense global indices
into the same flat accumulator arrays and the same key SoAs, so emission,
the spill drain cursor, the partial-state run format and the merge all see
exactly what they saw before. Only the INDEX is two-level.

# String mode

strHashTable is not converted here. Its keys live in a chunked arena
shared by the whole table and its entries carry a 32-bit hashTag; a
two-level split needs either a per-bucket arena (256 chunk lists, and
every arenaString alias must stay valid across the split — they do, the
chunks are append-only and never move, so the conversion can hand each
bucket the SAME chunk list and only re-index) or an arena that stays
global while only the entry array splits (simpler: the arena is already
chunked, so it is not the thing that rehashes). The tag is a stored
value, not an index window, so the low-8-bit bucket does not disturb it.
Deferred to keep this change to the two modes that dominate Q33.
