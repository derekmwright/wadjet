# Aggregate shared routing hash

Source: internal/engine/exec/partitioned_agg.go — hashOnceToggle, moved 2026-09-11 (#1026)

--- hash once: one key hash feeds partition selection AND slot indexing ---

Every row used to be hashed TWICE under partitioned aggregation: the router
hashed the whole key to pick an owner, then the owning sink's table hashed
the same key again to pick a slot. On ClickBench Q34 that is the same ~88
byte URL through two full hash passes per row.

hashOnceToggle threads the router's hash to the sink instead. The two
consumers read DISJOINT bit windows of the same 64-bit value — bit 63 down
to bit 0, partition at the top, slot at the bottom, untouched bits between:

partition owner = partitionFor(hash, parts), the high half of hash*parts,
i.e. a function of the TOP ceil(log2(parts)) bits (exactly
`hash >> (64 - radixBits)` when parts is a power of two, DuckDB's Shift).

table slot = hash & (cap-1), the LOW log2(cap) bits — byte-for-byte what
every table already did.

parts is one per worker (<= 4096 in any plausible deployment) so the
partition window is at most 12 bits; a table masking 40 bits would need
24 TiB of entries. The windows never meet. Nor does partitioning skew what
is left: fixing the owner confines the hash to a contiguous interval of
2^64/parts >= 2^52 values, over which the low 40 bits are still uniform.

WHICH function is unified matters. We adopt the SINK's, per key shape:

	int64 key   fibHash(key) — a high-bits fold, then the phi multiply. The
	            multiply's low bits are a bijection on ITS input's low bits,
	            and the fold leaves a dense integer range injective there, so
	            dense ids keep the collision-free sequential slot layout
	            intHashTable was built around while a key set of multiples of
	            2^s no longer collapses onto one chain (#306). The top bits
	            depend on every input bit, which is the textbook use of a
	            multiplicative hash and what the partitioner takes.
	packed key  packedHash(lo, hi) — the 128-bit fold in packed_hash.go.
	string key  strHash(key), whose low 32 bits are also strEntry.hashTag, so
	            threading it skips the tag computation too.

Going the other way (tables adopt the router's mix64/fnv1a) would have
re-spread every table's slot distribution and cost the int path its
sequential locality, for no gain. This way the tables see bit-for-bit the
slot sequence they saw before; only the second hash pass disappears.
