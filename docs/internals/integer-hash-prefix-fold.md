# Integer hash prefix fold

Source: internal/engine/exec/int_hash.go — fibHash, moved 2026-09-11 (#1026)

fibHash mixes an int64 key into the 64-bit hash every int-keyed table in
this engine indexes by.

It used to be the Fibonacci multiply alone — `key * phi` — and a
multiply-only hash has one bit-level property that matters here: the low k
bits of the product depend ONLY on the low k bits of the key. So a key set
whose members are all multiples of 2^s shares the same low s bits, and in
any table with at most 2^s slots every one of them lands on the SAME slot:
one probe chain, O(n) per lookup, a GROUP BY that degrades from linear to
quadratic. That is not a contrived input — an id column allocated in
strided blocks, a timestamp truncated to a minute or hour boundary, and a
byte-aligned offset all produce it (#306). The property was DOCUMENTED
in-tree rather than fixed, because the obvious fix gives up the other half
of the multiply-only behaviour: for DENSE keys 0..n-1, `key * phi mod 2^k`
is a bijection, so sequential ids collided exactly zero times.

The mixing step is a PREFIX-XOR fold of the key's high bits into its low
ones, applied BEFORE the multiply. Four xorshift-rights, each of them a
bijection on 64 bits, so the composition with the multiply is a bijection
too (TestFibHashIsInjective): two distinct keys never share a hash, and the
only collisions left are the birthday collisions of the truncation to
slots.

# Why a fold and not a real avalanche

murmur3's fmix64 (what DuckDB uses) was the first attempt. It avalanches,
so it fixes every stride — but it also gives up the dense-key bijection,
and that bijection is worth keeping: at the tables' 70% load factor, linear
probing's UNSUCCESSFUL-search cost, which is what an INSERT pays and
near-unique aggregation is all inserts, goes from 1.00 probes to about 6.
The fold keeps both, and the evidence is the probe count itself — a
deterministic number, unlike this package's aggregate wall-clock
benchmarks, whose run-to-run variance on a loaded host reaches ±200% even
on arms this function cannot touch (the string and packed key shapes hash
elsewhere).

Simulated over 2^20 keys in 2^21 slots, average probes per insert:

	family        dense  +1e9  ×3    2^4    2^8   2^12  2^16   2^24    random
	bare multiply  1.00  1.00  1.00  4.50   64.5  1024  16384  524288  1.50
	this fold      1.00  1.00  1.19  1.00   1.73  2.05  1.00   1.00    1.50
	fmix64         1.50  1.50  1.50  1.50   1.50  1.50  1.50   1.50    1.50

with truncated timestamps (×60000, ×3600000) going 8.50→1.54 and 32.5→1.50.
A dense key below 2^7 is untouched by any of the four shifts, and over a
dense RANGE the fold stays injective on the low log2(slots) bits, which is
what preserves the multiply's bijection there.

TPC-H SF1 confirms no wall-clock cost, interleaved in one window, three
runs each: bare 59.41 / 58.91 / 57.61 s, this 58.06 / 56.95 / 58.83 s.

The partition bits — which partitionFor takes off the TOP of the same word,
where the multiply already spreads well — are unaffected, so hash-once can
still route and index from one value (TestHashSpreadFamilies). The fold is
LINEAR, not an avalanche, so for a strided family the top and low windows
stay mildly correlated; TestTwoLevelThreeWindowSpread's ownerBucketSkew
records the 1.8× bucket skew that leaves.

Not gated by an optswitch toggle: a hash cannot change a query's row SET,
only the ORDER an unordered GROUP BY emits its groups in, and a toggle over
that would hand the optimization-invariance oracle a false divergence to
chase. Bisecting it means deleting the four shifts.
