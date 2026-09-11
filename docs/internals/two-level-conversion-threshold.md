# Two level conversion threshold

Source: internal/engine/exec/two_level_hash.go — twoLevelConvertAt / twoLevelConvertPolicy, moved 2026-09-11 (#1026)

twoLevelConvertAt is the live-entry count at which a flat group index
converts to the bucketed form. A var so benchmarks and tests can move it;
production never writes it.

ClickHouse converts at 100K. On THIS stack that is too early, because our
flat table does not have the costs 100K is meant to dodge: its entries live
in a MAP_NORESERVE reservation (ADR-0006 amendment), so a doubling is one
mmap the kernel backs with huge pages, never a Go-heap allocation, and up to
a few million entries the whole table is still L3-resident.

Measured through the real consume path on a 5900X
(BenchmarkHashAggregateHighCardTwoLevel, near-unique keys, COUNT+SUM+AVG,
one interleaved window, min of 5):

	shape              groups   flat     bucketed   delta
	single int64          8M    783 ms    758 ms    -3.2%
	packed two-int64      8M   1144 ms   1170 ms    +2.2%
	single int64          1M     76 ms     88 ms    +16%   (see below)
	packed two-int64      1M     99 ms    131 ms    +33%   (see below)

And on the index alone (BenchmarkIntIndexConvertThreshold, fill from empty,
off-heap backing, conversion forced at 100K so the sweep shows the
STRUCTURAL crossover rather than this threshold):

	entries    256K   512K    1M     2M     4M     8M    16M    32M
	flat       2.81   6.93   21.6   60.6  140.6  296.7  628.6 1289.8  ms
	bucketed   5.16   9.66   25.3   61.2  141.2  290.6  619.0 1268.6  ms

Two things follow. First, the crossover is a few million entries — that is
where the flat rehash stops being a cache-resident scatter — so converting
at 100K would tax every mid-cardinality GROUP BY for nothing. Second, the
1M rows above are NOT the steady state: at exactly the threshold the table
converted on its last batch and paid a whole conversion rehash (~30 ns per
live entry) with nothing left to amortize it. That was the irreducible
worst case of a bare size threshold, and it is what
convertsToTwoLevel's load-factor test removes: a table that settles just
past the threshold never crosses its load factor, so it never converts.
The default stays at 1M so that everything below a million
groups per sink — which is every TPC-H shape and most ClickBench ones —
keeps the flat index unchanged. ClickBench Q33 is ~6M groups per
partitioned sink and converts.

The second, harder-to-benchmark half of the case is the growth transient:
a flat doubling holds old+new live (1.5x the final table) at exactly the
moment memory is tightest, while a bucketed doubling holds one bucket extra.
Measured peak RSS filling 32M int keys: flat 1809 MB, bucketed 1685 MB.
That margin is the GOMEMLIMIT class from the Q33 postmortem, not a
throughput number.

WADJET_TWO_LEVEL_AT overrides it. That is not a tuning knob for operators:
it exists so the invariance oracle and the differential harness can drive
the bucketed path on corpora whose group counts are nowhere near a
million, which is otherwise the only way this code stays dark in CI.

R* (twoLevelMinAmortizeRows) scales with this override too, since it is
defined as a multiple of twoLevelConvertAt rather than an absolute row
count — so lowering WADJET_TWO_LEVEL_AT does not by itself guarantee the
DAG's unbounded-final-aggregate path (SetInputRowBound,
twoLevelAmortizeMultiple) reaches the bucketed layout: a small corpus's
per-task row count can still land below the scaled-down R* and get pinned
flat there, going dark. A DAG corpus run that wants bucketed coverage
under this override must also set WADJET_TWO_LEVEL_ROW_BOUND=0 to bypass
that pin outright.

Overriding it also switches conversion to EAGER — the size test alone
decides, without convertsToTwoLevel's load-factor lookahead. Both halves
of the shipped gate have to relax together for the override to do its job:
a corpus whose tables never reach a million groups is also a corpus whose
tables never reach a doubling, so a low threshold on its own would leave
the conversion, and everything downstream of it, dark. What eager mode
changes is WHEN the index converts, never WHAT it holds — the conversion
is value-preserving, so the oracle's row sets are the same either way and
its coverage of the bucketed path is strictly larger. Production, with no
override, always takes the load-factor rule (and
TestTwoLevelConvertsAtTheDoubling pins it there).
