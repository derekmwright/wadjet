# Two level epoch cap bound

Source: internal/engine/exec/two_level_hash.go — twoLevelBoundedMinGroups, moved 2026-09-11 (#1026)

twoLevelBoundedMinGroups is G* — the group count a BOUNDED sink's epoch
must be able to reach before the bucketed layout is allowed at all.

A bounded sink is one whose owner finalizes it and builds a fresh one
every C bytes of state (worker.cappedPartialAgg, C = 128 MB). Its index
never outlives one epoch, so a conversion has nothing after it to
amortize against: the flat→bucketed rehash is paid once per epoch, in
full, near the epoch's end, on a table that is about to be thrown away.
That is not a threshold to tune — it is a property of the operator's
configuration, and it is known before the first row arrives.

DERIVATION (SF100 TPC-H Q18, three same-window arms 2026-08-22,
scratchpad/window-analysis-2026-08-22.md §1; ClickBench 3-arm run):

		Gmax = C / s, s = per-group state (perGroupStateBytes)

	  - Q18's exchange partial aggregate: C = 128 MB, s ≈ 46 B
	    ⇒ Gmax ≈ 2.9 M. Measured groups per flush on the arm with the
	    bucketed layout DISABLED: 12 497 812 out_rows / 5 flushes = 2.50 M.
	    At that Gmax the bucketed layout costs the stage 3-4×: mean task
	    2.25 s (flat) → 6.96 s (old gate) → 10.13 s (load-factor gate), and
	    the conversion itself measures ~675 ns per live entry in production
	    — 22-27× the 25-30 ns the structure was calibrated on, because 8-10
	    tasks run the scatter concurrently against one shared L3.
	  - The bucketed layout's measured wins are all UNBOUNDED sinks that keep
	    one index for the whole input: ClickBench's high-cardinality GROUP BYs
	    (~6 M groups per partitioned sink on Q33) and the 16 M near-unique
	    arm of BenchmarkAggIntCardinalitySweep (−4.1 % vs flat). At 4 M groups
	    the same sweep measures the bucketed arm +31 % — a LOSS — so 4 M is a
	    floor on where bucketing could pay even with a full unbounded tail to
	    amortize against, and a bounded sink has no tail at all.

So G* = 4 M: at or below it every measurement of the bucketed layout is a
loss or a wash, and only above it is there a measured win. With today's
128 MB cap no bounded sink reaches it (Gmax tops out around 3.5 M for the
cheapest possible per-group state), which makes the rule equivalent to
"bounded ⇒ flat" in production while keeping the door open for a future
larger C. It is deliberately NOT compared against twoLevelConvertAt (1 M):
that is a live-count crossover for a table that will keep growing, and a
bounded sink's table by construction will not.
