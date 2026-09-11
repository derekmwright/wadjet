# Ohlcv mergeable bar state

Source: internal/engine/exec/agg_ohlcv.go — ohlcvState, moved 2026-09-11 (#1026)

OHLCV — one MERGEABLE aggregate state that answers a whole bar.

	ohlcv(ts, price, volume) -> ROW(open, high, low, close, volume, vwap)

The pattern is ADR-0035's and this is its first instance: a state whose
merge is ASSOCIATIVE and COMMUTATIVE is a state that can be computed
per-task and combined, which is what lets one bar cross the stage DAG, the
spill runs and the shuffle without the operator that finishes it ever seeing
a raw row (ADR-0010's merge form, the shape varianceState and covarianceState
already take).

	n        rows folded in; 0 means the group is EMPTY and the bar is NULL
	firstTS  the OPENING row's instant, epoch millis
	firstPx  the price at that instant
	lastTS   the CLOSING row's instant
	lastPx   the price at that instant
	high/low max / min price
	sumVol   Σ volume
	sumPV    Σ (price × volume)

merge:

	n      = a.n + b.n
	first  = the (ts, px) LEXICOGRAPHIC MINIMUM of the two
	last   = the (ts, px) LEXICOGRAPHIC MAXIMUM of the two
	high   = max(high)          low = min(low)
	sumVol = sum               sumPV = sum

**The tiebreak is a VALUE.** Two rows sharing an instant have no order a
query can see: a row POSITION is not observable across the arms — the single
path reads one file, the DAG reads four in whatever order tasks finish — so
picking "the first one that arrived" would make the bar depend on the plan.
`open` is therefore the price of the row with the smallest (ts, price) and
`close` the price of the row with the largest, which is exactly PostgreSQL's

	(array_agg(px ORDER BY ts, px))[1]        -- open
	(array_agg(px ORDER BY ts DESC, px DESC))[1]  -- close

and that spelling is the value oracle for every cell of the gate.

**NULL rule.** A row is skipped when ANY of ts, price, volume is NULL —
PostgreSQL's rule for a multi-argument aggregate, measured on 17.11:
regr_count(y,x) over (1,1),(2,NULL),(NULL,3),(4,4) is 2, not 4.

**Domain.** Decided ONCE per aggregate from the input columns' declared
types, never per row. It is EXACT — Int128 at a fixed scale, so the sums
carry every digit — unless price or volume is approximate, in which case the
whole state is float64, because one float operand makes the quotient float
on the server too. An exact sum that leaves the 128-bit carrier is 22003,
never a wrapped or narrowed number (ADR-0024).
