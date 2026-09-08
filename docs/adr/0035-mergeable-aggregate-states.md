# ADR-0035: A summarizing aggregate ships a mergeable STATE, not its answer

Status: Accepted (2026-09-08, arc A1, #965 — the OHLCV bar is its first instance)

## Context

`SUM` distributes because a sum of sums is a sum. `MIN` distributes because a
minimum of minima is a minimum. That property — the function's own output is a
valid input to itself — is what the stage DAG's partial-then-merge shape
assumes, and `physical.aggNeedsWholeInput` is the list of functions for which
it is false. Everything on that list is dispatched instead as a one-level
`RawInputAggregate`: every row of a group crosses the exchange, and an
ungrouped one collapses to a single task whose peak memory is the whole column.

Two families already escaped that cost, and they escaped it the same way.
`STDDEV` cannot be re-aggregated from finished standard deviations, but the
triple `(count, mean, M2)` CAN be combined pairwise, so a distributed plan
replaces `STDDEV(c)` with `VAR_STATE(c)` on the partial stage and
`VAR_STATE_MERGE` above it, and folds the triple into the number the query
asked for at the very end (`exec.FinalizeVarianceState`, `worker/var_fold.go`).
`CORR`/`COVAR_*` do the same with a sextuple. Both were written as one-off
repairs for a wrong answer on the DAG (#353, #339); neither was written down as
a pattern.

0.19's domain-native aggregates are all of that shape. A bar, a t-digest, a
HyperLogLog sketch, a TOP_K heap: none of them can be re-aggregated from its
own answer, and every one of them has a state that combines. Writing the rule
down once is what keeps the next four from being four more one-off repairs, and
from each inventing its own encoding, its own merge site and its own fold.

## Decision

**A summarizing aggregate is defined by a STATE with an associative and
commutative merge, and the state is what crosses every boundary. The answer is
computed once, at the end, from a fully merged state.**

Concretely, six things, and all six are the same six every time:

**1. The state is a struct whose merge is a LAW, not a procedure.**
Associative and commutative, over VALUES. The bar's is

    n      = a.n + b.n
    first  = the (ts, price) lexicographic MINIMUM of the two
    last   = the (ts, price) lexicographic MAXIMUM of the two
    high   = max(high)          low = min(low)
    sumVol = sum                sumPV = sum

and it is gated as the law rather than as a pair of examples:
`exec.TestTheBarsMergeIsAssociativeAndCommutative` folds one row set through
200 random partitions merged in random order and compares to folding every row.

**2. Every tiebreak is a VALUE, never an arrival order.** A row POSITION is not
observable across the arms — the single path reads one file, the DAG reads four
in whatever order tasks finish, a spilled run reads them back in run order — so
a state that breaks a tie by "whichever arrived first" answers a different
thing on each, and every answer looks plausible. The bar's `open` is the price
of the row with the smallest `(ts, price)` and its `close` the price of the row
with the largest, which is exactly PostgreSQL's
`(array_agg(px ORDER BY ts, px))[1]` and
`(array_agg(px ORDER BY ts DESC, px DESC))[1]`. Where no value can break the
tie, the aggregate refuses; it does not pick.

**3. The state travels as a FIXED-WIDTH HEX STRING in a synthetic column.**
`varianceState.encode` chose this and its reasoning holds for every member: the
column crosses parquet, the `.wshf` shuffle format and the NATS gather
(ADR-0010), all three of which are happier with text than with arbitrary bytes,
and float64 bits round-trip exactly through hex. Fixed width is load-bearing:
`decode` refuses anything else, so a truncated or foreign value is "no rows to
merge" rather than a plausible empty state.

The bar's is 128 bytes → 256 hex characters, and its first eight bytes are a
HEADER carrying the format version and the DOMAIN — which carrier the numbers
use and at what scales. That is the one thing the variance family did not need
and every typed state does: the coordinator's fold decodes a bar with nothing
but the string, no plan and no catalog.

**4. The synthetic column's name carries the KIND, in a namespace no identifier
can enter.** `__var_state#<kind>#<out>`, `__ohlcv_state#<out>`: the `#` is
illegal in an identifier, delimited or not, so the name cannot collide with a
user's column. One state serves every function that finishes it (three for the
variance family, three for covariance), and which function that is is decided
once, after the last merge.

**5. The DECLARED result comes from ONE function, asked by both the planner and
the operator.** `exec.OhlcvOutputFields(price, volume)` derives the bar's ROW
from the input column declarations, and `physical.aggSpecOutputType` /
`exec.HashAggregate.outputSchema` / the worker's spec conversion all go through
it. A second derivation is how a value comes to be declared one thing and
computed another; `exec.IntegerAccOutputType` is the same shape and exists for
the same reason (ADR-0024).

Each field declares what its OWN spelled-out aggregate declares:

| bar field | rule | measured on PostgreSQL 17.11 |
|---|---|---|
| open, high, low, close | the PRICE column's own type | `min(int4)`→integer, `min(real)`→real, `min(numeric(9,2))`→numeric(9,2) |
| volume | `SUM(volume)`'s type (`exec.IntegerAccOutputType`) | `sum(int4)`→bigint, `sum(int8)`→numeric |
| vwap | `AVG(price)`'s type — a vwap IS a weighted average of price | `avg(int)`→numeric; ADR-0024's +4 scale rule |

**6. The value oracle is the aggregate SPELLED OUT in PostgreSQL, with the same
row filter.** PostgreSQL has no `ohlcv`, but it has every piece of one, and a
multi-argument aggregate there SKIPS a row where any argument is NULL —
`regr_count(y,x)` over `(1,1),(2,NULL),(NULL,3),(4,4)` is 2, measured — so the
spelling carries that filter on every field:

```sql
(array_agg(px ORDER BY ts, px)             FILTER (WHERE ok))[1]  AS open
MAX(px)                                    FILTER (WHERE ok)      AS high
MIN(px)                                    FILTER (WHERE ok)      AS low
(array_agg(px ORDER BY ts DESC, px DESC)   FILTER (WHERE ok))[1]  AS close
SUM(vol)                                   FILTER (WHERE ok)      AS volume
(SUM(px*vol) FILTER (WHERE ok)) / (SUM(vol) FILTER (WHERE ok))    AS vwap
```

with `ok` = every argument IS NOT NULL. A group that keeps no rows answers
NULL — an aggregate over no rows is NULL on the server, and `(NULL::record).f`
is NULL there too (both measured) — so the whole ROW is NULL, not a composite
of empty slots.

Beside that external oracle there is an INTERNAL identity that is sharper
because it needs no rounding: every field of the bar must equal the aggregate
it is spelled as, IN THE SAME QUERY. `(bar).vwap` is
`SUM(price*volume)/SUM(volume)` digit for digit, which is what makes reusing
the engine's own `batch.DecimalDivAt` rather than a float quotient load-bearing.
Gated in `coordinator.TestTheBarIsTheSameOnEveryArm`.

## Consequences

- **The gates a new state must pass** are the six above turned around: the merge
  law over random partitions; the encoding's round trip and its refusal of
  anything else; the tiebreak under permuted fold order; the declared-type
  table against live PostgreSQL; the arm census (single / DAG / DAG-shuffled)
  against the spelled-out oracle; the wire's rendering of the result.
- **Until the decomposition exists, the function goes on
  `aggNeedsWholeInput`.** That list is the honest dispatch for a
  non-re-aggregatable answer, and the two-phase split over one is a silent
  wrong answer — a MAX of two bars is not a bar. A state whose decomposition
  has not been written yet is correct and slow, which is the right way round.
- **The spill needs nothing.** An aggregate carrying an `extraState` does not
  reach the partial-state spill format (`aggregate_partial_spill.go`, whose
  per-aggregate metadata is ten fixed bytes and whose merge is
  `kernel.Accumulator.Merge`); it takes the legacy raw-row spill and
  re-aggregates from input rows in Finalize. That is correct and slower, it is
  what MIN_BY and the variance family already do, and ADR-0027's rule applies
  unchanged: a spill gate proves it spilled.
- **A ROW result declares OID 25 (text) on the wire**, where PostgreSQL declares
  `record` = 2249 for an anonymous composite (measured). That is the whole ROW
  TYPE's declaration and it predates this ADR; it is recorded in ADR-0012's
  divergence list beside ARRAY's (#992). What the bar DID need was for the
  declaration to reach the renderer at all: a composite renders in DECLARED
  field order on the server, and until #965 the only source of that order was
  the CATALOG — which describes no value an aggregate constructs. The result's
  own `wadjet.ColumnMeta.Fields` is that source now.

## Not settled here

- **`(ohlcv(…)).open` inside ONE query block.** A field path's container must be
  a bare column reference (ADR-0022 rule 1, enforced at the parser), so a field
  path over any composite-returning EXPRESSION is refused 42809 — for every
  such expression, not only this one, and PostgreSQL answers the shape. The
  supported spelling is the derived block or CTE, which is gated. Closing it
  needs a `FieldAccess` AST node over an arbitrary expression plus a hoist that
  materializes the aggregate into a hidden slot; that is J1's machinery and an
  arc of its own.
- **`ohlcv(DISTINCT …)`.** PostgreSQL dedupes a multi-argument aggregate on the
  whole argument TUPLE; this engine's distinct set for one is keyed on the
  first TWO columns (`distinctFirstSighting`), so a bar would dedupe ignoring
  the volume. Refused `0A000` until the set is keyed on the whole tuple.
- **The windowed form.** `ohlcv(…) OVER (…)` is refused `0A000` with every
  other aggregate that has no window arm — see ADR-0012's divergence list for
  the census that made the previous fallback indefensible.
- **The partial-state spill format.** Extending
  `aggregate_partial_spill.go` to variable-width states would let the whole
  `extraState` family keep its k-way merge under memory pressure instead of
  re-reading raw rows. It is a format change (the per-aggregate header is ten
  fixed bytes) and belongs to whoever needs the throughput.

## Related

- ADR-0010 (WSHF/WSHC shuffle formats — what a partial state crosses)
- ADR-0012 (PostgreSQL decides semantics; the divergence list)
- ADR-0022 (a ROW field path is not a column reference)
- ADR-0024 (DECIMAL is finite fixed-point with PostgreSQL's result types)
- ADR-0027 (a spill gate proves it spilled)
- `docs/sql-reference.md` §OHLCV, `docs/data-types.md` §ROW
