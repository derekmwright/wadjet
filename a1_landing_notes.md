# ARC A1 — LANDING NOTES (#965: OHLCV + `time_bucket`, ADR-0035)

Branch `arc-a1-ohlcv`, worktree
`/home/dwright/Projects/caelum/.claude/worktrees/agent-af690cabef76a252e`,
base `46ec1bdf` (v0.18.64 tip). Sixteen commits, all `Signed-off-by:` only,
AI trailer count **0**. Tip `2faf5324`.

| sha | subject |
|---|---|
| `037b61ca` | feat(expr): time_bucket is PostgreSQL's date_bin, declared as a TIMESTAMP |
| `e8c25847` | fix(planner): an aggregate with no window form refuses 0A000, it does not crash |
| `254c8021` | feat(exec): ohlcv — one mergeable bar state returning a ROW |
| `4186c79a` | feat(coordinator): the bar's partial STATE crosses the DAG, not the bar |
| `784d6020` | test(exec): a star over a bar, and the bar's per-row cost against MIN_BY |
| `a16da0db` | test(scan): time_bucket does not stand between a predicate and the prune |
| `c6cf3053` | refactor(exec): a bar's merge keeps the DESTINATION's carrier, always |
| `c7c4b3b4` | refactor(worker): one rule for an aggregate's extra argument columns |
| `2dcc3cae` | fix(exec): a bar's encoded state carries the ROW it was computed for |
| `a4477eed` | docs(adr): ADR-0035's state header carries the DECLARED ROW, and why |
| `15e51e58` | docs: the README counts the bar, and says what the window position accepts |
| `80400878` | docs: a bar over zero total volume is a recorded superset, not a code comment |
| `a592bcce` | fix(exec): a bar's carrier is per FIELD GROUP, not per bar |
| `cb27ec95` | docs(adr): ADR-0035 — a carrier belongs to a field GROUP |
| `635ee96a` | test(exec): the bar's merge law and encoding are gated on a MIXED carrier too |
| `2faf5324` | refactor(exec): the bar's volume field is declared from its own column, once |

---

## 1. Mechanism per commit

**`037b61ca` — `time_bucket`.** `internal/engine/expr/time_bucket.go`,
registered `{fnTimeBucket, RetTimestamp}` and added to `temporalInputFuncs`.
Floor division toward −∞ in epoch milliseconds; origin defaults to
`1970-01-01`; three-argument form takes any origin. Refusals are PostgreSQL's
own (0A000 calendar stride, 22008 non-positive) plus one narrowing of ours
(42804 for a non-INTERVAL stride). Declares OID 1114 rather than text, which
is what #868's `Vector.SetValue` TIMESTAMP-from-text arm makes possible.

**`e8c25847` — the window fallback.** `physical.parseWindowFunc` discarded
`exec.ParseWindowFunc`'s `ok`, so a name with no window arm reached
`exec.Window` as ROW_NUMBER with a mis-typed output vector and PANICKED.
`exec.RefuseUnsupportedWindowFunc` is now the ONE refusal, raised by
`physical.refuseUnwindowable` (single path) and by the worker's fragment
builder (DAG, which previously raised a bare `fmt.Errorf` → XX000).
`exec.WindowFuncNames` derives the message's list from `ParseWindowFunc`
itself.

**`254c8021` — the bar.** `internal/engine/exec/agg_ohlcv.go`: `ohlcvState`,
its merge law, its self-describing hex encoding (272 characters after
`2dcc3cae`), `OhlcvOutputFields` (the ONE declared-type derivation), and
`ohlcvResolveDomain` (the 42883 refusal at the operator, which both paths run).
`AggOhlcv` / `AggOhlcvState` / `AggOhlcvStateMerge` join the enum;
`AggColumn`/`AggSpec`/`distributed.AggSpec`/`logical.AggExpr` gain `InputCol3`
and a declared field list. `wadjet.ColumnMeta.Fields` carries a ROW's
declaration off the plan, and pgwire reads it before the catalog walk. The bar
rides `aggNeedsWholeInput` in this commit — correct, and what ADR-0035 names as
the honest interim.

**`4186c79a` — the mergeable route.** `decomposeOhlcv` (coordinator) and
`applyOhlcvFold` (worker), mirroring `decomposeVar` / `applyVarFold`; the
`__ohlcv_state` slot family; `Coordinator.OhlcvStateRoutes` as the routing
counter; `ohlcv` off `aggNeedsWholeInput`.

**`784d6020`** — the star cell and the micro-benchmark.

**`a16da0db`** — two cells in `TestTypeMatrixPruningNeverChangesTheAnswer`:
`time_bucket` beside a range predicate on the same column, and the bucket used
as the predicate's own column.

**`c6cf3053` / `c7c4b3b4`** — two removals found by re-reading the diff: a
merge that could take its CARRIER from the source state, and a near-copy of the
argument pass-through rule that differed from its original in one place.

**`2dcc3cae` — the state carries its own declared ROW.** Found by extending the
census to a COMPUTED argument, which is where the planner's declaration runs
out. Three defects, all arm divergences, none of them a wrong number:
`ohlcv(ts, price*2, volume)` answered in process and failed loud on the DAG
(the fold took the planner's list and the planner had declined);
`ohlcv(ts, price, volume*2)` failed loud in process and answered a NULL BAR on
the DAG (the PARTIAL form did not refuse an unresolvable third argument); and a
merge stage deriving a field list from its STRING input got a float bar and
tripped the #361 silent-write guard. The encoded state's header now carries the
price, volume and vwap fields' (type, precision, scale) — 128 bytes to 136 —
`FinalizeOhlcvState` takes nothing but the string, and nowhere guesses a
declaration.

**`a592bcce` — the carrier is per FIELD GROUP.** Found by reading the final
diff. One `exact` flag for the whole bar declared an exactness it did not
deliver whenever the two inputs disagreed, and in BOTH directions: a
DECIMAL(18,4) price beside a FLOAT volume declared `open` DECIMAL(18,4) —
correctly — and carried it through a float64, and a FLOAT price beside an INT8
volume declared `volume` NUMERIC — correctly — and summed it in a float64,
wrong past 2^53. Exactness belongs to the columns that FEED each group:
open/high/low/close to the price's, volume to the volume's, and vwap only when
both are exact (PostgreSQL's own promotion). One more from the same read: with
a float price and an exact volume, the vwap denominator was taken from the
float slot the exact accumulator never wrote, so every such bar answered NULL.
`cb27ec95` records the rule in ADR-0035, where the next typed state will read
it, and `635ee96a` closes the two holes the split left in the state's own
gates: `ohlcvSame` and `ohlcvShow` compared and printed EVERY group through
the price's carrier, so a mixed divergence was invisible AND reported two
identical lines.

**`a4477eed` / `15e51e58`** — ADR-0035's §3 rewritten around what the header
now carries and why the plan cannot supply it, plus the census requirement
(a COMPUTED argument) that found all three divergences; and the README's
aggregate count, its date/time row, and its window line, which named the
CLAUSES the window position accepts and not the FUNCTIONS.

---

## 2. The PostgreSQL matrix

PostgreSQL 17.11, shared oracle server, measured 2026-09-08. Full probe SQL in
the scratch dir under `sql/`.

### `time_bucket` vs `date_bin` — 11 value cells, all AGREE on every arm

| probe | PG 17.11 | wadjet |
|---|---|---|
| `('15 min','2020-02-11 15:44:17','2001-01-01')` | `2020-02-11 15:30:00` | same |
| boundary `15:45:00` | `2020-02-11 15:45:00` | same |
| pre-epoch `('1 hour','1969-07-20 20:17:40')` | `1969-07-20 20:00:00` | same |
| `('1 day','1969-12-30 23:59:59')` | `1969-12-30 00:00:00` | same |
| `('1 hour','1900-01-01 00:00:01')` | `1900-01-01 00:00:00` | same |
| origin AFTER source | `2020-02-11 15:30:00` | same |
| default origin = epoch | `2020-02-11 00:00:00` | same |
| `1 week` / `100000 days` / `30 s` | as measured | same |
| NULL in any argument | NULL | same |
| DATE source | cast to timestamp | same |
| months/years stride | `0A000` | same SQLSTATE and sentence |
| zero / negative stride | `22008` | same SQLSTATE and sentence |
| declared type | `timestamp`, OID 1114 | `TIMESTAMP` |

Narrowings recorded in ADR-0012: the two-argument form (PG has none), and the
INTERVAL-literal-only stride (`42804`), which makes `INTERVAL '1 day 6 hours'`
unspellable here because the SQL parser accepts one `N unit` pair.

### The bar vs PostgreSQL's spelled-out bar

Oracle spelling (the FILTER is load-bearing — a multi-argument aggregate skips
a row where ANY argument is NULL; `regr_count(y,x)` over
`(1,1),(2,NULL),(NULL,3),(4,4)` is **2**, measured):

```sql
(array_agg(px ORDER BY ts, px)           FILTER (WHERE ok))[1] AS open
MAX(px)                                  FILTER (WHERE ok)     AS high
MIN(px)                                  FILTER (WHERE ok)     AS low
(array_agg(px ORDER BY ts DESC, px DESC) FILTER (WHERE ok))[1] AS close
SUM(vol)                                 FILTER (WHERE ok)     AS volume
(SUM(px*vol) FILTER (WHERE ok))/(SUM(vol) FILTER (WHERE ok))   AS vwap
```

| bucket | PG | wadjet (single / DAG / DAG-shuffled) |
|---|---|---|
| 12:00 | 10, 14, 7, 11, 11, 9.181818181818182 | identical on all three |
| 12:01 | 18, 21, 18, 21, 13, 19.153846153846153 | identical on all three |
| 12:02 (every row skipped) | all NULL | NULL bar |
| NULL bucket (NULL ts) | all NULL | NULL bar |
| whole table | 10, 21, 7, 21, 24, 14.583333333333334 | identical |

The tie cells are the point: rows 5/6 share `12:01:05` and rows 7/8 share
`12:01:45`, so `open` = 18 (smaller price) and `close` = 21 (larger price)
only under the `(ts, price)` tiebreak — PG's own `ORDER BY ts, px`.

Exact-domain vwap against `round(SUM(px::numeric*vol)/SUM(vol), s)`:
`9.1818` / `19.1538` at scale 4 (integer price), `9.181818` / `19.153846` at
scale 6 (DECIMAL(9,2) price). Identical.

`(NULL::a1_bar).open` is NULL on the server, which is why a NULL bar's field
access is NULL here rather than an error.

### Declared types, per input (measured against PG's min/sum/avg)

| price / volume | open…close | volume | vwap |
|---|---|---|---|
| int4 / int4 | int4 | bigint | numeric(38,4) |
| int8 / int8 | int8 | numeric(38,0) | numeric(38,4) |
| real / int4 | real | bigint | float8 |
| float8 / float8 | float8 | float8 | float8 |
| numeric(9,2) / int4 | numeric(9,2) | bigint | numeric(38,6) |
| numeric(18,4) / numeric(9,2) | numeric(18,4) | numeric(38,2) | numeric(38,8) |
| port / protocol | port | bigint | numeric(38,4) |

### Doors

Embedded API: `ColumnMetas` carry the ROW and its field types
(`(bar).open` over a DECIMAL(9,2) bar declares DECIMAL(9,2), `.volume` INT64,
`.vwap` DECIMAL(38,6)). pgwire: the composite renders
`(10.00,21.00,7.00,21.00,24,14.583333)` in DECLARED field order — asserted
against the sorted-key rendering, which carries the same six numbers — and a
NULL bar is a `-1` field length, not `(,,,,,)`. OID is 25 (text); PG declares
`record` 2249, recorded as a divergence (see §6).

---

## 3. Census, before → after

**`<agg> OVER (PARTITION BY g ORDER BY x)` over the 28 names
`plansql.IsAggregate` accepts** (measured on the v0.18.64 tip):

| | before | after |
|---|---|---|
| right | 5 (SUM COUNT AVG MIN MAX) | 5, same values |
| PANIC (`internal error in pipeline`, no SQLSTATE) | 23 | 0 |
| loud 0A000 naming the sixteen that work | 0 | 23 |
| silent wrong | 0 | 0 |

No right→anything move. PostgreSQL answers all 23; the refusal is in
ADR-0012's list.

**`ohlcv` / `time_bucket`**: both were `unknown function` before, so every
shape is new. Dispositions after:

| shape | disposition |
|---|---|
| the bar, grouped and ungrouped, 7 price × 2 volume types | right on 3 arms + spilled |
| an empty group / a group whose rows are all skipped | NULL bar |
| zero total volume | bar with a NULL vwap |
| a non-numeric price or volume, a non-temporal ts | 42883, all arms |
| `ohlcv(DISTINCT …)` | 0A000, plan time |
| `ohlcv(…) OVER (…)` | 0A000, all arms |
| `(ohlcv(…)).open` in one block | 42809 at the parser (pre-existing, see §6) |
| wrong arity | plan-time error |

---

## 4. Gates, and what fails when the fix is reverted

| gate | proven by reverting |
|---|---|
| `wadjet.TestTimeBucketAnswersWhatDateBinAnswers` (11 cells) | the floor correction → 4 cells fail |
| `wadjet.TestTimeBucketNullAndDateSource`, `…DoesNotBlockTheRowGroupPrune` | the `temporalInputFuncs` entry → both fail |
| `wadjet.TestTimeBucketRefusesWhatDateBinRefuses` (6 cells) | — |
| `expr.TestTemporalInputFuncsCoverage` + 2 `datePartFamily` cells | — |
| `wadjet.TestAnAggregateWithNoWindowFormRefusesRatherThanCrashing` | the `refuseUnwindowable` call → 23 cells fail |
| `coordinator.TestTheBarIsTheSameOnEveryArm` (17 cells × 3 arms) | the decompose → 3 cells fail |
| `exec.TestTheBarsStateCarriesItsDeclaredRow` | — (it is the unit behind `2dcc3cae`) |
| `exec.TestTheBarsDeclaredFieldsFollowItsInputs`, 2 mixed cells | collapsing the carrier to one flag → 6 cells fail |
| `coordinator.TestTheBarIsTheSameOnEveryArm`, 2 mixed cells | the same collapse → 4 cells fail |
| `coordinator.TestTheBarCrossesTheDAGAsAMergeableState` | the decompose → fails |
| `coordinator.TestDecomposeOhlcvRewritesTheBarIntoItsState` | — |
| `exec.TestTheBarsMergeIsAssociativeAndCommutative` (200 random partitions × 2 domains) | — |
| `exec.TestTheBarsEncodedStateRoundTrips`, `…TiebreakIsAValue…`, `…DeclaredFieldsFollowItsInputs` | — |
| `pgwire.TestPGWireRendersTheBarAsAPostgresComposite` + the NULL twin | — (it caught the sorted-key defect when first written) |
| spill sweep: 7 `ohlcv_*` cells, rawrow engagement **7 of 7 cells, 147 events** | — |

Whole-gate runs, all green. The first block was run on `2dcc3cae` and every
one of them was re-run on the final tip `2faf5324` unless marked:

- `go test -short -p 2 ./internal/... ./wadjet/ ./test/` — 48 packages ok, 0
  failures (re-run on the final tip)
- `go test -count=1 ./internal/coordinator/ ./internal/server/` (no `-short`) —
  ok (1144s / 91s); re-run on the final tip as
  `./internal/coordinator/ ./internal/server/pgwire/` — ok (1097s / 42s)
- `go test -count=1 ./wadjet/ ./internal/server/` (no `-short`)
- `TestTypeMatrixAnswersTheSameUnderEveryMemoryBudget` +
  `TestTypeMatrixPruningNeverChangesTheAnswer` (re-run on the final tip) +
  `TestContainerWindowKeysAnswerTheSameAcrossAWindowSpill` +
  `TestEveryGroupKeyProducerWritesTheSameBytes`
- `TestTPCHStageDumpGolden` + `TestTPCH_EnsureDistribution_Snapshot` +
  `TestTPCHQueries` + `TestTPCHOptimizationInvariance` — golden byte-identical
- `TestTPCHQueriesDecimal` + `TestTPCHDecimalDeclaredTypes`
- `TestNumericArc2` + `TestTwoPathInvariance*`
- `task pg-oracle:test` and `task pg-oracle:test-decimal` — both arms each
- `task build`, `gofmt -l` clean

**The seam gate is unchanged, deliberately.** The brief asks that "the seam
gate gains the ohlcv producer (all four producers write the same bytes)".
`TestEveryGroupKeyProducerWritesTheSameBytes` is about the GROUP KEY's
encoding — four producers holding three Go boxes for the same key
(ADR-0023 item 8). A bar is an aggregate VALUE, not a group key: it adds no
producer to that seam and no type to the key encoding, so there is nothing
there for it to join. What a bar DOES have is its own encoding across the same
boundaries, and that is gated as its own law
(`exec.TestTheBarsEncodedStateRoundTrips`: the round trip, the refusal of
anything that is not the encoding, and merge-through-the-encoding equalling
merge-of-the-states). The seam gate was run and is green.

**Pins deleted / moved:** one.
`pgwire.TestPGWireNestedAmbiguousReferenceRendersIdenticallyEveryRun` moved
from `(A,9)` to `(9,A)`. Its `(A,9)` was the sorted-key rendering — "no
declaration bound" — and was the only DETERMINISTIC answer while the catalog
walk was the only source of one. The query is unambiguous to the PLANNER
(`SELECT a.Attrs FROM nsa a JOIN nsb b …`), so the result's own declaration
answers it correctly now. Both halves still hold: 25 identical runs, and the
bytes are now right as well as identical. The resolver-level guard
(`TestPGWireNestedAliasDropsAnAmbiguousFold`) is untouched and still passes —
it calls `nestedColumnSchemas` with metas that carry no declaration, so the
catalog walk still drops the ambiguous fold.

---

## 5. Cross-territory hunks (for landing order)

- **`internal/worker/executor_fragment_window_test.go`** — I repaired a STALE
  expectation (`SUM(int8) OVER ()` read off `Float64Data`, which has been
  DECIMAL since v0.18.64). **origin/main `62e2d2dc` already carries an
  equivalent repair**, so this hunk is expected to conflict at rebase — take
  main's and drop mine.
- `internal/planner/sql/{ast.go, reserved_slots.go}` — two additive entries
  (`ohlcv` in `knownAggregates`, the `__ohlcv_state` slot family).
- `internal/planner/logical/{builder.go, optimizer.go, order_by_keys.go,
  const_arith_agg_typed.go, plan.go}` — `InputCol3` added beside every
  existing `InputCol2` guard. Additive; no existing condition changes meaning.
- `internal/planner/physical/plan.go` — `parseAggFunc`, `aggOutputType`,
  `aggSpecOutputType`, `aggOhlcvOutputFields`, `AggSpec.{InputCol3,
  OutputFields}`, `refuseUnwindowable`.
- `internal/server/pgwire/paraminfer.go` — `nestedSchemaFromMetas` tried
  before the catalog walk.
- `wadjet/wadjet.go` — `ColumnMeta.Fields`.
- `internal/coordinator/var_decompose_test.go` — `!=` → `reflect.DeepEqual`,
  because `AggSpec` gained a slice field and is no longer comparable.

---

## 6. DEFERRED, with mechanism

1. **`(ohlcv(…)).open` inside one query block** — refused 42809 at
   `select_parser.go:1911-1943`, where a field path's container must be a bare
   `ColRef` (ADR-0022 rule 1). This is true of EVERY composite-returning
   expression, predates A1, and PostgreSQL answers the shape. Closing it needs
   a `FieldAccess` AST node over an arbitrary expression plus a hoist in the
   logical builder that materializes the aggregate into a hidden slot and
   rewrites the access as a reference to it — J1's machinery, an arc of its
   own. The supported spelling is the derived block or CTE, gated in
   `TestTheBarIsTheSameOnEveryArm` (every value cell uses it) and documented.
   Recorded in ADR-0035 "Not settled here" and ADR-0012's list.
2. **`ohlcv(DISTINCT …)`** — 0A000. PostgreSQL dedupes a multi-argument
   aggregate on the whole argument TUPLE (`regr_count(DISTINCT y,x)` over
   `(1,1),(1,2)` is 2, measured); this engine's `distinctFirstSighting` keys on
   the first TWO columns, so a bar would dedupe ignoring the volume. Closing it
   is "key the distinct set on the whole tuple", which is a change to every
   multi-argument aggregate, not to this one.
3. **The windowed bar** — 0A000, with every other aggregate that has no window
   arm. A frame-sensitive bar needs a retracting state (open and close cannot
   be un-observed), which is a different state design; `exactFrameAcc` is the
   shape it would follow.
4. **The partial-state spill format** — an aggregate with an `extraState` never
   reaches `aggregate_partial_spill.go` (whose per-aggregate header is ten
   fixed bytes and whose merge is `kernel.Accumulator.Merge`); it takes the
   legacy raw-row spill and re-aggregates from input rows. Correct, slower, and
   what MIN_BY and the variance family already do. Widening that format would
   let the whole `extraState` family keep a k-way merge under pressure.
   ADR-0035 "Not settled here" carries the seven-point edit list from the
   survey.
5. **ROW's wire OID** — 25 (text) where PostgreSQL declares `record` 2249.
   Pre-existing, whole-type, and #992's neighbour. Recorded in ADR-0012 with
   the measurement; the bar's VALUE is PostgreSQL's byte for byte.

---

## 7. Self-flags — what I am least sure of

1. **The vwap DOMAIN rule.** I chose "exact iff BOTH price and volume are
   exact", and "vwap declares what `AVG(price)` declares". Both are defensible
   from PostgreSQL's own promotion rules and both are gated, but the second is
   a CHOICE among several PG-consistent ones — `AVG(price*volume)`'s scale, or
   PostgreSQL's magnitude-dependent division scale, would each be arguable.
   ADR-0024 item 2 already rejects the third for a good reason (a column's
   declared scale would change with the row count). If a reviewer prefers
   `AvgScale(priceScale + volScale)`, the change is one line in
   `OhlcvOutputFields` and the affected cells are named.
2. **The price fields' declared type.** I took the PRICE COLUMN's own type
   (PostgreSQL's `min(int4)` → integer), which DIFFERS from what this engine's
   own `MIN(int4)` declares today (int8 — that is #951). So `(bar).high` and
   `MAX(price)` in the same query declare different types over an int4 column,
   with equal values. I judged PG-correctness the right side to be on and said
   so in ROUND0 and ADR-0035; the alternative is to copy `minMaxDeclaredType`
   and inherit the known-divergent widening.
3. **`ohlcvResolveDomain` refuses at CONSUME, not at plan time.** An
   `ohlcv(text_col, …)` over an EMPTY table therefore answers a NULL bar
   instead of refusing, on every arm consistently. PostgreSQL refuses at parse.
   A plan-time refusal is available for the single path (`aggOhlcvOutputFields`
   already resolves the types) but not for `walkStages`, which returns no
   error — so a plan-time refusal would fire on ONE arm, which is worse than a
   consistent late one. Named here rather than hidden.
4. **`nestedSchemaFromMetas` answers only when EVERY ROW column has a
   declaration.** That keeps a mixed result on the catalog walk rather than
   half-answering, but it means one undeclared ROW column suppresses the
   declaration of a declared one in the same result. I could not construct such
   a result; the conservative direction seemed right.
5. **`applyOhlcvFold` reads the ROW shape from the FIRST non-empty state in
   the column.** Every state in one column came from one aggregate and so
   declares the same thing, but that is an argument rather than an assertion —
   a column mixing two declarations would take the first. I could not
   construct one (a stage's state column is one aggregate's output), and the
   alternative — carrying the planner's list as well — is the dependency
   `2dcc3cae` removed.
6. **The float bar's sums.** `sumVolF`/`sumPVF` are plain float64 additions, so
   their last bits move with fold order — ADR-0013's class 9, the same
   tolerance `SUM(float8)` already carries, and the census renders floats to
   six significant digits for exactly that reason. The exact domain carries
   every digit.
7. **`decomposeOhlcvFor` counts a route per DISPATCH CALL, not per bar.** A
   query with two bars in one stage increments once. The gate asserts
   "moved / did not move", which is what it is for, but the number is not a
   bar count and the doc comment says so.

---

## 7b. Micro-benchmark (`784d6020`, re-measured on the final tip)

2048 rows per iteration, `-benchtime=200x`. The two columns bracket a quiet and
a busy machine (a sibling author shares it), so read the RATIO rather than the
absolute:

| | quiet | busy |
|---|---|---|
| `OhlcvObserveExact` | 27.4 ns/row | 37.4 ns/row |
| `OhlcvObserveFloat` | 4.6 ns/row | 6.5 ns/row |
| `MinByObserveBaseline` | 0.54 ns/row | 0.69 ns/row |
| `OhlcvMergeExact` | 30 ns/group | 24 ns/group |
| `OhlcvEncodeDecode` | 310 ns/group | 331 ns/group |
| `OhlcvFinalize` | 1.19 us/group | 1.22 us/group |

A bar does strictly more than a MIN_BY — four extremes, two sums, and in the
exact domain those sums are Int128 with an overflow check per row — so the
~50x is expected. **Zero allocations per row** is the property worth keeping:
MIN_BY allocates one per row for its boxed value and the bar allocates none.

## 8. Filing candidates (I filed nothing, per the rules)

1. **`internal/worker/executor_fragment_window_test.go` was RED on the
   v0.18.64 tip** before this branch existed (`SUM(int8) OVER ()` read off
   `Float64Data` after K2 made it exact). Already fixed on origin/main
   `62e2d2dc`; noting it because it means the v0.18.64 tag shipped with a red
   package test.
2. **23 of 28 aggregates PANICKED in the window position** — fixed here
   (`e8c25847`), but it is a filing-worthy defect in its own right and the
   commit is its record.
3. **`docs/data-types.md` carried two contradictory paragraphs** about
   `DATE_TRUNC`'s declared type (one pre-#868, one post-). Corrected in
   `037b61ca`.
4. **A bar over ZERO total volume answers a NULL vwap where PostgreSQL's own
   quotient raises 22012.** Recorded as a superset in ADR-0012 (`80400878`)
   rather than left as a kernel comment, which is what it was until the final
   diff read.
5. **Operational, not code:** the machine's disk hit **49 MB free** mid-arc.
   `/home/dwright/.cache/go-build` is **111 GB** and there were **183 dangling
   anonymous Docker volumes** (39 GB of local volumes, 16 GB reclaimable).
   Pruning only the hash-named anonymous volumes recovered 15 GB, per
   `feedback_disk_hygiene_docker_anon_volumes.md`; the go cache was left alone
   because a sibling author shares the machine. It will fill again.

---

## 9. Issue-comment text

### #965 — CLOSE

> Landed as five commits on the 0.19 line.
>
> **`time_bucket(stride, ts[, origin])`** is PostgreSQL's `date_bin` under the
> name every time-series engine spells it, and PostgreSQL 17.11 is the oracle
> for every answer and both refusals: the division floors toward the past (a
> truncating one files every pre-1970 row under the bucket above it), a
> boundary belongs to the bucket it opens, the origin defaults to
> `1970-01-01`, a stride containing months or years is `0A000` and a
> non-positive one `22008`. It declares OID 1114, not text, so a driver reads
> it as a timestamp. Two narrowings are ours and recorded in ADR-0012: there is
> no two-argument `date_bin` to match, and the stride must be an `INTERVAL`
> literal (`42804`) because the accepted interval grammar is the SQL parser's.
>
> **`ohlcv(ts, price, volume)`** folds a group into ONE mergeable state and
> answers a whole bar as a ROW `(open, high, low, close, volume, vwap)`. The
> merge is associative and commutative — that is the whole claim, and it is
> gated over 200 random partitions merged in random order — and the tiebreak on
> a shared instant is a VALUE, `(ts, price)`, because a row position is not
> observable across execution arms. A row is skipped when any argument is NULL
> (PostgreSQL's multi-argument rule, measured), and a group that keeps no rows
> answers a NULL bar rather than a bar of NULLs.
>
> Each field declares what its own spelled-out aggregate declares: the price
> column's type for the four prices, `SUM(volume)`'s for volume,
> `AVG(price)`'s for vwap. `vwap` is a weighted MEAN computed by exact decimal
> division at AVG's scale, not a float quotient — but it is NOT digit-identical
> to a hand-written `SUM(price*volume)/SUM(volume)`, which takes division's own
> scale and, over INTEGER columns, is integer division (PostgreSQL answers `14`
> there where the bar answers `14.5833`). The two agree to the lesser of their
> two scales, gated on int4, int8 and three DECIMAL prices; the divergence from
> the server's magnitude-dependent division scale is ADR-0012's existing AVG
> entry.
>
> The partial STATE crosses the DAG, not the bar: `OHLCV(...)` is rewritten to
> `OHLCV_STATE(...) AS __ohlcv_state#out` at dispatch, merge stages fold states
> into states, and the final stage folds the last one into the ROW. That route
> is asserted with a counter (`Coordinator.OhlcvStateRoutes`), not inferred
> from rows, because the one-level dispatch answers the same bar.
>
> Value oracle: PostgreSQL's bar spelled out per field with the same row
> filter, on three arms plus the spilled one. Docs:
> `docs/sql-reference.md` §OHLCV, `docs/data-types.md` §ROW,
> **ADR-0035** (the pattern TDIGEST / HLL / TOP_K reuse), ADR-0012's divergence
> entries.
>
> Not closed here, and named in ADR-0035: `(ohlcv(…)).open` inside one query
> block (a field path's container must be a bare column reference, ADR-0022 —
> use a derived table or CTE), `ohlcv(DISTINCT …)` and the windowed form (both
> `0A000`).

---

## 10. Release-notes bullets

- **`TIME_BUCKET(stride, ts[, origin])`** — floor a timestamp to a fixed-width
  bucket, PostgreSQL's `date_bin` semantics digit for digit. Buckets are UTC
  and exactly `stride` wide; the origin defaults to `1970-01-01`. Returns a
  TIMESTAMP, so a client reads it as one. A stride containing months or years
  is refused (no fixed width), as is a stride of zero or less.
- **`OHLCV(ts, price, volume)`** — one aggregate that answers a whole bar as a
  ROW `(open, high, low, close, volume, vwap)`. General-purpose downsampling:
  latency, sensor readings, flow bytes, not only prices.
  ```sql
  SELECT bucket, (bar).open, (bar).close, (bar).vwap
  FROM (SELECT TIME_BUCKET(INTERVAL '5' MINUTE, ts) AS bucket,
               OHLCV(ts, price, size) AS bar
        FROM trades GROUP BY 1) t
  ORDER BY bucket;
  ```
  Rows sharing an instant are ordered by price, so the bar does not depend on
  how the query was executed. A row with any NULL argument is skipped; a group
  with no remaining rows answers a NULL bar. `vwap` is the exact weighted mean
  of the price, carrying `AVG(price)`'s type.
- Bars are computed in pieces and combined, so a distributed `OHLCV` ships one
  small state per group instead of every row.
- **A ROW column now renders in its declared field order on every path.** A
  composite produced by an aggregate previously rendered with its fields sorted
  by name.
- **An aggregate with no window form is now refused with a clear message**
  naming the sixteen window functions, instead of failing the query with an
  internal error. `STDDEV`, `MEDIAN`, `STRING_AGG`, `MIN_BY`, `CORR` and
  eighteen others are affected.
