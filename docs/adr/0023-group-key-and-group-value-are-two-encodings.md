# ADR-0023: A group's KEY and its VALUE are two encodings; never decode one out of the other

Status: Accepted (landed 2026-08-25 with the #566/#576 fix; amended 2026-08-29 with item 7, and 2026-09-02 with item 8 — one key ENCODER per column TYPE, after #788 found four types keyed two ways inside ONE operator). Supplements
ADR-0006, which says every pipeline breaker degrades past memory but does not
say what a drained group's bytes have to be.

## Context

A `HashAggregate` that drains partial state writes two different things per
group into the same run file:

- the **merge key**, which the k-way merger sorts and compares by
  (`appendSerializedKey` / `appendKeyValue`, `sort.go`), and
- the group's **VALUE**, which `Next()` has to hand back as the row the query
  selected.

They look interchangeable and are not. A key only has to be *injective and
order-stable*; a value has to be *the value*. Every place those two
requirements pull apart, the key is deliberately the one that gives:

- `kernel.KeyFloat32Bits` / `KeyFloat64Bits` fold every NaN payload onto one
  and `-0.0` onto `+0.0`, because the comparator calls those pairs EQUAL and
  "compares equal" and "serializes alike" have to name the same relation
  (#446).
- `kernel.CidrOrderKey` rewrites a CIDR leaf into PostgreSQL's inet order,
  because `'10.0.0.1'` and `'10.0.0.1/32'` are one inet value (#492, #520).
- `appendKeyValue` renders the fixed-width scalars as TEXT, which is injective
  for them and reversible for none of the types whose storage is not that text.

None of that is reversible, and none of it should be. It is a key.

The two got conflated for the four container types. `setPartialKeyFromAny`
had no slot for an `ARRAY`, `ROW`, `MAP` or `VECTOR`, so its default arm
captured the group's value as `fmt.Sprintf("%v", …)` — the *display* form —
and the emit side handed that text to a container vector's `SetValue`, which
refuses it (#361's silent-write guard). Every container GROUP BY failed the
moment the aggregate's partial state left memory (#566), and — because a
morsel-parallel clone hands its partial to the primary as run FILES in the
same format — a `VECTOR` group key failed with no memory pressure at all
(#576). One defect, two reports, one site.

The tempting repair is the wrong one: the merge key for a container is
already a tagged, self-delimiting, injective tree (`appendKeyElem`), so it
looks decodable. Decoding a value out of it would have answered `+0.0` for a
`-0.0` a row actually held, one NaN payload for another, and a CIDR's order
key in place of the text the column stores — a spilled answer differing from
the un-spilled one, which is the exact failure the drain exists not to cause.

## Decision

**A drained group's key and its value are separate encodings, and neither is
derived from the other.**

1. **The key may be lossy in whatever way the comparator is.** It folds what
   `=` calls equal and re-keys what an order says is one value. Its only
   contract is injectivity over DISTINCT values plus a stable order.

2. **The value is lossless.** `appendContainerKeyValue`
   (`internal/engine/exec/aggregate_container_key.go`) encodes exactly the
   boxed shapes `Vector.GetValue` produces and `Vector.SetValue` accepts, with
   RAW float bits and no re-keying, so the spilled path emits the bits the row
   carried.

3. **The value round-trips through the same boundary the un-spilled path uses.**
   The in-memory emit is a `GetValue` → `SetValue` round trip over
   `keyValues`; the drained emit decodes to those same boxed shapes and calls
   the same `SetValue`. That is what makes "spilled equals in-memory" a
   property of construction rather than a coincidence to be tested for per
   type.

4. **A null is written by `Vector.WriteNullAt`, never by hand.** The null arm
   used to set the bit and then patch the offsets of an enumerated list of
   bytes-class destinations. Such a list cannot be complete: a NULL ARRAY/MAP
   leaves its `Offsets` entry unwritten and a NULL ROW skips every CHILD's
   slot, so a bytes-class field below it keeps offsets describing the wrong
   bytes and the next non-null row reads from the start of the arena. Only the
   vector knows its own shape.

5. **A key producer is SHARED, not re-implemented.** The one consumer outside
   `exec` that builds this key from the same boxed value — the coordinator's
   cross-worker re-aggregation — calls `exec.AppendBoxedGroupKey` rather than
   writing its own. It had written its own (`fmt.Appendf("%v")`) and it was
   not injective: `ARRAY['a b']` and `ARRAY['a','b']` both render `[a b]`, so
   two groups every worker kept apart merged into one at the coordinator and
   the identical query answered differently distributed than single-process.
   A second implementation of a key is a second definition of equality.

6. **Neither encoder may write bytes its own reader refuses.** The container
   value codec caps recursion depth on both sides at the same number, and a
   subtree past the cap is marked REFUSED (a tag every decode errors on)
   rather than rendered — a rendering is not injective, and this is a key's
   payload. The first version capped only the reader and emitted `%v` past the
   encoder's cap, which both lost injectivity and produced bytes that failed
   to decode.

7. **A key across two TYPES is built at the pair's COMMON type, and the
   common type is PostgreSQL's OPERATOR resolution.** (Added 2026-08-29 with
   the #615/#650/#663 fix.) Item 1 says a key may fold whatever the
   comparator folds; it did not say what happens when the two sides of one
   comparison are not the same type. They were keyed at each side's own
   storage encoding while the comparator resolved the pair first, so `a.i =
   b.d` was right spelled as a WHERE and matched almost nothing spelled as an
   ON — `numeric IN (SELECT bigint)` panicked in the integer fast path,
   `bigint IN (SELECT numeric)` answered 0, and `int = float` inside a
   three-relation join panicked `inlineIntProbe`.

   The resolution is the one PostgreSQL uses for an OPERATOR, read off
   `EXPLAIN VERBOSE` on 17.11 (`physical.joinKeyCommonType`):

       int4 ⊕ int8       -> int8
       int  ⊕ float4     -> float8      (there is no float4 = int4 operator)
       int  ⊕ numeric    -> numeric
       float4 ⊕ float8   -> float8
       numeric ⊕ float4  -> float8
       numeric ⊕ float8  -> float8
       numeric ⊕ numeric -> numeric, exact, at either declared scale

   It is **not** `select_common_type`, the ladder a SET OPERATION uses
   (`physical.setOpWiden`, ADR-0024 item 2): there `real ∪ numeric` and
   `real ∪ integer` are REAL, because real is a PREFERRED type of the numeric
   category for that resolution and merely a resolvable one for an operator.
   The two ladders are separate functions with separate pins for that reason,
   and a fix applied to one is not a fix to the other.

   Three corollaries, each a place the invariant is enforced rather than
   assumed:

   - The **fast paths are gated on the RESOLVED type**, never on either
     column's storage. The integer hash path was enabled from the BUILD side's
     type alone, and the probe loops then indexed the PROBE column's
     `Int32Data`/`Int64Data` — nil for a DECIMAL or a FLOAT. Resolved on the
     pair the question answers itself: the int path is legal exactly when the
     pair's common type is an integer, and then both sides are integer-class
     columns by construction.
   - The **partition hash uses it too**. The two sides of a shuffle join
     repartition on their own key columns, so a pair hashed at each column's
     own width sends equal values to different partitions and the join
     downstream matches none of them — the same defect one layer down, and
     silent. `ExchangeStage.KeyTypes` carries the same list the join carries,
     and `Distribution.KeyTypes` carries it into the property algebra so two
     differently-hashed exchanges are not called interchangeable.
   - It travels **past the spill boundary**. A grace-partitioned build replays
     each spilled partition through a temporary `HashJoin`; without the pair's
     type on it, the replay rebuilds its index at the column's own encoding
     and the join stops matching past the spill only.

   **What the plan-time resolution can see, and what catches the rest.** The
   resolver reads the SHARED declared-type layer (`emittedColTypes` for an
   aggregate / window / projection / DISTINCT chain, whose Project arm types a
   CAST; `setOpDeclaredOutputSchema` for a side that IS a set operation), plus
   the source names still visible below a rename, because a key may be spelled
   either way. Its first version was a walk of its own over scans and renames
   only, and answered NOTHING for a side rooted at an aggregate, a window or a
   set operation — so `a.d = b.k` over `(SELECT bigint AS k … GROUP BY …)`
   resolved to nothing and fell back to the very gate this item replaces.

   A side it still cannot type resolves to `KeyTypeUnresolved`, and that is
   **not** a plan-time refusal: refusing on "cannot type" would refuse every
   join over a table function, an unannotated scan or a shape the walk does
   not cover, most of them perfectly well-typed at run time. The refusal lives
   at RUN TIME instead (`exec.checkProbeKeyTypes`), where both sides' ACTUAL
   encodings are known at once and a false positive is impossible. It raises
   on exactly two conditions: the integer fast path over a non-integer probe
   (the panic), and a numeric-ladder pair whose key ENCODINGS differ with
   nothing resolving them (the silent miss). `keyEncodingClass` is what makes
   that decidable — it is coarser than the type, so PORT/PROTOCOL/DATE against
   INT32 and IPv4/MAC/TIMESTAMP/DURATION against INT64 keep matching, as they
   always have.

   **The same ladder governs `x IN (SELECT y …)`**, which is `x = y`
   quantified. When it decorrelates into a semi join it gets the ladder from
   the key resolution above; when the inner select item is COMPUTED it does
   not decorrelate, and the two remaining mechanisms each had a rule of their
   own:

   - `expr.InSubquery` consulted whichever typed value set matched the
     PROBE's own Go box, so every cross-rung pair missed every member —
     `numeric IN (SELECT float8)` answered 0 against PostgreSQL's 7, and its
     NOT IN answered 7 against PostgreSQL's 0, inventing rows rather than
     dropping them. It resolves the rung from (probe kind, set kind) now and
     carries the set in every spelling the ladder can ask for: exactly, as
     canonical decimal text, and as float64. A FLOAT set gets no decimal
     view, because `numeric ⊕ float8` is float8 and an exact comparison
     against it would be a different predicate.
   - `physical.materializeInSubquery` inlines the set as a LITERAL list, and
     PostgreSQL's multi-element `real IN (…)` NARROWS its literals to real[]
     (#549) while a subquery WIDENS the real to float8. A float32 probe
     therefore declines that path entirely — the mirror of the float32 SET
     `inSetLiteral` already declined. A float64 member that rendered without
     a decimal point re-parsed as an INTEGER literal and compared exactly,
     which dropped the 2^53 row PostgreSQL matches at float8; the rendering
     keeps the point.

   The DECIMAL rung needs no `(p,s)`: `AppendDecimalKey` is scale-normalized,
   so an integer keyed at scale 0 lands on the DECIMAL holding the same
   quantity and 12.75 keys alike at scale 2 and scale 4 (#474). The float rung
   reads a DECIMAL through the correctly-rounded nearest double, exactly as
   `numeric::float8` does, which is also why `bigint = double precision` says
   9007199254740993 and 9007199254740992.0 are EQUAL — PostgreSQL's answer,
   pinned so nobody "fixes" it into an exact integer comparison.

8. **One key ENCODER per column TYPE, and the type is what dispatches it.**
   (Added 2026-09-02 with the #788 fix.) Item 5 says a key producer is SHARED
   and states it for the one consumer outside `exec`. The same rule holds
   INSIDE one operator, and there it had been broken for four types.

   A `HashAggregate` produces the bytes the k-way merger compares from four
   places, holding one value in three different Go boxes:

   | producer | the box it holds |
   |---|---|
   | the int-mode and packed-int drains (`appendIntModeSortKey`) | the int64 STORAGE |
   | the compact and str/generic drains (`appendSerializedKey`) | `Vector.GetValue`'s box |
   | `migrateToGenericMap`, when a NULL key migrates an int-keyed table mid-consume | the RAW int64, re-boxed |
   | `decodeSerializedKey`, on the deferred generic path | re-boxed from the binary hash key |

   Those are not one encoding. `GetValue` FORMATS the int-stored types whose
   storage is not their text, so a DATE key was `14610` from the int drain and
   `\n2010-01-01` from the boxed remainder; the merge compares bytes and never
   combined them, and 421 groups came back as 772–841 rows with `sum(n)`
   unchanged — right totals, wrong grouping, past a spill boundary only (#788).
   Patching the int drain alone still reproduced, because `migrateToGenericMap`
   is a second source of the digits. The sweep for the property found FOUR
   types with two identities: DATE, IPv4 and MAC (display text against storage
   digits) and BOOL, whose migration box is `int64(1)` where `GetValue`'s is
   `true`.

   `exec.appendGroupKeyColumn` is the single encoder, dispatched on the
   DECLARED type and never on the box — the box is exactly what disagreed —
   with `appendTypedIntKey` as its int arm and `batch.KeyStorageInt` as the
   inverse boxing that lets the boxed producers reach the same integer. The
   encoding is the value's STORAGE and not its display: it is what the hot int
   path already wrote for INT32/PORT/PROTOCOL, what the coordinator's own
   cross-worker re-aggregation already wrote for DATE/IPv4/MAC, and item 1's
   own rule that a key is keyed on what the comparator compares. A box no
   column of the type can hold is NOT guessed into an integer — a wrong integer
   is a wrong GROUP — and keys as its own length-prefixed text, which cannot
   collide with the bare-digit arm.

   The gate is `exec.TestEveryGroupKeyProducerWritesTheSameBytes`: for every
   flat type and every producer, one value has ONE merge key, compared byte for
   byte. It deliberately does not go through a query — a query reaches these
   producers only under a memory budget, a key-path migration and a partitioned
   plan at once, which is why #788 survived four investigation rounds — and
   `TestGroupKeyProducerSweepCoversEveryDeclaredType` fails when a 23rd type is
   added without a sample. The end-to-end arm is
   `wadjet.TestTypeMatrixAnswersTheSameUnderEveryMemoryBudget`, whose
   `group_by_distinct_c_date` cell was the #788 pin.

   **The encoder is single-valued on the type's DOMAIN, and the domain is an
   unstated invariant.** `batch.KeyStorageInt` inverts `GetValue`'s formatting
   by re-parsing it, so the two producers agree exactly while an IPv4 column's
   storage is a uint32, a MAC's is 48 bits and a DATE's year is 1..9999.
   `SetValue`'s TEXT door guarantees that — it stores NULL for an unparseable
   date and declines an unparseable address — but `SetValue` also accepts a RAW
   INTEGER verbatim with no domain check, and for one outside the domain the
   failure is NOT the guarded `ok=false` text fallback: `formatIPv4`/`formatMAC`
   truncate to 32/48 bits, the parse re-widens, and `KeyStorageInt` answers
   ok=true with a DIFFERENT integer than the int drain writes. No wadjet writer
   produces such storage — every path goes through the text door — so this is
   not reachable from SQL, and `TestAnUnstorableBoxKeysApartFromEveryRealValue`
   covers the other side of the invariant (a box that does not parse) and not
   this one. A foreign parquet file is the one producer that bypasses the door,
   which is where the invariant would have to be checked if it ever needs to be.

   The VALUE a group emits is untouched by all of this: it stays the boxed
   round trip of items 2–3, and nothing decodes a value out of a key.

## Consequences

- Adding a type to the group-key path means answering both questions
  separately: how does it KEY (and what does the comparator say is equal?),
  and how does its VALUE survive a round trip? Answering only the first is
  what #566 was.
- The container value codec is not a general serialization format and must not
  grow into one. It exists to carry `GetValue`'s boxes to `SetValue`, and its
  test asserts exactly that round trip, including NaN payloads and `-0.0`.
- The gates for this live at three layers on purpose, because no single one
  can force the path: the operator with a forced tracker
  (`exec.TestContainerGroupByAcrossASpillMatchesMemory`,
  `TestNullContainerGroupKeyDoesNotDesyncLaterRows`), a worker stage task with
  a real shared budget (`worker.TestContainerGroupKeySpillsOnAWorker`), and
  the embedded API for end-to-end agreement. An embedded-API budget CANNOT
  force a container drain — a morsel-parallel clone charges a tracking-only
  spill view whose `ShouldSpillFor` is false — and a gate that claims to and
  does not is worse than no gate: the first version of that arm measured zero
  drain writes for three of the four container types.
- The per-TYPE seam has a gate of its own, and it is the one the three layers
  above could not be: `exec.TestEveryGroupKeyProducerWritesTheSameBytes` asks
  the property directly, per type and per producer, with no query and no
  memory budget in the way. A gate whose trigger is a CONDITION cannot be
  relied on to fire; this one is decidable.
- `typematrix.Corpus` now puts a container in the GROUP-BY position. It never
  did, which is why a whole class of container-key defects was invisible to
  every differential gate in the repo.

## Alternatives considered

**Decode the value from the merge key.** Rejected: the key is deliberately
lossy (above), and every loss is a wrong answer that appears only past a
spill boundary — the hardest class to see.

**Keep the display string and re-parse it into the container.** Rejected: a
container's display form is not injective (`ARRAY['a b']` vs
`ARRAY['a','b']`), so distinct groups would merge; and no parser for it
exists or should.

**Hold a one-row `*batch.Vector` per group and encode it with
`batch.EncodeContainerColumn`.** A real option — that codec already carries
nested Array/Map/Row for the shuffle — and rejected for this seam only: the
drain has the boxed `any` from consume time, not the source vector row, so
taking it would mean keeping a vector alive per group for the whole build.
The boxed codec costs one encode per group per drain and nothing while the
aggregate is in memory.
