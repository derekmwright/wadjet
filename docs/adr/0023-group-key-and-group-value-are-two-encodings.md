# ADR-0023: A group's KEY and its VALUE are two encodings; never decode one out of the other

Status: Accepted (landed 2026-08-25 with the #566/#576 fix). Supplements
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
