# Consumer published identity

Source: internal/planner/physical/published_identity.go — bindConsumersToPublishedIdentity, moved 2026-09-11 (#1026)

```go
// A CONSUMER BINDS THROUGH THE IDENTITY ITS PRODUCER PUBLISHED (#770).
//
// ADR-0026 §2 gave a GROUP BY key two names — the PUBLISHED name every
// consumer above the aggregate reads it under, and the RESOLUTION spelling the
// computing fragment looks up in its own input — and carried both on the
// Stage. The two names stop at the aggregate. A JOIN publishes names of its
// own: `joinOutputSchemaWithMapping` emits the probe's columns and then the
// build's with every DUPLICATE bare name QUALIFIED by its owning alias, and a
// join's `Columns` (an OutputFilter) plus its exchanges' payload manifests are
// built from `NeededColumns`, which spells the name the QUERY wrote. So a
// consumer that resolves a column by a SECOND spelling — the resolution
// spelling of a group key, an aggregate's argument, a window's argument — is
// handed a name and left to hope the payload carries it under exactly that
// text.
//
// Two things go wrong, and #770 is both at once:
//
//   - the value IS on the stream, under the spelling the join published for
//     it. `SELECT DISTINCT x.w, y.w, z.w` over three derived arms resolves
//     x's key to the source column `a`, which the join publishes as `x.a`
//     because z's arm carries an `a` too. The runtime's own fallback then
//     finds TWO columns ending `.a` and declines, which is right — a stream
//     with two `.a` is not one the engine may guess at — and the task fails
//     on a query PostgreSQL answers.
//   - the value is on NO stream at all, because a narrowing stage below
//     dropped it. y's key resolves to `w`, which the y arm's fragment
//     computes and the join UNDER the consumer filtered away.
//
// The first is answered by RESPELLING to what the producer publishes; it costs
// no bytes. The second is answered by CARRYING the value, which does — so it
// is asked SECOND and only of a reference the first could not place. The
// TPC-H stage-dump golden is the measurement: every group key there already
// binds, so no query gains a column.
//
// The two questions are asked of the stream the fragment will really see —
// `aggregateInputStreamColumns` with the narrowing lists APPLIED — which is a
// different question from the one `resolveStageGroupKeys` asks. That pass runs
// before the payload is settled and asks what the arms can SUPPLY; this one
// runs after and asks what they will SHIP. Both are needed: the first picks
// the value, the second picks its name.
//
// The pass runs in two phases, and the order is the whole of the argument that
// it costs nothing: phase 1 CARRIES only what no spelling on the stream
// reaches, phase 2 then RESPELLS every consumer against the stream those
// carries produced. Doing them in one loop would respell against a stream a
// later stage's carry is about to change.
```
