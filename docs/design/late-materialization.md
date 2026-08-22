# Late materialization: view vectors for join output

Status: DRAFT — awaiting review. Grounded against main a76c083, 2026-07-08.

## 1. Problem and evidence

Every hash-join probe batch is fully materialized today: match pairs are
collected as references, then every output column is gathered (copied) into a
freshly allocated batch (`join.go:2141-2148`). In a fused multi-join pipeline
(Q05/Q07/Q08/Q09 shapes — `buildPipeline` appends successive probes into one
`[]UnaryOperator`, plan.go:4190; distributed fragments carry consecutive
`OpHashJoinProbe` specs, executor_fragment.go:1656), a column that merely
passes through k joins on its way to the aggregate is copied k times, and rows
eliminated by a later probe or residual filter are copied and then thrown away.

Fresh evidence (SF1 CPU profile, 2026-07-08, main a76c083; query phase =
~64.7s of samples after excluding datagen/ingest):

- `HashJoinProbe.Execute` cum 7.15s = **11.0% of query-phase CPU** — the
  largest single operator, ahead of `HashAggregate.Consume` (5.03s).
- Of that, **~4.9s (~7.5% of query phase) is pure output materialization**:
  `gatherVector` on probe columns 2.40s, `gatherBuildVector` 1.18s,
  single-build fast-path gather 0.09s, output-batch alloc + `memclr` ~1.2s.
  Matching itself (hash lookups, pair collection) is the minority of probe
  time.
- GC is a first-order cost in the same profile (`gcDrain` 20s of 120s total).
  Join output is a top allocation site (fresh vectors per output batch, up to
  16×2048 rows); deferring materialization removes most of those bytes.

The roadmap's "~13% memmove" figure (2026-05-22 block profile) is documented
nowhere and predates morsel/streaming-exchange/CBO; this profile replaces it
as the payoff basis. Note the honest bound: late materialization does not
eliminate the final copy — a column touched downstream is still gathered once.
The save is (k−1) intermediate copies per passed-through column, all copies
for rows later eliminated, and the allocation/GC bytes of intermediate
batches.

## 2. Verified current state (anchors)

- Match pairs are already references: `matchPair{probeRow int32, ref
  buildRef{batchIdx,rowIdx}, matched bool}` (join.go:1885, :32). The gather
  into a fresh pooled batch (join.go:2085-2148) is the only materialization
  step. `countingSortPairs` (join.go:1438) orders pairs by build batch for
  gather locality.
- Semi/anti joins already late-materialize: they return the *input* batch
  with `in.Sel` set (join.go:2695-2705) — zero copy. This design extends the
  same idea to joins that must emit build columns, which `Sel` cannot express
  (one batch-level `Sel` cannot address two source batches, and build columns
  need one entry per *pair*, not per probe row).
- Build batches are columnar, `Detach()`ed, retained in `h.buildBatches`, and
  freed only in `HashJoin.Close()` after the probe pipeline fully drains
  (join.go:2982-2996; broadcast builds refcounted via `broadcastJoinCache`).
  `buildRef` addressing already survives `consolidateBuild`'s ref rewrite
  because refs are minted at probe time, post-consolidation (join.go:1425).
- There is no view/dictionary vector anywhere: `batch.Vector` is owned typed
  storage (vector.go:225-246); every consumer reads `Int64Data[row]` etc.
  directly. The expression compiler is row-indexed on concrete vectors
  (expr.go:116,163-165) and its `EvalVec` path additionally requires dense
  rows. Filter kernels read raw columns (filter.go:407).
- Pipeline ownership: after each op, `prev.Release()` recycles the input
  batch (pipeline.go:100-101). A reference into the probe input would dangle
  without an explicit pinning contract (build batches already have one).
- Retaining consumers: Sort (`Detach`+append, sort.go:91-92), Window
  (window.go:129-130), CollectSink/BatchSink (pipeline.go:594), the deferred
  join / reverse-bloom bridges (plan.go:4218, :4087). Serializing sinks all
  gather rows anyway: shuffle `writeChunk(cols, sel, n)`
  (shuffle_format.go:110-148), `gatherReplySink` (gather_reply_sink.go:75),
  `partitionedShuffleSink` (partitioned_shuffle_sink.go:142).
- Column pruning through joins already exists (`OutputFilter` from
  `NeededColumns`, join.go:1902-1912, plan.go:4183) — late materialization is
  orthogonal: it defers the *row* copy of the columns that survive pruning.
- Adjacent prior art: PR #192 `BuildStoreCols` arrival projection returns a
  view RecordBatch sharing vectors (join.go:1517-1556) — the share-and-detach
  pattern this design generalizes.

## 3. Design: view (dictionary) vectors

New optional form on `batch.Vector` — the standard columnar-engine primitive
(DuckDB slice vectors, Velox/Arrow dictionary vectors):

```go
type Vector struct {
    ...
    // View form: when Base != nil this vector owns no typed storage;
    // logical row i is Base row Indices[i]. Nulls (if allocated) is the
    // view's own bitmap and overrides Base (outer-join null-fill);
    // otherwise nullness comes from Base through Indices.
    Base    *Vector
    Indices []uint32
}
```

- `Flatten()` materializes a view into owned storage — implementation-wise it
  IS `gatherVector(dst, Base, Indices)` (sort.go:530), the exact copy we do
  today, moved to first touch. All 22 types incl. nested come along for free.
- `RecordBatch.FlattenViews()` flattens all view columns;
  `FlattenColumn(i)` flattens one (column-granular laziness).
- Composition: gathering *from* a view emits a new view over the same Base
  with `newIndices[i] = old.Indices[rows[i]]` — uint32 arithmetic, no data
  copy. This is what makes k-join chains one-copy-per-column.
- All probe-side view columns of one output batch share a single backing
  `Indices` slice (the pair probe rows); build-side views share another. Two
  uint32 arrays per output batch replace one full copy per column.
- `MemBytes()` of a view counts indices + own bitmap only. Base bytes are
  accounted by their owner (build tracker / probe pin — §4). No charge is
  ever added to shared broadcast-cache vectors (2026-06 stall incident).
- Escape safety: a view has nil typed slices, so any non-aware code that
  reads `Int64Data[row]` fails loud (index panic), not silently wrong.

### Join output under the flag

`HashJoinProbe.Execute` (inner + left in v1; see §7):

- Probe columns: view over `in.Columns[srcIdx]` with the shared probe-row
  indices. If `in` itself carries views (chained probe), compose.
- Build columns: view over `buildBatches[0].Columns[srcIdx]` when the build
  is single-batch (the common case post-`consolidateBuild`); multi-batch
  builds keep today's `gatherBuildVector` copy (a view has one Base).
- Left-join unmatched rows: view with own null bitmap set on unmatched pair
  positions; `Flatten` composes own-nulls over base-nulls — exactly
  `gatherBuildVector`'s matched branch (join.go:3155).
- Right/full-outer probe-phase output may also use views, but the
  `FlushUnmatched`/`FlushMatched` emission paths (join.go:2821-2961) stay
  eager — they run post-probe against build refs and are small.
- Semi/anti/right-semi/right-anti: unchanged (already optimal or build-only).

## 4. Ownership and lifetime

Rule: **views never survive the pipeline's per-batch cycle.** The push
pipeline drives one batch through the whole op chain before the next
(pipeline.go:92-115), so views over probe input i are dead (flattened,
composed away, or serialized) before batch i+1 enters.

- The probe pins its input: on emitting view output, `in.Detach()` and hold
  it as `pinnedProbe`, releasing the *previous* pinned input at the start of
  the next `Execute` and at `Close`. Bounded at one batch per join. The
  pinned batch's `MemBytes` is reserved on the join's tracker while held.
- Build bases need nothing new: `HashJoin.Close()` after pipeline drain is
  the existing guarantee; broadcast refcounts already cover shared builds.
- Every retaining or serializing consumer flattens on entry: Sort, Window,
  CollectSink/BatchSink, spill writers, the join bridges — one
  `b.FlattenViews()` call guarded by a cheap `HasViews()` check. The
  pipeline additionally flattens defensively before any sink not declared
  view-aware, so correctness never depends on remembering a call site.

### 4.1 Emit-storage reuse (2026-08-22)

The same ownership rule that lets views be lazy also lets the probe's *own*
emit storage be reused, and the SF100 window that measured the Go heap lock at
88% of all worker mutex delay (81 s per worker per suite run — see
`docs/benchmarks/sf100-window-analysis-2026-08-22.md` §5) made that the largest
single lever on the join path: `emitViewOutput` charges ~273 GB per suite run,
all of it in large objects (>32 KB) that take the heap lock directly.

`BatchPool` could not recycle any of it, because it recycles only what a driver
hands back with `Release()`, and the worker's `fragmentDriver`
(`internal/worker/executor_fragment.go`) does not opt into `ReleaseInputs` —
so on the distributed path every probe output batch was freshly allocated.

The signal the worker path *does* carry is `Detach`. It already means "a
consumer keeps this past the call that handed it over", and every retaining
consumer calls it. So:

- `RecordBatch.Detach` records the claim on the batch **and on every column
  vector** (`Vector.Claim`, which recurses through `Base` and nested children).
  The per-vector claim is what makes it hold through a *derived* batch:
  `ColumnPrune`, the set-op emitter and partitioned aggregation's `selView`
  mint a new `RecordBatch` over the same `*Vector` pointers, and a view minted
  downstream over one of the probe's gathered columns propagates the claim
  through `Vector.Base`.
- `HashJoinProbe` keeps its last output (`probeEmitBuf`, `join_emit_reuse.go`)
  and writes over it — the shell, the probe/build index arrays, the view
  composition buffers, and the eagerly-gathered build columns via
  `Vector.ResetForWrite` — only while no claim has landed. One claim
  surrenders the whole buffer, since every piece of it is reachable from the
  batch the consumer kept.
- `emitViewOutput` pins its input with `DetachPool` rather than `Detach`: that
  reference is transitive (it dies with this output batch), so claiming the
  input outright would stop an *upstream* probe reusing its storage forever.
  Pool severance — the reason the call exists — is unchanged.

Kill switch: `WADJET_VECTOR_REUSE=0` (`optswitch` name `vector-reuse`) restores
a fresh allocation per output batch. Local effect, `BenchmarkProbeEmitViewOutput`
at TPC-H widths: single-batch build (views only) 20.9 KiB → 4.6 KiB per output
batch (−78%), multi-batch build (eager gather) 118.3 KiB → 2.1 KiB (−98%),
24 → 7 allocs, −32% wall.

## 5. Consumer matrix (what flattens, what stays lazy)

| Consumer | v1 behavior |
|---|---|
| Chained HashJoinProbe | Compose (the headline win). Key columns needed for hashing are flattened individually (one column) or read through indices in the inline probes — start with flatten-key-column, measure, then teach `inlineIntProbe` indirection if it shows. |
| Filter family | Flatten only the filtered column(s); `Sel` manipulation itself is view-compatible (Limit needs nothing). |
| Project / expr | Flatten columns referenced by expressions; `DirectCopy` of a view passes the view through. `VecEval` path keeps its existing `Compact()` behavior. |
| HashAggregate | `FlattenViews()` on `Consume` — by then the upstream win is banked; teaching the agg fast paths indirection is a measured follow-on, not v1. |
| Sort / Window / TopN | `FlattenViews()` on `Consume` (they retain batches; pinning bases would defeat the memory win). |
| Shuffle / gather / collect sinks | v1: `FlattenViews()` before serialize. v1.5: `writeChunk` already gathers rows via `Sel` — reading through view indices there makes the final copy free for distributed fragments ending in an exchange. |
| Spill paths | Probe-side grace partitioning happens before views exist (input is concrete); spill writers flatten like other serializers. |

## 6. Distributed interplay

Nothing crosses the wire: views are an intra-fragment representation, always
resolved at the fragment's serializing sink. `.wshf` format, exchange
protocol, and stage materialization are untouched. Broadcast-cached builds as
view bases are safe under the existing refcount (views die with the probe
pipeline that holds the cache reference). The local fast path and worker
fragment path share `HashJoinProbe`, so both get the feature from one
implementation.

## 7. What v1 does NOT do

- No view output from Sort/Window/aggregate/scan — joins only.
- No expression/kernel indirection — flatten-on-touch everywhere except probe
  chains and (v1.5) shuffle serialization.
- No multi-base views: multi-batch builds keep the eager gather.
- No view spilling: any batch that reaches a spill/retain point flattens.
- Right/full-outer flush paths stay eager.

## 8. Observability, gates, rollout

Dyn-filter lesson applied from day 1: the feature must prove it engaged.

- Counters: `LateMatBatchesEmitted`, `LateMatViewColumns`,
  `LateMatBytesDeferred` (bytes NOT copied at emit), `LateMatFlattens`
  (where views got materialized, labeled by consumer). Logged per query at
  Info alongside `SortMergeJoinsPlanned`-style reporting.
- Flag: `--late-materialization` (bool). **Default ON since 2026-07-09**,
  after the phase-5 evidence landed: SF10 same-window pair −6.2% suite,
  SF100 pair −4.9% (51.0m→48.5m), Q08 −35.9%/−43.9% at the two scales,
  22/22 row-identical everywhere, engagement proven by plan-side dispatch
  markers (48 join stages) and worker runtime counters (825k view batches,
  3.6M columns serialized with no intermediate copy, 35k flattens).
  `--late-materialization=false` is the kill switch.
- Gates, in order:
  1. `batch` package unit tests: view round-trip for all 22 types, nested,
     null composition, Flatten/Compact/ToRows/RowAt/CopyValueFrom, MemBytes.
  2. Full exec suite + `-race`; SF0.01 22/22 forced-on.
  3. `cmd/tpch-harness --mode=local` both arms (hard rule for any
     coordinator/distribution-adjacent change).
  4. SF1 profile pair: expect `gatherVector`-in-probe and join-output alloc
     bytes to drop and `LateMatBytesDeferred` ≫ 0; wall may be flat at SF1
     (no-revert-on-serial-clog — CPU/alloc is the signal).
  5. Deploy-gated SF10 + SF100 same-window pairs (user approval per deploy);
     per the A/B method note, attribute per-query deltas only with the
     counters proving engagement, and treat ±15%/query as noise floor.

## 9. Phases

1. **Batch primitive**: view form on `Vector`, `Flatten`/`FlattenViews`/
   `FlattenColumn`/`HasViews`, MemBytes, generic-accessor audit
   (`GetValue`/`RowAt`/`Compact`/`CopyValueFrom` view-aware or guarded),
   exhaustive tests. Zero behavior change.
2. **Join emission + defensive flatten**: flag-gated view output from
   `HashJoinProbe` (inner+left), probe-input pinning lifecycle, flatten
   calls at every retain/serialize/expr touch point, counters. Gates 1-3.
3. **Composition**: chained-probe index composition + key-column handling;
   Filter/Project column-granular flatten. Gate 4 (SF1 profile pair).
4. **v1.5 shuffle indirection**: view-aware `writeChunk` so fragment-final
   copies disappear. Re-run gates 2-4.
5. **Activation**: SF10 + SF100 same-window pairs; flip default on evidence.
