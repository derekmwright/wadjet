# Exchange reuse and shuffle-byte reduction (Q18/Q21 arc)

Status: DESIGN — evidence complete, levers A1/A2 root-caused to specific
code sites, B/C designed. No code yet.

Author: 2026-07-21 session. Evidence run: SF100 `results/20260721-120005`
(bin d484147, steady pass), local plan dumps at `sqlToStages` workerCount=3.

## 1. Problem

Q18 (4m17) + Q21 (3m44) are 32% of SF100 steady suite wall. Both are
span-length-bound (arrival-waits memo): their walls are the lengths of
their exchange-repartition legs, not barrier waits. Those legs move far
more bytes than the queries need — most of it from two mechanical
defects, not from any inherent shuffle cost.

Per-leg measurements (steady pass, coordinator `task result` sums):

| query | leg | rows | bytes | B/row | contents |
|---|---|---|---|---|---|
| Q21 | repartition-11 (l2 EXISTS leg) | 600.0M | **85.69 GB** | 143 | raw lineitem, FULL WIDTH |
| Q21 | repartition-2 (l1 leg) + its scan-0 | 379.4M | **54.18 GB** (×2: scan write + shuffle) | 143 | filtered lineitem, FULL WIDTH |
| Q21 | repartition-15 (l3 leg) | 379.4M | 9.30 GB | 25 | filtered lineitem, pruned ✓ |
| Q18 | repartition-7 (outer join leg) | 600.0M | **85.69 GB** | 143 | raw lineitem, FULL WIDTH |
| Q18 | repartition-15 (join-8 output re-shuffle) | 600.0M | **40.13 GB** | 67 | already partitioned on the same key |
| Q18 | repartition-2 (orders leg) | 150.0M | 18.91 GB | 126 | full-width orders |

Needed widths: l2 needs `{l_orderkey,l_suppkey}` ≈ 25 B/row (~12 GB);
Q18's outer leg needs `{l_orderkey,l_quantity}` ≈ 25 B/row (~15 GB); l1
needs ≈ 25 B/row (~9.5 GB). The pruned l3 leg proves the target width is
achievable today — 25 B/row on the identical table.

Excess ≈ **200 GB of S3/network traffic per steady pass in these two
queries alone**, and the absorbed-scan defect (A2) applies to every
query whose unfiltered probe/build legs shuffle (Q03/Q04/Q05/Q07/Q12
shapes), so the suite-wide number is larger.

## 2. Root causes (found, with code sites)

### A1 — polluted `Stage.Columns` defeats the runtime projection guard

Plan dump, Q21 `scan-0` (the l1 leg):

```
cols=[l1.l_receiptdate l_orderkey l_commitdate l1.l_commitdate s_suppkey l_receiptdate l_suppkey]
```

Two kinds of junk: alias-qualified duplicates (`l1.l_receiptdate`) and
OTHER-TABLE names pulled in through join keys (`s_suppkey` on a lineitem
scan). The worker's parquet projection
(`cachedFileStreamSource.projectColumns`) is deliberately all-or-nothing
— one name missing from the file schema and it silently reads the full
schema (the guard exists for derived-column names whose base inputs are
unknown). Junk names trip the guard → full-width scan → full-width
shuffle. Q21 scan-0/repartition-2 measured 143 B/row against a clean
~25 B/row plan.

Fix (planner): sanitize scan `Columns` at emission — resolve
`alias.col` to `col` when the alias belongs to this scan, drop names
that belong to other tables (they arrive via join-key column
collection). The guard itself stays (it is correct for genuinely
derived names); add a one-line WARN when it trips so this class of
regression is visible in worker logs instead of silent.

### A2 — scan-absorbed shuffles drop the column list entirely

Unfiltered leaf scans are not dispatched as scan stages; their
repartition reads base parquet directly. `dispatchShuffleStage`
(execute_stage_dag.go) synthesizes the source it hands to
`runShuffleSide`:

```go
synthetic := physical.Stage{
    ID:        stage.ID + "-src",
    Type:      physical.StageScan,
    ScanFiles: sourceFiles,
    TableName: stage.TableName,
    EstimatedBytes: upstream.Bytes,
}   // NO Columns — prunedScanColumns() returns nil → task.Columns nil → full width
```

The worker path is already correct (`executeShuffle` →
`newCachedFileStreamSourceWithProjection(..., task.Columns)`, with a
projection test); the coordinator just never gives it columns. The
header comment on `dispatchShuffleStage` even records the
over-approximation.

Fix (coordinator + planner): when the repartition's dependency is a
pure table scan, carry that scan's (sanitized, post-A1) `Columns` into
the synthetic stage. `prunedScanColumns` already intersects with the
catalog schema, so junk cannot leak through this path. For chained
(non-scan) shuffles the input is WSHF and columns are ignored — nil
stays correct there.

**Explains**: Q21 r-11 85.69 GB, Q18 r-7 85.69 GB (byte-identical —
both are full-width raw lineitem), Q18 r-2 orders 18.91 GB, and the
same shape suite-wide.

### B — identity re-shuffle of already-co-partitioned join output

Q18 re-shuffles join-8's 600M-row output on `l_orderkey`/24
(repartition-15: 40.13 GB, 154 s span) even though join-8's output is
already hash-partitioned by its join key (`o_orderkey = l_orderkey`,
count 24 — both its inputs were shuffled to that layout). Local plan
reproduces it (repartition-49 in the plan dump: join-8 output
re-shuffled on `o_orderkey`/24 — the identity case is visible at plan
time, so it is testable at SF0.01).

Root cause: `OutputDistribution` labels a join's output with the PROBE
side's key NAMES; the downstream consumer requires the other side's
name for the same value set. `Distribution.Satisfies` compares key
strings — no equivalence classes — so `HashPartitioned[o_orderkey]/24`
fails `ClusteredOn[l_orderkey]` and `EnsureDistribution` inserts the
exchange.

Fix (planner): propagate join-key equivalence — after an equi-join,
the output distribution's key set includes both sides' names for each
join-key pair (or: `Satisfies` consults an equivalence map carried on
the stage). Both Trino and Spark keep exactly this equivalence-class
structure in their partitioning properties. Guard: counts must match
exactly; only equi-join keys create equivalences; outer joins only on
the preserved side.

### C — duplicate/subsumable exchanges of the same relation

Q21 shuffles lineitem on `l_orderkey`/24 twice from base data:
raw (l2, 600M rows) and filtered `l_receiptdate > l_commitdate`
(l3, 379M rows). The raw shuffle is a superset of the filtered one.
One shared shuffle can serve both consumers:

- key, count identical (l_orderkey, 24);
- columns: union (l2 {l_orderkey,l_suppkey} ∪ l3 {+l_receiptdate,
  l_commitdate} — or a scan-computed boolean, +1 B/row);
- filter: weakest of the legs (here: none);
- each consumer applies its residual filter + projection post-shuffle
  (a filter OpSpec at the consumer's input; extra columns ride the
  late-materialization path until touched, so the l2 consumer pays ~0
  for the widened schema).

Fingerprint for sharing: (dep = pure scan of same table, sanitized
column union, key list, partition count, weakest filter). v1 scope is
leaf-scan-fed exchanges only — post-join exchange CSE and cross-query
reuse are explicitly out (scratch is per-query; cross-query needs
invalidation).

Correctness constraints (all verified against current machinery):
- consumers with dynamic-filter pushdowns are EXCLUDED from sharing in
  v1 (a shared scan must not apply one consumer's blooms to another's
  rows);
- task retries overwrite identically-named partition files — sharing
  changes nothing (consumers already tolerate re-published outputs);
- skew-split reads per-partition byte vectors per consumer join —
  unaffected (read-only over shared files);
- eager feeds are keyed per producer stage — a shared stage feeding two
  consumers is the existing multi-dependent shape (flag is off anyway);
- scratch lifecycle is per-query (`queries/<id>/` purge) — no
  refcounting needed.

## 3. What this replaces

The original arc pitch was "Q21 shuffles lineitem 4×, dedup the
exchanges." The evidence pass shows dedup (C) is the SMALLEST of the
three levers: after A, the shared-shuffle candidate legs shrink from
95 GB to ~21 GB combined; C then saves the ~9 GB filtered leg plus one
600-file scan pass and 12 shuffle tasks. A and B are pure waste
removal with no plan-shape change and no new invariants; they go first.

## 4. Expected effect (steady pass, from §1 measurements)

| lever | traffic removed (Q18+Q21 only) | plan change |
|---|---|---|
| A1 | Q21 scan-0 54.2→~9.5 GB, shuffle r-2 54.2→~9.5 GB | none (same stages, narrow payload) |
| A2 | Q21 r-11 85.7→~12 GB; Q18 r-7 85.7→~15 GB, r-2 18.9→~6 GB, r-3 2.8→~1 GB | none |
| B | Q18 r-15 40.1→0 GB (stage eliminated, 154 s span) | one fewer exchange stage |
| C | Q21 r-15 9.3→0 GB, −12 shuffle tasks, −1 scan pass of 600 files | two exchanges merge into one |

Wall-clock translation is deliberately unforecast: legs overlap
(Q21 runs ~2.8 stages in flight), so byte savings convert sublinearly.
The SF100 pair is the arbiter. Suite-wide, A also narrows absorbed
shuffle legs in Q03/Q04/Q05/Q07/Q12 shapes for free.

## 5. Sequencing and validation

1. **A1+A2 together** (one PR): planner column sanitization +
   synthetic-stage column carry + guard-trip WARN. Plan-level unit
   tests (assert sanitized `Columns` on Q18/Q21 fixtures; assert
   synthetic stage carries columns); worker projection test already
   exists. Gates: unit, SF0.01, `tpch-harness --mode=local` (both
   states of nothing — this has no flag; it is a defect fix, kill
   switch `--shuffle-projection=false` restores old behavior for A/B
   only).
2. **B** (second PR): key-equivalence in distribution properties +
   plan test asserting Q18's identity exchange disappears (local
   repro exists). Kill switch `--exchange-elide-copartitioned=false`.
3. **C** (third PR, after A/B validate): exchange fingerprint +
   consumer residual filters. Flag `--exchange-reuse`, default off for
   its first SF100 pair, flipped on its own evidence.
4. One SF100 pair validates the A+B batch (rows 44/44 vs baseline;
   watch Q18/Q21 headline, Q03/Q04/Q05 for the suite-wide A effect,
   and `spread=`/`fragment task phases` lines from PR #247 for leg
   attribution). C gets its own pair.

Adjacent work note: Q18's t=3 sort/limit final (HELD task #3 in the
handoff) remains untouched by this arc; fold it in only if B's stage
elimination shifts its position on the critical path.

## 6. Rejected alternatives

- **Coordinator-level output cache keyed by fingerprint** (instead of
  planner CSE for C): catches the same within-query cases but hides
  the sharing from EXPLAIN and from plan tests; the planner is the
  architectural home (the plan graph is already a DAG).
- **Runtime intersection projection** (instead of A1's planner
  sanitization): unsafe — the all-or-nothing guard exists precisely
  because an unknown name may signal missing derivation inputs.
  Sanitizing at the source keeps the guard's semantics intact.
- **Cross-query shuffle reuse**: real (Q18-r7 ≡ Q21-r11 today,
  byte-identical), but needs invalidation + shared-scratch lifecycle;
  not worth opening before within-query waste is gone. Revisit only if
  post-A/B/C profiles still rank shuffle bytes first.
