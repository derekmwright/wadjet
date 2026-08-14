# Shared-subplan dedup — clone legs ride one subtree

Status: SHIPPED 2026-08-14 (pass `dedupeSharedSubplans`,
`internal/planner/physical/shared_subplan_dedup.go`; kill switch
`WADJET_SHARED_SUBPLAN=0`). Generalizes lever C of
`docs/design/exchange-reuse.md` from leaf-scan-fed exchanges to
join-rooted stage subtrees. Diagnosis:
`docs/benchmarks/gap-closing-diagnosis-2026-08-14.md` (lever #2).

## Problem

Subquery decorrelation clones whole legs of the physical plan:

- **Q11**: the scalar-subquery leg (total German partsupp value) is a
  stage-for-stage clone of the main leg — at SF100, 2× 80M-row partsupp
  scans, 2× supplier⋈nation broadcast joins, 4.55 GB shuffled where
  2.6 GB suffices, 2× 24-task hash joins. Its scan projects a strict
  subset of the main leg's columns.
- **Q17**: the decorrelated `AVG(l_quantity)` leg re-joins
  lineitem⋈part(Brand/Container) as a SEMI join that is row-equivalent
  to the main leg's INNER join — 2× full 600M-row lineitem decodes on
  the same workers, identical 600,982-row outputs.

## Mechanism

A planner pass over the final stage list, placed after
`rewireAggOverRawExchange` and **before `fuseStageChains`** (chain
fusion absorbs consumers into the legs, breaking clone symmetry; at
this position distributions are final and `pruneScanOutputColumns`
still sees the post-dedup consumer set).

1. **Fingerprint**: bottom-up structural hash per stage — JSON of the
   stage with identity fields zeroed (ID, ScanAlias, Columns,
   OutputColumns, estimates, exchange Build/ProbeAlias) and
   stage-reference fields (LeftDep/RightDep, FusedJoins.BuildDepStage…)
   resolved to dependency-slot indexes, plus child fingerprints in slot
   order. Projected columns are deliberately excluded — clones consumed
   by different outer queries always disagree on them (same rationale
   as `cteSubtreeHash`'s RequiredColumns exclusion). Only
   scan/replicate/repartition/join stage types fingerprint; anything
   else (or ScalarDependencies, dynamic-filter marks, etc.) refuses,
   so v1 never touches subtrees it doesn't model.
2. **Exact match** (Q11): join-rooted subtrees with equal fingerprints
   merge; consumers of the duplicate rewire to the keeper, and a
   liveness sweep from the plan's pre-rewire roots drops the orphaned
   subtree. Keeper choice: a lockstep walk requires one leg's scans to
   read a superset of the other's, pairwise (v1 has no union-columns
   merge; incomparable legs are skipped and logged).
3. **Semi≡inner** (Q17): a semi join with empty JoinFilter whose
   fingerprint-with-JoinType-"inner" matches a live inner join rewires
   its consumers onto the inner output — **without any
   build-uniqueness oracle**. Gate: every consumer is an aggregate
   with `GroupByCols ⊇ probe join keys` and only duplication-invariant
   functions (avg/min/max). Within one group all rows share the join
   key value, so the inner join's duplication factor (matching build
   rows per key) is constant per group — avg/min/max are invariant
   under uniform within-group duplication. This is strictly weaker
   than proving the build unique (which the catalog cannot do — PK
   metadata does not exist).

No runtime changes: stage outputs already support N consumers
(`outputs` map is never deleted from; Q18's rp-7 is read twice in
production) and scratch cleanup is per-query, so no refcounting.

## Correctness hazards handled

- **Qualification collision**: the join executor qualifies build
  columns only on probe-name collision. If the keeper's *extra* probe
  columns collided with a build output name, the build column's
  emitted name would flip from bare to qualified and a rewired
  consumer's bare reference would silently rebind to the probe column.
  The coverage walk rejects any pair where keeper-extra columns
  intersect build-side scan columns.
- **Self-join shapes**: a consumer already depending on the keeper is
  never rewired (would alias two join slots onto one input); the
  duplicate then survives intact (all-or-nothing per duplicate).
- **Field drift**: `TestSharedSubplanDedup_StageFieldCoverage` forces
  every future `Stage` field to be classified
  (hashed/excluded/reference/refused); unclassified fields fail the
  test with instructions to audit `stageEdgeRefs`/`rewireEdges`/
  `fingerprintAs`. Hash-by-default is the safe direction — an unhashed
  distinguishing field could false-match, but a hashed one only
  suppresses dedup.

## Effect (plan level, verified)

- Q11: 15 → 10 stages; one partsupp scan, one join pipeline; the
  scalar leg's partial aggregate reads the shared join output.
  SF100 shape (hash-join legs) dedups identically
  (`TestSharedSubplanDedup_Q11ShuffleShape`).
- Q17: 15 → 11 stages; semi leg dropped (`semi_to_inner=true`); the
  AVG partial reads the inner join's output. Halves the 600M-row
  lineitem decode and removes join-5's 55s-cumulative sink residual
  from the plan entirely (the separately-diagnosed sink_ms constant —
  still worth understanding, but no longer on Q17's path).

## Validation

- Plan-level: `shared_subplan_dedup_test.go` (both Q11 regimes, Q17
  semi≡inner, kill switch, self-join guard, incomparable-columns skip,
  fingerprint sensitivity, field-coverage guard). Golden snapshots:
  only q11/q17 changed, 20 queries byte-identical.
- E2E: `coordinator/shared_subplan_e2e_test.go` — 3 workers, real NATS,
  multi-chunk tables, both arms vs Go-computed ground truth.
- Gates: `./internal/...` green, TPC-H SF0.01 22/22, worker+coordinator
  `-race`, `tpch-harness --mode=local` PASS.
- **SF100 pair owed** (deploy needs approval): multi-consumer reads of
  a large join output have not run at scale; expectation q11 8.6→~5s
  (Trino 5.2), q17 16.9→~12s steady with the second lineitem decode
  gone. Watch: join-2/join-6 output read amplification under the
  streaming-exchange local/peer tiers, q11 serial tail (unchanged,
  separate lever #3).

## Out of scope (v1)

- Union-columns merge when neither leg covers the other (log line
  `shared_subplan: skip` marks the residual).
- Aggregate/sort/window-rooted subtree dedup; leaf-exchange sharing
  with residual filters (exchange-reuse lever C's original form) —
  the subsume pass covers the filtered-subset case.
- Cross-query reuse (see exchange-reuse.md §6).
- q11's serial scalar tail (partials needlessly dep the scalar) —
  lever #3 in the diagnosis memo, untouched here.
