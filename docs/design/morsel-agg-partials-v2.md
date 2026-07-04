# Morsel v2: bounded aggregate partials — design memo

Status: DRAFT for review, 2026-07-03. Grounded against branch
`feat/morsel-execution-v1.5` @ `0cf95ec` (= main `02f461c` + morsel
v1.5–v1.7). Companion to `morsel-execution.md` (v1 design + §4.1.1/§4.3
postmortems). No code yet.

## 1. Problem

The SF100 v1.7 acceptance run (2026-07-03) completed Q01–Q16 with correct
rows and real wins (Q01 −17%, Q06 −15%, Q08 −8% vs baseline), then Q17's
fused scan-aggregate (`GROUP BY l_partkey`, ~18–20M distinct keys per task
shard) killed all three workers: 21.6 GB LIVE heap after GC against a
21.3 GB GOMEMLIMIT. Heap profiles
(`s3://wadjet-bench-sf100-use2/debug/morsel-v17-treatment-20260703/`)
attribute the bytes to HashAggregate **breaker** state. Three stacked
failures:

1. **k× partial-state multiplication.** `runBreakerConsumeParallel` gives
   each of k=8 consumers a `CloneSink` HashAggregate partial. High-NDV
   keys have no locality across morsels, so each clone accumulates nearly
   the full key set: per-clone SoA state for 20M keys ≈ 1.2 GB, ×8 clones
   ×mc=4 tasks ≈ 38 GB demand. This is the §4.3 rule-1 hazard realized.
2. **Clones are pressure-blind by design, and the primary's trigger
   under-counts.** Clones run on `SpillManager.TrackingOnlyView()`, whose
   `ShouldSpillFor` returns false unconditionally — including the heap
   backstop (`memory/spill.go:240-243`). The primary and the fragment
   runner's collapse check compare the *tracked* bytes to the budget, and
   tracking under-counts (§2.3), so neither tripped: zero breaker
   collapses fired during Q17 (the linear-path heap collapse fired
   correctly during Q05). The 0.95×GOMEMLIMIT heap backstop arms only
   inside the GC death spiral that starves heartbeats — too late by
   construction.
3. **The barrier merge is O(total) at the worst moment.** `mergeSinkState`
   has a cheap SoA↔SoA path (`mergeIntGroupSoA`, `aggregate.go:2818-2823`)
   but its fallback migrates **both** sides to the generic map
   (`aggregate.go:2831-2833` → `migrateToGenericMap` →
   `materializeFlatAccums`), materializing a second full copy of all
   partials (14.2 GB + 9.6 GB cum in the profiles) and nil'ing the SoA
   arrays so every subsequent merge also takes the fallback.

## 2. Verified machinery (what already exists)

2.1 **Canonical, instance-independent partial-state runs.** The partial
spill file format (`aggregate_partial_spill.go:30-53`) stores groups
sorted by a canonical binary sortKey; `kWayMerger`
(`aggregate_partial_spill.go:1082-1174`) merges runs by sortKey equality
via `Accs[i].Merge(...)`. Merge does NOT depend on shared partition ids or
a common `drainK` — runs written by *different HashAggregate instances*
merge correctly. `finalizeViaPartialMerge` assembles the in-memory
remainder (`newPartialGroupCursor`, SoA-direct, no materialization) plus
one `newFileRunSource` per file.

2.2 **Run writing needs only a directory.**
`newPartialSpillWriter(h.Spill.SpillDir(), header)`
(`aggregate_partial_spill.go:1420,1522`) creates files with `os.Create`.
`TrackingOnlyView` carries `dir` and the shared tracker
(`memory/spill.go:148-157`), so a clone can write runs and release its
tracking charge without any shared spill state.

2.3 **The accounting gap is enumerable and mostly incremental-friendly.**
`groupMemoryUsage` (`aggregate.go:308-350`) counts hash-table buckets, the
24 B slim groupState, SoA arrays, and (via `strHashTable.MemoryUsage`,
`str_hash.go:244-246`) the arena copy of string keys. It does NOT count:
the ~96 B `groupStateExtras` struct, `keyValues []any` boxed keys,
per-group `accs []kernel.Accumulator`, `distinctSets` maps + contents,
`extraState`, `h.keys [][]any`, `h.serializedKeys []string` (two extra
full copies of every string key), or generic-map overhead. Int-keyed SoA
is nearly exact — the documented "accuracy budget" (`aggregate.go:316-323`)
— which is why drift is fatal specifically on paths that leave pure
int-SoA. All uncounted categories are append-once per group, so
insert-time counters (bump on `ensureExtras` and the append sites) avoid
O(groups) walks; the string arena already demonstrates the pattern.

2.4 **Streaming emit already avoids materialization.** `Next` emits
SoA-direct (`loadAccFromFlat`, `aggregate.go:2636-2640`); the ~3 GB
materialize is confined to the migrate paths (comment
`aggregate.go:2423-2438`).

2.5 **Row-level partition routing does not exist.** The only
partition-by-hash selection is per-group at drain time
(`spillPartialPartitions`: `fibHash(key) & (K-1)` over `ForEach`,
`aggregate_partial_spill.go:1498-1503`); `consumeBatchIntGroup` never
surfaces a per-row hash.

## 3. Design

The unifying idea: **a clone partial is a bounded buffer, not an
unbounded aggregate.** Aggregate state that exceeds the bound leaves
memory as canonical sorted runs — the same bytes it would eventually
become anyway on the validated external-merge path. The barrier stops
being an O(state) in-memory merge and becomes (mostly) a file handoff.

### 3.A Bounded clone partials with self-drain to run files

- Each clone tracks its own state bytes with the byte-true incremental
  counters (§3.B) — a plain field, no tracker round-trip needed for the
  threshold check.
- When a clone's state crosses `clonePartialDrainBytes` (derived, not a
  new tunable: `sharedPoolBudget / (2 · k · maxConcurrent)` — the same
  shape as the dispenser's `splitMinCost`; ~19 GB /(2·8·4) ≈ 300 MB at
  SF100), the clone drains **itself**: sort + write its full state as
  partial-state run files via the existing writer, release its tracking
  charge, reset to empty. Pure `os.Create` I/O onto NVMe; no
  SpillManager coordination, no locks shared with other clones.
- At the barrier, per clone: if it never drained and the SoA↔SoA merge
  applies, merge in memory exactly as today (low-NDV keeps today's fast
  path, zero regression); otherwise the clone drains its remainder to
  runs and hands the primary its **file list**, which the primary appends
  to `h.partialSpillFiles`. `Finalize` then k-way merges as it already
  does for self-spilled state.
- Never-OOM shape: per-task aggregate state ≤ k · drainBytes (clones) +
  primary state (real SpillManager governs it, now with truthful bytes) —
  bounded and tracked, independent of NDV.

### 3.B Byte-true aggregate accounting

- Add insert-time byte counters for every uncounted category in §2.3:
  bump on `ensureExtras`, `keyValues`/`accs`/`distinctSets`/`extraState`
  allocation sites, `h.keys`/`h.serializedKeys` appends, and per-entry
  string bytes on the non-arena copies. `groupMemoryUsage` becomes
  `counted-structures + incremental counters` — still O(1)-ish per call.
- `distinctSets` map contents: charge per inserted distinct key
  (len + fixed map-entry overhead constant). Estimate, but a truthful
  one; today's contribution is zero.
- This fixes the primary's spill trigger and admission for ALL aggregate
  paths (serial included — the drift exists today without morsels), and
  provides the clone drain threshold.

### 3.C Retire the migrate fallback in `mergeSinkState`

- Replace `migrateToGenericMap()`-both-sides (`aggregate.go:2831-2833`)
  with: drain the incompatible side(s) to partial-state runs and append
  file lists. In-memory merge remains only for the compatible fast paths
  (scalar, SoA↔SoA, dual-int).
- This removes the O(total) allocation spike from every barrier and also
  fixes the poisoning behavior (one fallback merge nil'ing
  `intFlatAccs` and forcing all later merges down the slow path).
- The Q17 profile proves the fallback fired despite both sides being
  int-keyed; the exact failed condition (`aggregate.go:2820`) must be
  identified during implementation — first task is an instrumented unit
  reproduction (candidates: post-partial-spill state on the primary,
  null-key demotion, compact-mode divergence). Whatever the trigger, with
  §3.C it stops being catastrophic.

### 3.D Pressure triggers (what §3.A/B make sound again)

- With truthful bytes, the primary's `ShouldSpillFor` and the breaker
  collapse check trip when they should. Keep the existing collapse rule
  unchanged; it becomes a guard against non-aggregate pressure, not the
  only line of defense.
- Clones stay pressure-blind on the shared manager (correct — they can't
  honor relief) but are now self-bounding, which is the property that
  actually matters.

### 3.E Partition-owned partials: explicitly deferred

The §4.3 end-state (clones own disjoint `fibHash & (K-1)` key ranges)
would additionally deduplicate accumulation work (each group accumulated
once instead of merged k ways). But it requires row-level routing that
does not exist (§2.5) — either a broadcast channel (every consumer sees
every morsel) or an intra-fragment scatter — both new concurrent
machinery. §3.A achieves the memory bound with existing, SF100-validated
components; the run merge is linear disk I/O on NVMe. Revisit
partition-owned only if v2 profiles show run-merge I/O or duplicate-key
merge CPU on the critical path.

## 4. Never-OOM analysis

New memory consumers: none. New bound: clone state ≤ drainBytes each,
charged to the shared tracker until drained. Runs live on NVMe (the
Q21-ENOSPC retry path already covers disk exhaustion). The barrier no
longer allocates proportionally to state. The primary remains the only
spiller on the real manager, now with truthful numbers. Serial paths:
byte-true accounting makes existing spill triggers fire earlier and more
accurately — behavior change is "spills when it actually should," the
never-OOM direction.

## 5. Blast radius

`internal/engine/exec/aggregate.go` (counters, mergeSinkState fallback,
clone self-drain entry), `aggregate_partial_spill.go` (writer reuse from
clones — expected near-zero change), `internal/worker/executor_fragment.go`
(barrier file handoff), `memory/spill.go` (none expected — TrackingOnlyView
already carries dir + tracker). Parquet, planner, sinks: untouched.

## 6. Gates

Unit: counter truth tests (allocate N groups of each shape, assert
groupMemoryUsage within a small bound of measured heap growth);
clone-drain round-trip parity (drain mid-consume, barrier file handoff,
Finalize merge = serial result, including float-order tolerance);
migrate-fallback retirement test (construct the incompatible-merge case,
assert no materializeFlatAccums allocation and correct results); -race on
all. Then SF0.01, harness local both flag states (race-built), SF10
same-window pair, SF100 with **Q17/Q18 as acceptance** — same-window
control per the 2026-07-03 rule.

## 7. Priorities

§3.B first (it is independently correct, fixes serial-path drift, and
everything else keys off truthful bytes), then §3.C (removes the
catastrophic allocation), then §3.A (the bound). Each lands separately
gated; §3.A is the SF100 unlock.
