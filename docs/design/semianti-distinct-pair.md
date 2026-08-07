# Distinct-pair builds for `<>`-filtered semi/anti joins

Status: shipped (2026-08-07). Kill switch: `WADJET_SEMIANTI_NE=0`.

## The class

Decorrelated `EXISTS`/`NOT EXISTS` subqueries with a self-inequality —
"another row with the same key but a different value" — compile to
semi/anti hash joins whose ONLY residual condition is
`probe.col <> build.col`. Q21 is the canonical shape (both legs:
"another supplier on the same order"), but the operator keys on the
predicate form, not the query.

The generic filtered semi/anti build stores build batches + arena refs
and walks per-key candidate chains through a filter closure at probe
time. At SF100 Q21 that was two ~30M-row row-storing builds inside the
fused stage (26.8s summed task CPU cold after shared-build fusion) for
an answer that needs almost none of that state.

## The observation

For the predicate `∃ build row: key = k AND value ≠ x`:

- a key with **≥2 distinct** build values satisfies it for EVERY x
  (two distinct values cannot both equal x);
- a key with exactly **1 distinct** value v1 satisfies it iff `v1 ≠ x`.

So per key, two distinct values are a complete answer. The build
collapses to `key → (v1, v2, n≤2)` — 24 B/key, no batch storage, no
arena — and the probe is one hash lookup plus at most two integer
compares. No closure, no chain walk.

## Mechanism

- **Recognition** (`physical.ParseSemiAntiNE`): the join filter must be
  EXACTLY one column-to-column `<>`/`!=` condition. Conjunctions,
  literals, and other operators stay on the generic closure path. Wired
  at both HashJoin construction sites (worker fragment executor,
  single-process planner); `SemiAntiFilter` stays set as the fallback.
- **Activation** (`exec.neTryEnable`, first build batch): requires the
  int-key fast path and an integer value vector. On failure the build
  falls through to the generic filtered path unchanged.
- **Build** (`insertNEBatch`): keyOnly-class flat path — the
  partition-on-arrival spill machinery is skipped exactly as
  `SemiAntiKeyOnly` skips it, because the state is bounded-compact.
  NULL keys and NULL values are skipped at insert (neither can satisfy
  the equality/inequality).
- **Probe** (`probeNESemiAnti`): typed loop; NULL probe key or value →
  EXISTS is false (semi drops, anti emits), matching SQL three-valued
  semantics and the closure path.
- **Engagement marker**: `semi_anti_ne: distinct-pair build active`
  (worker log) — the A/B grep.

## Interaction with the fused Q21 stage

Shared-build fusion (f6a47c6) already runs the semi+anti pair in one
stage reading one exchange. With distinct-pair builds, each leg's build
becomes a single decode pass + compact table update; the remaining
duplication is the decode itself (both legs read the same partition
slice). Collapsing to literally ONE build serving both probes (the
anti leg's `receipt > commit` build filter becomes a second value-pair
tracked per key) is the follow-up slice if the pair shows the decode
duplication still matters.

## Memory note

Distinct-pair state is not partition-spillable (same as keyOnly). A
key-heavy NE build (~24 B/distinct key + table) is 20-50× smaller than
the row-storing build it replaces, so pressure strictly decreases on
the shapes that engage. The defensive fallback (planner recognized the
filter but the value column is not an int vector at runtime) builds
flat WITHOUT spill; that shape — string `col <> col` semi/anti under a
spill-configured worker — does not occur in the current planner corpus.

## Validation

- Differential unit tests vs the closure path: semi+anti, int64+int32,
  duplicate keys, 1-vs-2 distinct values, NULLs on both sides of both
  columns, selection vectors (`join_semianti_ne_test.go`).
- Parse recognition table incl. kill switch.
- TPC-H SF0.01 22/22; harness local small both arms; SF10 q21/q04/q22/
  q16 engagement + row/value-sig identity vs prior runs.
