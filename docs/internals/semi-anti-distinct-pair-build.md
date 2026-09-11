# Semi anti distinct pair build

Source: internal/engine/exec/join_semianti_ne.go — nePair, moved 2026-09-11 (#1026)

Distinct-pair build for `probe.col <> build.col` semi/anti joins
(docs/design/semianti-distinct-pair.md).

The decorrelated-EXISTS self-inequality class (Q21's "another supplier
on the same order" legs) probes with exactly one residual condition:
build value ≠ probe value. For that predicate, two distinct build-side
values per key answer the EXISTS for EVERY probe value — if a key has
≥2 distinct values, at least one differs from any x; with exactly 1,
the answer is v1 ≠ x. So the build collapses from a row-storing hash
table (batches + arena + per-candidate filter closure walks) to
key → (v1, v2, n≤2): no batch storage, no arena, probe = one lookup
plus at most two integer compares.

NULL semantics (SQL three-valued, matching the closure path):
  - build rows with NULL value can never satisfy `<>` — skipped at
    insert (a key whose every value is NULL behaves as absent: EXISTS
    is false).
  - probe rows with NULL value make every comparison UNKNOWN — EXISTS
    is false regardless of build content: semi drops, anti emits.
  - NULL probe keys keep the existing convention (semi drops, anti
    emits).
