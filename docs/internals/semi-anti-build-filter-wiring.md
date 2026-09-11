# Semi anti build filter wiring

Source: internal/planner/physical/semi_anti_build_filter.go — markSemiAntiBuildFilters, moved 2026-09-11 (#1026)

```go
// markSemiAntiBuildFilters wires probe-sourced dynamic filters onto the
// build input of semi/anti hash joins
// (docs/design/semi-anti-build-dynamic-filters.md).
//
// For a semi or anti join J, any build row whose key matches no probe row
// can neither emit (semi) nor block (anti) a probe row, so filtering J's
// build input by the key set of J's probe input preserves semantics
// exactly. The probe key set is collected at runtime: J's immediate
// probe-side dependency stage S gets an OUTPUT-side DynamicFilterEmit
// (its output IS J's probe input), and the build-side base scan B gets
// the matching ConsumeDynamicFilters plus a stat-dep edge B ← S. The
// coordinator's existing partial-merge and shuffle/scan consume threading
// do the rest; bloom false positives only KEEP extra build rows (safe),
// and a missing/failed emit degrades to an unfiltered build (safe).
//
// Q21 is the motivating shape: the raw 600M-row lineitem exchange feeds
// an EXISTS semi build and a NOT-EXISTS anti build (via the subsume
// flag) probed by only ~7M rows; the probe dep's l_orderkey set covers
// ~3-4% of the table. Q04 (EXISTS lineitem vs quarter-filtered orders)
// and Q22 (NOT EXISTS orders vs filtered customer) are the same class.
//
// Eligibility (v1):
//   - J is StageHashJoin with JoinType semi or anti and single-column
//     integer join keys.
//   - Build side: J.RightDepStage is either an exchange-repartition whose
//     sole dep is a leaf scan (Q21's subsumed raw exchange), or a leaf
//     scan carrying an absorbed Exchange (fuseScanShuffle, Q04). The
//     consume attaches to that scan stage B.
//   - Probe source: S = J.LeftDepStage must show reduction evidence — a
//     join, an aggregate, or a scan with pushed filters over a DIFFERENT
//     table than B. A raw same-table scan can only produce a ~100%-pass
//     bloom (and the runtime adaptive disable would bypass it anyway).
//   - S must not transitively depend on B or its exchange (the stat-dep
//     edge would create a cycle — Q21's join-16 probes join-12, which
//     builds from the same exchange; join-16 is skipped and instead
//     benefits from the consume its sibling join-12 installed on the
//     shared scan).
//
// NULL keys: the emit op skips NULL probe keys and the consume op drops
// NULL-key build rows — both correct for the engine's equi semi/anti
// operators, whose hash lookups never match NULL. (NOT IN's null-
// SENSITIVE semantics are already approximated as null-insensitive
// AntiJoin at decorrelation — decorrelateInSubqueries' documented punt —
// so this pass introduces no additional NULL hazard.)
```
