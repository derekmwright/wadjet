# Eager join consumer boundary

Source: internal/coordinator/eager_feed.go — eagerEligibleJoinConsumer, moved 2026-09-11 (#1026)

eagerEligibleJoinConsumer reports whether join stage s may clear
dispatch on its two producers' eager feeds (memo §3.3/§6, Phase C2
scope). Requires:
  - a hash_join on the fragment path (no GroupByCols — those take the
    legacy task path, which reads frozen Task.Inputs). broadcast_join is
    out of scope (broadcast edges stay S3+KV per memo §7);
    sort_merge_join is dormant (gate off) and excluded from slice 1.
  - both primary deps are standalone exchange-repartitions (the feeds'
    producers); fused-build and chained-build deps keep the barrier
    (their real outputs ride task.FusedJoins[i].BuildFiles /
    task.Operators[i].BuildFiles — the dependency wait loop completes
    them before the clearance decision runs, so an eager dispatch sees
    real outputs for every non-primary dep).
  - not the gather-fused stage, no scalar deps (joins never carry
    dynamic-filter emits/consumes; checked defensively).

Stage-chain fusion (§13, docs/design/stage-chain-fusion.md) grows
Dependencies by exactly one per ChainedJoinSpec; a fused chain is
eligible like any other hash join — only the primary probe/build feed
eagerly, the chain's builds are complete by clearance time. A chain
with an absorbed partial aggregate carries ChainedAgg* fields, not
GroupByCols, so the fragment-path restriction above is unaffected.

The skew decision itself happens later, at the feed threshold
(eagerJoinWouldSplit) — this gate is structural only.
