# Semi anti build dedup contract

Source: internal/planner/logical/semi_anti_dedup.go — dedupSemiAntiBuildSide, moved 2026-09-11 (#1026)

dedupSemiAntiBuildSide wraps the build side (right child) of every SEMI
or ANTI join in a GroupBy on the join keys, so the hash-join build phase
constructs a hash table sized to NDV rather than raw row count.

Motivation: for Q04 (orders ⨝SEMI lineitem on l_orderkey) and Q21
(lineitem self-joins on l_orderkey), the build side is a fact table
(30M-60M rows) whose join key has much lower cardinality (~15M
orderkeys). Without dedup the build hashtable holds 2-4× the entries
it needs, the dynamic-filter eligibility check rejects it as too big
(Q04/Q21 SF10 A/B audit, 2026-05-25), and the probe is slowed by
duplicate-key probe collisions for the same orderkey.

Semantics: SEMI / ANTI joins return a subset of LEFT rows based on
existence in the RIGHT side. Whether RIGHT has duplicates is
irrelevant to the result — only the SET of right keys matters. So
wrapping RIGHT in GroupBy(rightKeys) is a semantics-preserving
rewrite that bounds build cardinality by NDV.

This pass runs after pushdownPredicates (so filters land on the inner
scan before dedup) and before reorderJoins (so the dedup'd subtree's
cost estimate flows through the join reorderer).
buildDedupToggle is the #287 kill switch. This pass CHANGES THE ROW SET when
it is wrong, in both directions — a build side narrowed to too few columns
makes a semi join answer nothing and an anti join answer everything (#562) —
so it belongs in the registry the invariance oracle enumerates. Had it been
there, the oracle would have reported #562 as a divergence under
WADJET_SEMIANTI_BUILD_DEDUP=0 the first time a two-key correlation entered
any corpus, instead of the shape having to be noticed by hand.
