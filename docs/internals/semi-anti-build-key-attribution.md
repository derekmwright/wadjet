# Semi anti build key attribution

Source: internal/planner/logical/semi_anti_dedup.go — extractRightJoinKeys, moved 2026-09-11 (#1026)

extractRightJoinKeys reads a join condition STRUCTURALLY and returns the
build-side key of every one of its conjuncts, or nil when even one conjunct
cannot be attributed.

All-or-nothing is the whole contract. The caller projects the build side
down to exactly these keys, so a key list that is short by one conjunct
deletes a column the join still compares and the join then matches NOTHING
— a semi join answers zero rows and an anti join answers every row, both
silently.

This used to split the text on " and " and then on the first "=". The
condition a decorrelated EXISTS/IN writes is rendered with " AND "
(renderDecorrelatedKeys), which that split does not see: a two-key
correlation came through as ONE part whose right operand was the literal
text "b.k AND a.k2 = b.k2", and the only key that survived was the first
conjunct's (#562). It is the same lexical-where-the-condition-is-structural
defect physical.parseJoinKeys was rewritten for in #351, one layer up, so
this reads the same way: parse, flatten the top-level ANDs, and require
each conjunct to be an equality between two bare column references.

Side membership is decided by what the build subtree's ROOT EMITS, which is
the schema the narrowing's own Project will read. It used to be decided by
collectSubtreeColumns — every column the subtree READS anywhere — and the
two differ exactly where a Project renames: over a derived table
`(SELECT c_bool AS k FROM typemx GROUP BY c_bool) b`, the read set holds
`c_bool` and the emitted set holds `k`, so `c_bool = k` attributed the BUILD
key to `c_bool` and projected a column the build root does not have —
`column "c_bool" does not exist in the input schema`, at build time, on
every arm. That was unreachable until a derived table could BE a build side
(#852); it is a defect in this attribution either way.

emittedColumns needs the scan annotation, so an un-annotated subtree emits
nothing and the read set is the fallback — the pre-#852 behaviour, and a
decline at worst. A name that resolves on BOTH sides (a self-join's
`k = k`) is not attributable from the condition alone and bails, as it
always has.

The last decline is about the narrowing's own Project rather than the
condition: it aliases every key to its BARE name, so a QUALIFIED key would
be renamed out from under the join that still asks for it. See the comment
at the check.
