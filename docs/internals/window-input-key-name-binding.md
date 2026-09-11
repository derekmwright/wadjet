# Window input key name binding

Source: internal/engine/exec/window.go — Window.bindKeyNames, moved 2026-09-11 (#1026)

bindKeyNames rewrites every PARTITION BY term, window ORDER BY term and
input column to the spelling the input batch actually carries, and REFUSES
a partition or order key the input does not carry at all.

Both halves close #585. A window key was resolved with RecordBatch.
ColumnIndex's exact-name match at three separate sites — the columnar
compute, the external partition walker and the row-oriented spill path —
and every one of them treated the -1 as a key to SKIP. `PARTITION BY p.g`
over a batch carrying `g` therefore dropped out of the key list, and a
window whose only key dropped out degrades to ONE partition spanning the
input: ROW_NUMBER() numbered straight through three groups, SUM OVER
answered the whole-table sum, and nothing said a word. The same silence
covered a key that names nothing at all (`PARTITION BY nosuchcol`).

The qualified↔bare fallback is columnIndexFallback's, which is what every
other operator resolves a column with (the hash join's keys, the
aggregate's group keys), so a window resolves names the way the rest of the
engine does. Refusing what it cannot resolve is unresolvedAggColumn's rule
applied one operator over: an unresolvable GROUP BY key collapsing every
row into one group is the same defect as an unresolvable PARTITION BY key
collapsing every row into one partition, and the aggregate stopped
answering it in silence first.

An EXPRESSION key (`PARTITION BY id % 3`) never reaches the refusal: the
planner materializes it as a computed column named by the expression's own
text before the operator sees a row (physical.windowKeyProjections), the
same way a GROUP BY expression is pre-projected for the hash aggregate. A
key that reaches here unresolved is one nothing computed.

InputCol takes the fallback but NOT the refusal. It is not always a column:
COUNT(*) OVER () carries "*", and a constant argument carries its literal
text — the operator has no parser to tell those from a misspelled column,
and the planner is where an unknown one is caught. What the fallback fixes
is the qualified spelling after a join, whose symptom was an all-NULL
output column rather than a wrong one (#585's note).

The rewrite copies before it writes: NewWindow copies the WindowColumn
structs but not the slices inside them, which are the planner's own — on
the single-process path they are the logical plan's, and a cached plan
re-run against a differently-spelled input would otherwise see the previous
run's binding.
