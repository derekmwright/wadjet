# Shapegen deterministic window queries

Source: internal/oracle/shapegen/shapegen.go — func (g *Gen) genWindow(q *Query) {, moved 2026-09-11 (#1026)
Superseded: Qualified/computed window keys are now resolved and materialized by physical/window_keys.go and covered by window_partition_key_test.go; the claim they are silently dropped today is stale.

genWindow emits window functions, including the ones whose answer is CHOSEN
by a comparison rather than computed: MIN and MAX.

This arm did not exist. The package comment claimed window functions among
the shapes it generates and no arm produced one, so the fuzzer could not
have found #569 — windowed MIN/MAX failing the query outright for twelve of
the twenty-two types — nor any other defect that needs a window to fire.

Three rules keep the generated query's answer DETERMINED, which is what a
differential arm needs before it can call a difference a defect:

 1. The window's ORDER BY is the entry's single-column PK, so no two rows
    tie and a running or sliding frame has exactly one answer. Without a
    unique key the arm emits only whole-partition and OVER () shapes, whose
    answers do not depend on the order within the partition.
 2. SUM and AVG over a float are generated but marked NOT exact, because
    accumulation order is a legal difference (ADR-0013 §4).
 3. MIN, MAX and the value functions are marked exact: they return one of
    their input's values untouched, so any difference is a real one — and
    Opaque follows the column's kind, since an IPv4's rendered form does
    not order the way the address does.

The PARTITION BY key is written BARE and is always a plain column. A
QUALIFIED reference (`PARTITION BY t0.g`) or an EXPRESSION (`PARTITION BY
id % 3`) is silently dropped by the engine today and the window degrades to
a single partition (#585). Generating those shapes here would bury every
other window divergence under that one; widen this once it is fixed.

# Adding an arm moves every seed

The shape roll is weighted over this table, so a new entry changes which
shape each seed draws and therefore the QUERY each seed generates — in
every shape, not just this one. That is not a side effect to be avoided; it
is how a generator covers new ground. It does mean the arm lands with a
batch of unrelated findings: this one surfaced #593 (a comma-join whose
equi-predicate is in WHERE runs as a real cross product and is OOM-killed,
seed 51) and #594 (the same FROM shape answering ZERO ROWS, seed 24). Both
are pre-existing — the same suite with this arm's weight at 0 passes — and
both are now filed rather than hidden.
