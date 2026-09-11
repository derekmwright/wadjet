# Batch computed decimal write boundary

Source: internal/engine/batch/vector.go — func (v *Vector) SetComputedChecked(i int, val any) error {, moved 2026-09-11 (#1026)
Superseded: Declared-precision checks also exist in exact arithmetic At wrappers and expression evaluation; set-operation coercion is not the only boundary. These batch writers still lack declared precision.

SetComputedChecked is SetValueChecked for a caller whose value came out of an
EXPRESSION rather than off a wire or a file.

The two differ over exactly one box: an INTEGER. SetValueChecked refuses one
into a DECIMAL column because its callers are row→batch adapters, where an
integer box is the ALREADY-SCALED carrier of ADR-0018 §4 and storing it as a
value would divide it by 10^scale (#547/#541). An expression has no such
spelling: `expr.ColRef` over a DECIMAL column boxes the value's rendered
TEXT, exact arithmetic boxes text, and the only way an integer reaches a
DECIMAL output vector is as a genuine value at scale 0 — the integer branch
of a choice construct PostgreSQL types numeric (#695).

So this sibling exists rather than a widening of SetValueChecked: the row
adapter keeps its refusal, and the expression sites (exec.Project,
physical.aggPreProject and expr.EvalDecimalInto) take this one. It is also
what makes the box rule DRIFT-PROOF. expr.decimalChoiceArm classifies arms
by node kind to compute the result TYPE, and a kind it has not learned yet
makes the fold decline — which used to mean the integer box met the DECIMAL
vector the PLAN had already allocated and the query died with a 22003 for a
value PostgreSQL answers (`CASE WHEN … THEN d ELSE CAST(i AS BIGINT) END`).
The store no longer depends on that classification being complete.

The scaling is checked: an integer too large to carry at this scale is
22003, never a wrapped number.

Two limits it shares with SetValueChecked, both recorded rather than fixed
because no SQL surface reaches either today:

  - A box of a type a DECIMAL column cannot take at all — a bool, a []byte —
    falls through to SetValue, whose mismatch() PANICS. The query boundary
    recovers it, so a client sees an internal error rather than PostgreSQL's
    42804 datatype_mismatch. Nothing in the SQL layer produces such a box for
    a DECIMAL output: the type fold declines for every non-numeric arm, so
    the vector would not be a DECIMAL one.
  - Neither writer enforces the DECLARED PRECISION, only the scale, because
    batch.DecimalColumn carries Scale and no precision. So a value inside the
    Int128 but past the type's own 10^p band is stored:
    `GREATEST(numeric(38,30), 100000000::bigint)` writes 39 digits under a
    type capped at 38. ADR-0024 item 4 makes the declared precision the bound
    that matters, and the set-operation coercion is the only door that
    currently enforces it (physical.setOpCheckedDecimalText).
