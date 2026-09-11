# Computed window argument declarations

Source: internal/planner/physical/window_declared_output.go — windowComputedArgDecl / windowSpecOutputType, moved 2026-09-11 (#1026)

windowSpecOutputType declares the output type of one window expression over
the subtree rooted at the Window node that owns it. It is to windowOutputType
what aggSpecOutputType is to aggOutputType (#329, #333): the name list
answers everything that is input-independent, and the input column answers
the rest.

The value functions copy a value out of their argument column, so the
argument's catalog type is the answer. It is resolved through
inputColTypes — the Window node's own input schema, which stops at anything
that can rebind a name — and colRefDeclaredType, so a parameterized type
(DECIMAL without its scale, VECTOR without its dimension, the nested types)
declines the same way it does for a projection.

UNDECIDABLE cases fall back to windowOutputType's float64, which is exactly
today's behavior: a computed argument (`FIRST_VALUE(a || b)`), a column no
scan below annotates, two scans that disagree, or an input the walk cannot
describe at all. A confidently wrong type here is worse than the fallback —
nothing downstream corrects a declaration, which is the whole of #345.
windowComputedArgDecl types a window aggregate's COMPUTED argument from the
argument's own AST, and reports whether that expression carries an
int8-domain operand. It is aggComputedInputDecl's window face and asks the
same two functions — nodeDeclaredType and aggInputIsWideInteger — over the
same declarations, because the two spellings of one aggregate have to reach
the same type.

Both halves are needed and neither is enough alone. The DECLARATION cannot
tell int4 arithmetic from int8 arithmetic: every integer expression in this
engine computes in int64 (ADR-0024's recorded widening), so `w_i32 * 1` and
`w_i64 * 1` both declare INT64. The WIDTH walk cannot tell an integer
expression from a float one: it answers "not wide" for both. Together they
say what PostgreSQL says — `sum(int4-domain)` is bigint, `sum(int8-domain)`
is numeric, and anything that is not an integer keeps the float64 fallback.

Three guards keep it to the shapes it can see:

  - a BARE column declines here and is typed by colRefDeclaredType above:
    an int4 column already declares INT32, which IntegerAccOutputType
    answers directly.
  - no node, or an undecided expression, declines. Unknown keeps the
    existing fallback rather than narrowing on a guess.
  - the node must still SPELL the argument the operator will evaluate.
    respellOverAggregate rewrites InputCol when a window sits above an
    aggregate, and a rewritten argument resolves its ColRefs against names
    the stale AST does not carry — so a mismatch declines rather than
    typing a spelling that no longer applies.

#987 review B1: `SUM(CASE WHEN … THEN 1 ELSE 0 END) OVER ()` — TPC-H Q12's
shape, bigint in PostgreSQL and bigint in the grouped spelling here — went
out as DECIMAL(38,0) under OID 1700 where its grouped twin went out under
20. One question, two spellings, two boxes.
