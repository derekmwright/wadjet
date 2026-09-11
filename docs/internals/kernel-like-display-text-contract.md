# Kernel like display text contract

Source: internal/engine/exec/kernel/compare.go — func likeTextRenderer(typ batch.TypeID) func(*batch.Vector, int) string {, moved 2026-09-11 (#1026)

likeTextRenderer resolves, once per column, the row->text function LIKE
matches a pattern against.

Wadjet renders every SIX network-native types and UUID as human-readable
text for CAST AS STRING and scalar function arguments (#484) — LIKE follows
the same convention rather than refusing outright the way PostgreSQL does
for inet/cidr/macaddr (verified live: `'10.0.0.1'::inet LIKE '10.%'` raises
"operator does not exist: inet ~~ unknown"). ADR-0012 item 11 records the
decision and its reasons. TypeCIDR is already TEXT in its own storage
(parquet/schema.go), so it falls through to the same BytesData path
TypeString/TypeBytes use.

That CAST-agreement claim used to be scoped to seven types, DATE excepted:
CAST AS STRING answered the epoch DAY (15007) for a DATE column while this
renderer, the projection and PostgreSQL's own `date::text` all answered
2011-02-02 — a separate defect in CAST's string family (#521). #521 also
found the identical gap for FLOAT32 (CAST AS STRING answered the
float64-widened text, not the float32-shortest-round-trip form this
renderer and the projection use). Both are fixed now — Cast.Eval's
string-family case renders every ColRef operand through the same
boxedTextOperand this file's LIKE kernel already agrees with — so the
claim covers every flat type again.

The default arm covers every remaining flat type (Int64/Float64/Bool/
Decimal/Date) with the row's own boxed value — fmt.Sprint on whatever
Vector.GetValue returns — never indexing BytesData on a column that does
not have it, which is the one invariant this function exists to restore
regardless of what LIKE against a given type is decided to MEAN. The four
container types never reach here at all: ResolveLikeFilterKernel refuses
them before calling this function (#522).

This rendering is the DEFINITION of what LIKE matches, so the
row-at-a-time path has to reproduce it rather than the other way round:
expr.boxedTextOperand reads Vector.GetValue for the four types ColRef.Eval
boxes differently (IPv4, MAC, DATE, FLOAT32) — the same resolver Cast.Eval
now shares — and wadjet.TestLikeAnswersTheSameAtBothSites sweeps every
flat type through both sites. Changing a per-type arm here without
checking that sweep re-opens the divergence it exists to catch.
