# Batch checked value writes

Source: internal/engine/batch/vector.go — func (v *Vector) SetValueChecked(i int, val any) error {, moved 2026-09-11 (#1026)

SetValueChecked is SetValue for a caller producing a stored VALUE rather
than ingesting an already-encoded one.

SetValue's DECIMAL arms answer a conversion they cannot make exactly with
the nearest thing they can store — the saturated end of the Int128 range
for text too wide at this scale, zero for text that is not a number, the
raw carrier for an integer box, and a float64 round trip for a float box.
Each is right for the caller it was built for (a comparison bound, #462;
ingest's already-scaled carrier, ADR-0018 §4) and each is a silently wrong
ROW anywhere else: a 10^30 union arm came back as
17014118346046923173168730371.5884105727 (#553) and an integer arm came
back divided by 10^scale (#547/#541).

So this sibling exists rather than a signature change on SetValue: the
unchecked writer keeps its callers and its cost, and every value-producing
row→batch path (FromRowsChecked, and through it the single-process
set-operation adapter) takes this one. Every DECIMAL box is exact-or-error
here — text through the checked parser, a float through its shortest
round-trip spelling, an integer refused outright — and the errors carry
PostgreSQL's SQLSTATEs: 22003 for a value with no carrier, 22P02 for text
that names no number (ADR-0024 item 4).

Every other type, and every other box, delegates to SetValue unchanged.

"Every other type" once included the CONTAINERS, and that was #898: a
DECIMAL leaf inside a ROW, an ARRAY or a MAP was written by SetValue's
recursion (child.SetValue, appendToVector) and got the unchecked contract
back — `not-a-number` stored 0.00, an integer 42 stored 0.42, a 53-digit
decimal stored a saturated Int128 — with no error, in the same call whose
scalar form refuses all three. So the walk descends: a container is
traversed HERE and every leaf takes the checked writer, with the field and
element path carried into the message so the refusal names WHICH leaf.
