# Batch vector type mismatch boundary

Source: internal/engine/batch/type_mismatch.go — type TypeMismatchError struct {, moved 2026-09-11 (#1026)

TypeMismatchError reports a write of a value a vector has nowhere to put:
SetValue was handed a Go value whose type has no conversion into the
vector's storage.

Until #361 such a write VANISHED — the slot kept its zero value and was
marked valid — which is the mechanism behind an entire bug family
(#310, #327, #331, #333, #345, #353, #361, #371, #372): some declaration
upstream picks the wrong vector type, and instead of an error the query
answers 0 on every row. The write site is the one seam every one of those
defects must cross, so it now panics with this typed value.

The panic carries a query ERROR, not a crash: it implements the
exec.FatalEvalPanic contract (Error + FatalEvalError), the same route the
expression evaluator uses for a condition with no error return (#347).
The pipeline drivers, the worker's task-level recover, the coordinator's
and the embedded API's query entries all convert it back into an error —
"a wrong type may cost a wrong answer, never the server" (#310) still
holds, with the improvement that it now costs an ERROR instead of a wrong
answer.

The deliberate non-panics: a nil value is a NULL (WriteNullAt); STRING and
BYTES destinations coerce any value through its string form, which is a
documented rendering (group keys rely on it); and a PARSE failure of a
value-level string (an unparseable IPv4, MAC, UUID) keeps its historical
null-ish result — the type was right, the value was not.
