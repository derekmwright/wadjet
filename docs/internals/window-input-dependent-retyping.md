# Window input dependent retyping

Source: internal/engine/exec/window.go — Window.retypeValueColumns, moved 2026-09-11 (#1026)

retypeValueColumns re-declares each input-dependent window function's
output type from the input vector it will actually read, the way
exec.Project resolves a projection's type from its input batch instead of
trusting the planner's declaration (project.go). It reports whether
anything changed.

Three families are input-dependent: the five value functions (their output
IS the input's type), MIN/MAX (the same, since #569), and SUM/AVG — whose
output is not the input's type but their ACCUMULATOR's, DECIMAL over a
DECIMAL column and FLOAT64 over everything else (#586).

Defence in depth for #345: the planner now resolves these types from the
catalog, but a declaration that arrives wrong — a spec built by a caller
with no schema to resolve against, an input type the planner had to decline
— is otherwise final, because Window allocates batch.NewVector(OutputType)
and every write of a value the vector cannot hold is dropped in silence.

The lookup is RecordBatch.ColumnIndex's exact-name match, which is how
computePartitionColumnar resolves InputCol, so the type declared here is
always the type of the vector the compute reads. A name the input does not
carry leaves the declaration alone — the compute finds no input column
either and writes nothing but NULLs.
