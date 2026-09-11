# Row filter schema check boundary

Source: internal/engine/exec/filter.go — Filter.Check, moved 2026-09-11 (#1026)

Check is the row path's half of the #147 guard, run ONCE on the first
batch: a predicate whose column references name nothing in the input is
a query error, never UNKNOWN on every row.

KernelFilter has refused that since #147, because a filter that matches
nothing is indistinguishable from genuinely empty data. The row
evaluator had no equivalent — expr.ColRef.Eval simply answers nil — so
every defect that handed this operator the wrong NAME came back as a
silent zero-row answer (#653). The check lives here rather than inside
the predicate because a Predicate returns bool and has nowhere to put
an error; callers that know the predicate's references set it
(expr.CheckFilterColumns), and callers that do not leave it nil.

WHO sets it is the whole of its safety, and the two paths differ. The
single-process planner sets it on every non-correlated row filter,
because in one process each operator DECLARES its output schema and an
empty join side still declares the columns it would have produced. The
DAG sets it only on a filter reading a base-table SCAN
(OpSpec.ScanSchemaFilter): a stage's input schema there is read back
from what an upstream task WROTE, and a hash-join partition whose build
side was empty writes only the join keys for the missing side — so a
build column that is legitimately NULL for every row of that partition
is absent from the schema, which is TPC-H Q20's
`ps_availqty > 0.5 * __scalar_0` and not a defect.
