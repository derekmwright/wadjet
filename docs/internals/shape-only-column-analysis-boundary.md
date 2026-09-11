# Shape only column analysis boundary

Source: internal/planner/logical/shape_only_columns.go — shapeLenFuncs, moved 2026-09-11 (#1026)

Shape-only column analysis — the planner half of the offsets-shape
evaluation class.

A byte-array column every one of whose uses reads its SHAPE rather than
its CONTENTS never needs its bytes materialized: the scan can decode it
as lengths alone (internal/engine/scan/lengths_decode.go). ClickBench
Q28, `SELECT CounterID, AVG(LENGTH(URL)) ... WHERE URL <> '' GROUP BY
CounterID`, uses URL twice and neither use reads a byte.

Shape uses recognized here, and the engine paths that make each one
byte-free:

	LENGTH / octet_length / bit_length over a bare column
	                          expr.ColShapeLen           (offsets subtraction)
	col IS [NOT] NULL         expr.ColIsNull / exec.NullCheckFilter (null mask)
	col = '' / col <> ''      expr.ColEmptyStr, kernel.emptyStringFilter,
	                          exec.ColumnCompare's empty test, and the
	                          scan-level RowPred, which reads pages not vectors
	COUNT(col)                kernel.ResolveBatchCount   (null mask)

The analysis is deliberately conservative and fails CLOSED: a column is
marked only when every reference to it, anywhere in the plan, is one of
those forms. Anything the walk cannot classify — an unhandled AST node,
an unhandled plan node, more than one scan (column names are not
table-qualified in the accumulated sets), a join, DISTINCT, a set
operation, a wildcard scan with no pruned column list — abandons the
analysis for the whole query. A false positive here is a wrong LENGTH
value, so the bar is "provably every use", not "no use I happened to
see".

The correctness net behind it: a shape-only column that still reaches a
value consumer panics at batch.BytesColumn.Value rather than answering
from an empty arena.
