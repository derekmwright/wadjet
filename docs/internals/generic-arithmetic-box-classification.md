# Generic arithmetic box classification

Source: internal/engine/expr/boxed_pair.go — classifyOperand, moved 2026-09-11 (#1026)

The GENERIC arithmetic node answers from its resolved mode for the
same reason its typed sibling above does — and it is the node that
NEEDS it, because it is where every operand with no typed protocol
arrives: a negated column, a CAST, a scalar function, and a
CHOOSING construct (`COALESCE(a, 0) + 1`), none of which satisfy
Float64Expr for compileBinOp to build the typed node from.

Its exact arm boxes the result as a DECIMAL COLUMN's value is boxed
— the rendered text, decArm.evalDecimalBox — so leaving it
unclassified sent every comparison above it to compare()'s byte
order: `(COALESCE(a,0) + 1) > 1` answered TRUE on the rows holding
1.00, because "1.00" sorts above "1", and `GREATEST(COALESCE(a,0)
+ 1, 2)` picked 2 over 13.75. The arm existed before the choosing
constructs reached it (`-a + 1`, `CAST(a AS DECIMAL(9,2)) + 1`,
`ABS(a) + 1`); giving them the exact kernel is what made the whole
class visible.

The int mode boxes a real int64 and answers boxNumber, exactly as
BinOpNumeric's does. Everything else — float arithmetic, and the
date/interval shifts this node also evaluates — keeps the
unclassified answer, because a temporal value is not a number and
declaring one here would be the wrong declaration rather than a
missing one.
