# Temporal arithmetic operand units

Source: internal/engine/expr/expr_scalar_fns.go — func temporalOperand(b *batch.RecordBatch, row int, e Expr, v any) (any, bool) {, moved 2026-09-11 (#1026)

temporalOperand resolves the date side of `date ± interval` to a value
intervalShift can read, and reports whether the operand is a date at all.

It is resolveTemporalArgs for the binary-operator path, and it exists for
the same reason: ColRef.Eval boxes a DATE column as its epoch-DAY number and
a TIMESTAMP column as its epoch-MILLISECOND number, and a bare number has
lost the unit that says which. Recovering it here — where the operand is
still a column reference whose vector knows its declared type — is what
#322 did for date_add/date_sub arguments; the operator never got it, so
`o_orderdate - INTERVAL '90' DAY` fell through to the numeric path and
projected the raw day number (issue #332). A resolved DATE is tagged
civilDate for the same reason it is there: the result renders, and a whole
day must render as a calendar date.

A CAST to a temporal type is resolved the same way and for the same reason:
it now BOXES its result the way the matching column type does (epoch days /
epoch milliseconds, see castTemporal), so the unit lives in the destination
type rather than in the number. Without this case `DATE '1998-12-01' -
INTERVAL '90' DAY` — TPC-H Q1's filter, and every typed date literal, which
the parser lowers to a CAST — would have fallen straight through to numeric
arithmetic once #340 stopped the cast passing its text along.

Text passes through as text, keeping the string path's own rendering.
Everything else — a bare integer, a computed expression, a column of any
other type — declines, and the caller's numeric arithmetic runs unchanged.
