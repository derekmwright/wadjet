# Temporal cast box and error contract

Source: internal/engine/expr/expr_temporal_cast.go — func castTemporal(b *batch.RecordBatch, row int, operand Expr, v any, kind castTemporalKindT) any {, moved 2026-09-11 (#1026)
Superseded: castTemporalText supplies both the text value and error; non-text numeric DATE operands additionally take the checked day-count path.

castTemporal is CAST(x AS DATE) / CAST(x AS TIMESTAMP).

Both produce the box the corresponding COLUMN type produces: epoch DAYS for
DATE, epoch MILLISECONDS for TIMESTAMP, both int64 — the same values
ColRef.Eval hands out for a batch.TypeDate / batch.TypeTimestamp column, and
the same values batch.Vector.SetValue stores back into one. That identity is
the whole point of the fix (#340): until now the cast returned its argument
unchanged, so `CAST('1996-01-10' AS DATE) - 1` subtracted 1 from the number
ToFloat64 read out of the TEXT's leading digits and answered 1995.

The operand resolves through temporalOperand — the #332 helper — so a DATE
column arrives as a civilDate and a TIMESTAMP column as a time.Time, with
the unit their bare int64 box has lost recovered from the declared column
type; parseDateArg (#322) then reads whichever form arrived. Nothing here
parses a column value itself, so the cast cannot disagree with date_add,
date_diff or `date ± INTERVAL` about what a column means.

TEXT that resolves to no instant at all RAISES — 22007 for text that is not
a date, 22008 for a well-formed date naming no day — which is what
PostgreSQL answers and what #836 and #840 are. #340 chose NULL because the
expression layer had no per-row error channel; it has one (FatalEvalPanic,
#347), the numeric casts have used it since #367, and #836 is the issue
that noticed ADR-0012's residual text still said otherwise. Every non-text
box that fails to parse keeps its NULL — see raiseTemporalCastRefusal for
why that boundary is where PostgreSQL puts it.
