# Batch decimal special value classification

Source: internal/engine/batch/decimal.go — type DecimalSpecialKind = parquet.DecimalSpecialKind, moved 2026-09-11 (#1026)

DecimalSpecialKind names one of the three values PostgreSQL's `numeric` has
and this carrier does not: NaN and, since PostgreSQL 14, ±Infinity. An
Int128 at a fixed scale has no bit pattern for any of them and the parquet
DECIMAL annotation has none either (ADR-0024 items 1 and 6).

The constants ARE their rank in PostgreSQL's numeric order, which is a total
order rather than IEEE754's: -Infinity below every finite value, Infinity
above every finite value, and NaN above Infinity and equal only to itself.
So an int comparison of two kinds orders them, and the sign of a non-finite
kind says which end of a column's range it sits past.

DecimalSpecialKind, DecimalSpecialText and the numeric-text grammar below
live in internal/storage/parquet and are read through from here.

This package IMPORTS that one (batch.Vector is built from parquet.Column),
so the lower package is the only place a SINGLE accept-set can sit — the
same reason ParseDateDays lives there. The file writer has to classify the
text it is about to store exactly as the comparison path classifies the text
it is about to compare, or 'NaN' is 22003 on one path and 22P02 on the other
and a client branching on the code cannot see past the difference
(ADR-0024 items 4 and 6, #647).
