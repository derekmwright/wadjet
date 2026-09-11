# Decimal column literal domain

Source: internal/engine/expr/decimal_literal.go — decimalLitCmp, moved 2026-09-11 (#1026)

decimalLitCmp binds a bare column reference to the numeric literals it is
compared against, so that when the column turns out to be a DECIMAL the
comparison is answered in the column's own domain — the unscaled integer at
the column's scale — instead of through float64 on both sides.

Two separate losses live on that float64 path, and this closes both (#452):

  - the LITERAL: a float64 holds ~15-16 significant decimal digits, so
    `= 493827160549382.7160549350` became `= 493827160549382.6875` and
    matched nothing, while `>` gained the row it should have excluded.
  - the COLUMN: ColRef.Eval boxes a DECIMAL as its rendered text, and
    compare() has no numeric reading of text against a float64, so it fell
    through to a LEXICOGRAPHIC comparison — "1339815.97" against
    "1.33981597e+06" — which is not the same order and not the same
    equality.

The binding is decided at COMPILE time (the operand shapes) and applied per
BATCH (the column's type and scale), because a column's type is not known
until a batch arrives. Anything that is not a materialized DECIMAL column
falls through to the generic path untouched, which is what keeps every
other type answering exactly as before.
