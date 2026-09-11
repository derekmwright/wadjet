# Boxed pair order table

Source: internal/engine/expr/boxed_pair.go — func (p *boxedPair) order(b *batch.RecordBatch, lv, rv any) (c int, ok, unknown bool) {, moved 2026-09-11 (#1026)

order compares two boxed values under the rule their DECLARATIONS select,
returning -1, 0 or +1, or ok=false when no such rule applies and the caller
must fall through to compare().

The rules, each PostgreSQL's:

  - DECIMAL against DECIMAL: the two exact decimals, at whatever scales the
    columns declare — "1.50" and "1.5000" are one number (#477, #506).
  - DECIMAL against a numeric LITERAL: the literal's exact source text
    against the column's, because PostgreSQL types an unsuffixed decimal
    literal as `numeric` and compares it at full precision (#452, #465).
  - DECIMAL against a non-DECIMAL number: exact against an integer, float64
    against a float, which is what `numeric <op> double precision` does —
    it casts the numeric (#476).
  - TEXT against a numeric LITERAL: the literal's source TEXT against the
    column's value, bytewise. PostgreSQL refuses this pair outright —
    verified live, `WHERE s = 1.5` over a text column is 42883 "operator
    does not exist: text = numeric" — but that is an OVERLOAD RESOLUTION
    failure, and wadjet has one generic comparison operator with no
    overload set to fail resolution against, exactly the situation
    ADR-0012 item 5 already records for unary minus over a quoted string.
    So the pair gets the STRING column's own rule instead of a reading of
    its digits, which is also what the vectorized kernel answers (#504).
  - A NUMBER against a QUOTED literal: the NUMBER's rule, because
    PostgreSQL types an unknown-typed literal from the operand it meets.
    `k > '2'` over a BIGINT column is `k > 2` there, not a text comparison
    and not a comparison against zero.
