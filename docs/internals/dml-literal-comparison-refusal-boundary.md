# Dml literal comparison refusal boundary

Source: wadjet/dml.go — func refuseDMLLiteralPairs(node plansql.Node, schema []parquet.Column) error {, moved 2026-09-11 (#1026)

refuseDMLLiteralPairs raises, BEFORE any row is read, for a comparison whose
operand pair PostgreSQL's overload resolution refuses.

A QUALIFYING PREDICATE IS NOT A PROJECTION. ADR-0012 item 12 records a
deliberate divergence: PostgreSQL refuses `text = numeric` outright (42883),
and wadjet — having one generic comparison operator and no overload set to
fail resolution against — gives the pair the column's own rule, comparing
the STRING column's bytes against the literal's source text. Every answer
that produces is PostgreSQL's answer to the QUOTED spelling of the same
predicate, which is a defensible concession when the consequence is a
COUNT. It is not one when the consequence is a WRITE:

	DELETE FROM pr WHERE name > 5     PG: 42883.  wadjet: DELETE 3, table EMPTIED

`"a" > "5"` is true for every row because 0x61 > 0x35, so wadjet answered
PostgreSQL's answer to a DIFFERENT predicate and destroyed a three-row
table (#721). Nobody wrote that consequence down because no fixture
attempted it: the ADR entry was reasoned entirely about the read path.

So the divergence stays where its reasoning holds — a SELECT still gets the
byte rule — and a DML statement's qualifying predicate refuses the pair.
The asymmetry is recorded in ADR-0012 item 12 rather than left implicit.

Three pairs, and the bound is deliberate:

  - a STRING or BYTES column against an unquoted NUMBER literal (42883).
    This is the shape above, and the one that loses rows.
  - any non-BOOL column against a BOOLEAN literal (42883). `id = true` is
    PostgreSQL's `bigint = boolean`; wadjet answered `DELETE 0` on the DML
    door and 22P02 on the SELECT door — two doors disagreeing about one
    predicate.
  - a numeric column against a QUOTED literal naming no value of it. The
    runtime already refuses this (22P02, #536/#646), but the refusal needs
    a ROW to reach it, so `DELETE FROM empty WHERE id = 'abc'` answered
    `DELETE 0` where PostgreSQL raises. The test is
    expr.RefuseNumericLiteral — the SAME predicate the runtime uses, so the
    two cannot disagree about which strings name a value.

Temporal and network columns against a number are deliberately NOT refused
here: those pairs have their own accept-sets (parquet.ParseTimestampMillis
and friends), wadjet's network literal parsers are STRICTER than
PostgreSQL's input grammar, and refusing on them would reject input
PostgreSQL accepts — the one thing ADR-0012 item 1 forbids. The boundary
carries fixtures either way.
