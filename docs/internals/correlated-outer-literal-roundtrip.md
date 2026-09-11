# Correlated outer literal roundtrip

Source: internal/engine/expr/outer_literal.go — func outerLiteral(v *batch.Vector, row int) (plansql.Node, error) {, moved 2026-09-11 (#1026)

The re-run's outer values are rendered TYPED.

A correlated subquery this engine cannot express as a join is re-executed
per outer row, and the outer row's values reach the re-run as LITERAL TEXT
substituted into the subquery's WHERE. What that text MEANS is decided by
the literal's own spelling, so a value whose Go box has lost its wadjet
type is re-typed by whatever the box happens to look like:
`batch.Vector.GetValue` hands a DECIMAL back as its rendered TEXT
(vector.go, `case TypeDecimal`), and the old renderer's `default:` arm
wrapped anything it did not recognize in quotes. `a.w_d2 = b.k` against a
BIGINT inner therefore became `'2.00' = b.k` and raised 22P02 — a query
PostgreSQL answers with 3 rows (#679). DATE, TIMESTAMP, the six
network-native types, UUID and BYTES reached the same arm.

Two rules, and the second is the one that keeps this honest:

 1. Every value is rendered as a literal THIS engine's own parser reads
    back as the SAME value at the SAME type — a CAST where the bare
    spelling would re-type it (DECIMAL, DATE, TIMESTAMP, REAL), a bare
    numeric where the type is already the literal's, a quoted string where
    the type resolves a string operand (the network types, UUID).
    `TestOuterLiteralRoundTripsEveryType` compares each rendering against
    the value read straight out of the column.
 2. A type with NO literal spelling in this dialect is a REFUSAL, not a
    guess. ARRAY, ROW, MAP and VECTOR have no literal at all, and a BYTES
    value that is not valid UTF-8 (or holds a NUL) has none either: the
    only bytea spelling the parser accepts is a quoted string, and the
    bytes that do not survive that round trip would come back as different
    bytes. Rendering them anyway is exactly the trade — a plausible wrong
    answer for a loud one — that protocol item 8 forbids.

A NULL is `null` for every type: it is the value the outer row holds, and
every comparison over it is UNKNOWN, which is what PostgreSQL answers.
