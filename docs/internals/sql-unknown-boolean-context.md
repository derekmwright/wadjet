# Sql unknown boolean context

Source: internal/planner/sql/boolean_context.go — func CoerceBooleanLiterals(info *SelectInfo) error {, moved 2026-09-11 (#1026)

CoerceBooleanLiterals resolves an UNKNOWN-typed literal used as a truth
value, which is the one shape a boolean context accepts that is not already
a boolean (#599).

PostgreSQL types a bare quoted literal from the context it meets, so a
boolean context runs it through the boolean input function: measured live on
postgres:17-alpine, `SELECT 1 WHERE 'true'`, `'yes'`, `'t'` and `' 1 '` all
SUCCEED, `WHERE NULL` succeeds and returns nothing, and `WHERE 'abc'` is
22P02 `invalid input syntax for type boolean: "abc"` — NOT the 42804 a
typed non-boolean gets.

Wadjet answered 0 rows for all four, because nothing typed the literal at
all: `expr.FilterPredicate`'s generic arm takes a failed `v.(bool)`
assertion for FALSE. So this runs at parse time, where the literal can
simply BECOME the boolean it names.

The grammar is PostgreSQL's `parse_bool_with_len`, the same one
`CAST(<string> AS BOOLEAN)` already follows (ADR-0012): case-insensitive, C
whitespace trimmed, any non-empty PREFIX of "true"/"false"/"yes"/"no", plus
"on"/"off" and the single characters "1" and "0". `'tr'` and `'fals'` ARE
values; `'o'` alone is not, because it cannot choose between "on" and "off".
