# Declared literal validation

Source: internal/planner/physical/validate_literal.go — checkLiteralTypes, moved 2026-09-11 (#1026)

checkLiteralTypes refuses a constant that names no value of the type its
context demands, from the column's DECLARATION, before any row exists.

The column's declared parquet.TypeID reaches here through colScope
(validate.go), and refuseLiteralForType holds the rule per type that has a
"this string names no value of me" test: the whole numeric family — the
integer types, the FLOAT types and DECIMAL — each read with its OWN
PostgreSQL input grammar. PostgreSQL resolves an unknown-typed literal's
type from the column it meets and refuses at parse/bind time — `SELECT
count(*) FROM t WHERE d = 'abc'` is 22P02 there whether or not the table
holds a row.

#579 widened colScope from a bare `isDecimal bool` to the full TypeID so the
network types (CIDR/IPv4/IPv6/MAC/UUID) can join this rule, but wiring their
refusal waits on #627 — wadjet's network parsers are stricter than
PostgreSQL's grammar, so refusing on them here would reject PG-valid input
(see refuseLiteralForType).

Wadjet already raised the same SQLSTATE, but from inside the COMPARISON, so
it depended on a row reaching it and on which operand won (#517):

  - PER ROW. An empty table, or a conjunct no row survives to — `k > 100000
    AND d IS DISTINCT FROM 'abc'` — answered zero rows instead of erroring.
  - PAIRWISE, so the DATA decided. GREATEST/LEAST compare (best-so-far,
    candidate) pairs and a pair refuses only when a DECIMAL column is on one
    side and the bad literal on the other, so the SAME three arguments
    refused under GREATEST and answered under LEAST:
    `GREATEST(k, 'abc', d_2)` raised and `LEAST(k, 'abc', d_2)` returned a
    row. A refusal that depends on which operand won a comparison is not a
    type rule at all.

Both close here, because a declared type is not a property of a row. The
runtime refusals stay: they cover the shapes this binder cannot see — an
expression it does not parse, an open scope, a column whose source is a
derived table or a CTE — and, being the same predicate
(`expr.RefuseNumericLiteral` over `kernel.QuotedLitStatus`), they cannot
disagree with this one about which strings name a value of which type.

It is as conservative as the rest of the binder (validate.go's contract): it
refuses only when the column PROVABLY resolves to a declared type with a
rule, in a closed scope. A false positive breaks a working query; a false
negative merely leaves the refusal where it already was.
