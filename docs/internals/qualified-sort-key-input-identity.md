# Qualified sort key input identity

Source: internal/planner/logical/order_by_keys.go — sortKeyCarried, moved 2026-09-11 (#1026)

sortKeyCarried reports whether the Sort's input already emits key.

With a SELECT list, the Project below the Sort narrows the schema to exactly
its outputs, so the select-list names are the whole of what a sort key can
resolve against. With `SELECT *` the Sort reads the relation itself, whose
column set is not known here — a bare column reference is taken at its word
(that is the pre-existing contract, and the catalog rejects a name that does
not exist), and anything computed is materialized.

items are the SELECT-list columns outputs was derived from, index for index,
and they are needed for one rule: **a QUALIFIED ORDER BY term names an INPUT
column, never a SELECT-list alias** (#488). PostgreSQL only consults output
names for a bare identifier — that is what makes `SELECT s_acctbal AS
s_suppkey … ORDER BY s_suppkey` order by ACCTBAL — and `x.col` is resolved in
the FROM scope like any other expression. The two rules meet here because
namesSameColumn deliberately tolerates one side carrying a qualifier the
other omits, so `s.s_suppkey` matched the output named `s_suppkey` and the
sort read the alias: verified live on postgres:17-alpine, which orders that
query by the real key while this engine ordered it by the shadowing alias, on
both arms and silently.

The match therefore has to prove the output IS that input column: the select
item is a bare column reference of the same name, qualified by the same
relation or by none. Anything else falls through and the term is
materialized as a hidden key over the input, where it belongs.
