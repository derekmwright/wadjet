# Sql parenthesized row field path

Source: internal/planner/sql/select_parser.go — pn, ok := expr.(*ParenNode), moved 2026-09-11 (#1026)

PostgreSQL's ROW field path: `(container).field`.

The PARENTHESES are the whole point. `c_row.b` is spelled like
`table.column` and PostgreSQL reads it that way — it is a
missing-FROM-clause error there — so the parenthesised form is
the only spelling PostgreSQL reads as a field, and it is the
only one that can carry a RELATION qualifier beside the
container: `(x.c_row).b` says arm `x`, container `c_row`,
field `b`, which the bare two-part form cannot say at all.
That matters because a container two arms both publish is
refused (42702, physical.colScope.resolveRef) and this is the
escape hatch PostgreSQL offers for it.

Only a PARENTHESISED expression takes a dot here. `a.b.c` is
still a syntax error, which is ADR-0022's position and matches
PostgreSQL's own reading of the unparenthesised three-part form
(it takes the parts as catalog.schema.column and refuses).

The container itself must be a BARE name. `(x.c_row).b` parses
and is REFUSED (0A000) rather than answered, because a
three-part identity is not something this engine's ColRef can
carry — see below.
