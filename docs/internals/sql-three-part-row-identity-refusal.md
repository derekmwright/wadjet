# Sql three part row identity refusal

Source: internal/planner/sql/select_parser.go — return nil, sqlerr.New("0A000",, moved 2026-09-11 (#1026)

A TWO-PART container reference. Two spellings reach here and
the message must fit both, because the parser cannot tell
them apart: a relation-qualified container `(x.c_row).b`,
and a nested path `((c_row).rw).k` whose container is itself
a path. Calling either one "relation-qualified" was wrong
about the other, and neither is necessarily a container at
all — `(d.b).x` over a DECIMAL column has this shape too,
and PostgreSQL answers it 42809.

Both need a THREE-part identity, and this engine has a
two-part one: `plansql.ColRef` is {Table, Column} and every
resolver ADR-0022 binds together reads those two fields.
Measured on an attempt: the reference resolves to NULL at
every arm, with no join anywhere in the query, because the
container's declaration is keyed by its BARE name at each
declaration site and the qualifier is stripped before the
field is asked for.

A silent NULL is the one answer this must not give, so the
spelling is REFUSED while the identity is two-part.
ADR-0022 carries the mechanism and what closing it takes.
