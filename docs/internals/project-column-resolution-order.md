# Project column resolution order

Source: internal/planner/logical/filter_project_pushdown.go — projRefs.resolve, moved 2026-09-11 (#1026)

resolve returns the replacement for one column reference, or nil to leave
it alone. ok=false declines the whole rewrite.

The order is ADR-0022 §1's, which is expr.ResolveColumnRef's, which is what
actually resolves the name at RUN time: the spelling as written, then the
qualifier read as a ROW column THAT DECLARES the name as its field, and only
then the BARE column after dropping the qualifier. Resolving in a different
order describes a different column.

The two middle steps were the other way round until 2026-09-04, and the
strip is a fallback for a RELATION qualifier — it exists so `t.col`
resolves where the stream carries only `col` — so taking it first made
`rw.b` over `SELECT c_row AS rw, id AS b` mean `id`. PostgreSQL 17 rejects
the unparenthesised form outright (`missing FROM-clause entry for table
"rw"`, 42P01) and reads `(rw).b` as the FIELD, which is its only anchored
answer and now this engine's; answering the bare spelling at all is the
superset ADR-0012 records (#769).

declaresField is what keeps the field arm off an ordinary qualified
reference: a qualifier naming an output that is not a ROW container, or a
container that does not declare the field, falls through to the strip
exactly as before.
