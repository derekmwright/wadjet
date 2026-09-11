# Pgcompat scalar shim boundary

Source: internal/engine/expr/pgcompat.go — func init() {, moved 2026-09-11 (#1026)
Superseded: The engine now has roles and per-object authorization; these scalar shims do not consult that state and are not authorization decisions.

PostgreSQL-compatibility scalar functions, plus the handful of common
spellings the registry was missing.

These exist because of what the #341 inventory found. Erroring on an
unresolvable function name is the fix, but the silence it replaces was
load-bearing on the pgwire introspection path: a probe that fired 75 real
DataGrip / psql / SQLAlchemy / Superset statements at the server found 29
distinct function names that reached the expression compiler and answered
NULL. Most pg_catalog functions never get that far — the synthetic-answer
path in internal/server/pgwire intercepts them on statement text, which is
why format_type, pg_get_expr, pg_table_is_visible, pg_get_userbyid,
pg_get_viewdef, pg_get_constraintdef, pg_get_indexdef, pg_size_pretty and
pg_database_size are absent below. The ones here are the residue: the
functions a client calls in a FROM-less SELECT, or over a real table, where
no intercept applies.

Each one answers what a single-node engine with no roles, no tablespaces and
no per-object grants can honestly answer. Where PostgreSQL's answer depends
on state Wadjet does not keep, the shim returns the value that keeps a client
moving (privileges: granted; visibility: visible; comments: none) rather than
a fabricated one.

Deliberately NOT shimmed, so they now error: pg_get_functiondef,
pg_get_partkeydef, pg_get_serial_sequence, pg_relation_size,
pg_total_relation_size, pg_tablespace_location, to_regclass, pg_sleep. Each
needs catalog or storage state a scalar function cannot reach from here, and
a plausible-looking wrong answer from one of them is worse than a named
error — to_regclass in particular returns NULL for "no such table", so a
shim that always returned NULL would report every table as missing.
