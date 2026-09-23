# ADR-0044: The system catalog is relations the engine scans

Status: Accepted (2026-09-23, #1251, arc PC)

Related: ADR-0039 (a table function in FROM is a relation — the path the
catalog relations travel), ADR-0034 (metadata follows the effective table
decision), ADR-0012 (PostgreSQL decides semantics; the divergences below).

## Context

Every client reads `pg_catalog` and `information_schema` before it sends a
query of its own: psql's `\d` family, pgJDBC's `DatabaseMetaData` (DataGrip,
DBeaver, Metabase), SQLAlchemy's inspector (Superset). Through v0.24 the
pgwire layer answered them with a canned responder: it recognised a
statement's subject and a handful of predicate spellings in its TEXT and
answered a precomputed row set, with "no rows under the SELECT list's labels"
for everything else. Each predicate or shape it had not been taught was
ignored — `information_schema.columns WHERE table_name = 'nosuch'` listed
every column, `COUNT(*)` answered the raw rows (#1251) — and each fix taught
it one more spelling.

## Decision

1. **A catalog relation is a FROM item the engine scans.** Package
   `syscatalog` holds PostgreSQL 17.11's own relation list — every column of
   every `pg_catalog` table and view and `information_schema` view, read off
   a live catalog (`pg17_relations.tsv`) — and the parser resolves
   `pg_catalog.x`, `information_schema.x` and an unqualified pg_catalog name
   (PostgreSQL searches pg_catalog first; a WITH query of the name still
   wins) to a table-function FROM item named by the relation. WHERE, JOIN,
   aggregates, ORDER BY and LIMIT are the ordinary engine's.
2. **Rows are materialized when the scan starts, through the identity's
   view** (`syscatalog.Access`, installed by `auth.AuthorizeTableFunctions`
   on every SELECT door). A relation or column the identity may not read is
   absent from every catalog relation; a scan with no access decision on its
   context refuses 42501.
3. **Columns are typed by the engine type whose values and comparisons a
   client relies on** (an OID is int8, `name` is text), and a user column's
   type is described by the rule the wire declares it with
   (`syscatalog.TypeOf` restates `pgwire.pgColumnOID`). The catalog and the
   RowDescription say the same thing.
4. **What the catalog lists:** pg_catalog, public and information_schema;
   the user tables and the system relations themselves (pg_class,
   pg_attribute, information_schema.tables/columns); the types the wire
   declares and their arrays; one database and one role (the identity,
   not a superuser); the NOT NULL constraints. Relations this server has no
   objects for are empty. The superuser-only relations refuse 42501.
5. **Catalog functions are bound to the same snapshot** (`regclass` both
   ways, `pg_get_userbyid`, `to_regclass`, `pg_get_serial_sequence`,
   `pg_relation_is_publishable`) at compile time; a path with no catalog view
   (a DAG worker) refuses them by name. On the DAG a table-function scan runs
   on the coordinator-local pipeline.

## Consequences

The recorded divergences are listed on the differences page and pinned in
`pgwire.TestArcPCToolCatalogQueriesAnswerAsPostgreSQL`: one database and one
role (PostgreSQL also lists templates and its superuser); `relam` is 0 and
`pg_am` empty; no DOMAIN types in pg_type; `current_schemas()` answers text;
a string literal cast to `regclass` is read to its OID and printed as it;
`E'…'` strings, set-returning functions in a SELECT list and
`information_schema._pg_expandarray` are not implemented.
