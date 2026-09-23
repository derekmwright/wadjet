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
   RowDescription say the same thing. **The rule is what the wire declares
   today**: an ARRAY column is described as its element's array type with
   the element's typmod (`numeric(10,2)[]`, 1231/655366), which is what a
   result carrying rows declares. Where the wire itself is not yet
   self-consistent — a stored array read through a zero-row result declares
   text (25), and a container's element typmod is not carried on the wire
   (#1250, #1268, #1017) — the catalog does not paper over it: those
   issues move the wire, and the catalog follows by construction. A
   computed array (`ARRAY(subquery)`, `current_schemas()`) declares its
   element's array type or is refused; it never goes out as text in a Go
   rendering.
2a. **Masked is visible, denied is absent.** A column the identity may read
   under a MASK is part of the relation it can see: its definition (name,
   type, NOT NULL, ordinal) is listed in every catalog relation and its
   values arrive masked. A DENIED column or relation is absent from every
   catalog relation. The catalog describes what the identity can SELECT;
   hiding a masked column's definition would describe a relation the
   identity's own `SELECT *` contradicts (arc PC round 2, B5).
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
`pg_am` empty; no DOMAIN types in pg_type; a string literal cast to
`regclass` is read to its OID and printed as it. Round 2 made the rest
answer: `E'…'` strings, `x = ANY(array expression)`, set-returning functions
as whole SELECT items (`unnest`, `generate_subscripts`,
`information_schema._pg_expandarray`), constant expressions as
`generate_series` arguments, and `current_schemas()` as an array.

**Amended 2026-09-23 (round 3).**

- *One PostgreSQL major, on the wire and in the catalog.* The catalog is
  PostgreSQL 17's, so the server reports 17 everywhere it reports a version
  (startup `server_version`, SHOW, `current_setting`, `version()`). A tool
  chooses its catalog spellings by the advertised version: psql 17's `\l`
  against an advertised 15 asked for `daticulocale`, which 17 renamed. A
  future catalog revision moves the registry and the advertised version
  together.
- *pgJDBC `getPrimaryKeys` answers*: `(result.KEYS).x` is ADR-0022's
  qualified row field, read by `row_field` from the reference's value; and
  a predicate over a set-returning output stays above the set it filters
  (below it the output is still the array).
- *Values through the new surface*: an `ARRAY[…]` constructor read by a
  set-returning item has one common element type (`unnest(ARRAY[1,2.5])` is
  1, 2.5); `x op ANY/ALL(typed array)` pairs x with the ELEMENT type by
  PostgreSQL's rule (42883 where no operator exists); `E'…'` combines a
  surrogate pair and refuses a malformed escape with PostgreSQL's SQLSTATE.
- *A syntax error is PostgreSQL's sentence*, chosen where the parser assigns
  42601 from the token it stopped at, never the parser's stage labels.
- *A catalog scan reads no key while the catalog is unchanged.* Every user
  table's definition is cached per catalog GENERATION (NATS KV's bucket
  sequence; MemKV's write counter), and the identity-viewed snapshot and its
  rows per exact view within a generation. A DDL moves the generation; a
  policy change is a different view; the session-dependent relation
  (`pg_stat_ssl`) is built per scan. On a live file-backed server over 1,000
  tables every psql `\d` command runs within 3x PostgreSQL 17.11's time.
