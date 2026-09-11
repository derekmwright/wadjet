# Parquet ddl column resolution

Source: internal/storage/parquet/schema.go — func DeclaredColumn(name, typeStr string, nullable bool) (Column, error) {, moved 2026-09-11 (#1026)

DeclaredColumn builds a Column from one DDL column declaration: the type
exactly as written, plus its nullability.

It is the ONE place a declaration becomes a Column, because a DECIMAL's
(p, s) lives in the type TEXT and nowhere else. Three copies of "ParseTypeID
and fill in the name" existed — the embedded API, the HTTP server and gRPC —
and only the first read the parameters, so `CREATE TABLE t (d DECIMAL(9,2))`
over HTTP or gRPC produced a Precision 0, Scale 0 column: 12.34 stored as
12, 9999999.999 stored as 10000000 with no error, and DECIMAL(50,2)
accepted (#647 review). Copies of a declaration parser drift toward the
laziest one; there is now one to drift from.
It resolves EVERY parameterized type, not only DECIMAL's (p, s). The first
version read the decimal parameters and nothing else, so `VECTOR(384)`
created a column with `Dimension: 0` — a table no INSERT could ever write,
failing at flush with an internal error and no SQLSTATE — and
`ARRAY(DECIMAL(9,2))`, `ROW(a INT64, d DECIMAL(9,2))` and
`MAP(STRING, DECIMAL(9,2))` lost their element, field and key/value
declarations entirely (#675). ResolveColumn already knew how to read all of
them and had no non-test caller; this is that caller.
The NAME is taken as given. It used to be lowercased here, which folded a
DELIMITED declaration too — `CREATE TABLE t ("WatchID" INT64)` stored
`watchid`, so the one spelling PostgreSQL guarantees would work was the one
that did not, and a DDL-created table could not hold a name a
parquet-registered one holds every day. Since #731 an UNQUOTED identifier
is already folded when it gets here (the lexer does it, once), so the only
declarations this changes are the delimited ones, which are exactly the
ones that asked to keep their bytes. Fold-uniqueness within the schema is
still enforced, by catalog.checkDistinctColumnNames.
