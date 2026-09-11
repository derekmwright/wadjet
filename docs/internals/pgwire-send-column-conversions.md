# Pgwire send column conversions

Source: internal/server/pgwire/server.go — func sendColumnTypes(columns []string, metas []wadjet.ColumnMeta) []parquet.TypeID {, moved 2026-09-11 (#1026)
Superseded: timestampColumns has become sendColumnTypes and now also covers DATE, DECIMAL and UUID, not a timestamp-only mask.

timestampColumns returns a mask over columns marking those the
RowDescription declared as TIMESTAMP (OID 1114).

The engine boxes a timestamp as epoch milliseconds — the right thing for
every compute path that shares that boxing — but a client reads the value
according to the OID we already told it, so the send path has to convert.
Without this the wire carried "826727136000" under a declared `timestamp`,
which psql prints back verbatim and a typed client (pgJDBC, DataGrip,
SQLAlchemy) fails to parse (#321).

Returns nil when no column is a timestamp, so the common query pays one
nil check and nothing else. Metas normally arrive in column order; the
name lookup is the fallback for callers that reorder or rename.
The same reasoning covers DATE, which the engine boxes as a rendered
string: under a binary format code those text bytes were written beneath
the declared OID 1082, whose value is a 4-byte day count, so the client
decoded whatever the string happened to contain.

Returns nil when no column needs conversion, so the common query pays one
nil check and nothing else. Metas normally arrive in column order; the
name lookup is the fallback for callers that reorder or rename.
