# Pgwire legacy nested schema resolution

Source: internal/server/pgwire/paraminfer.go — func (c *pgConn) nestedColumnSchemas(sql string, metas []wadjet.ColumnMeta) *nestedFieldSchema {, moved 2026-09-11 (#1026)
Superseded: The legacy path now prefers exact result declarations from nestedSchemaFromMetas and can return ordered structure; catalog-name lookup is only the fallback.

nestedColumnSchemas resolves the declared structure of every ROW/ARRAY/MAP
output column for the LEGACY (non-coord) query path, by matching it, by
name, against a column of the same name in a catalog table the statement
references — columnParamOIDs' technique, applied to a different question:
not which wire OID a BOUND PARAMETER should decode as, but which field
order and element type an OUTPUT VALUE should render with. The coord path
has an exact answer instead (queryViaCoord reads it straight off the
query's own output schema, which also covers a computed expression); this
is the best this layer can do without that.

A column two tables carry under the same name but a DIFFERENT top-level
type is dropped, the same conflict rule columnParamOIDs applies — a wrong
confident structure would silently drop fields formatPgComposite cannot
find under it, which is worse than the order-agnostic fallback.

Skipped entirely (nil, no catalog round trip) when metas says no output
column is a nested type, which is the ordinary query.

Returns byName only (nestedFieldSchema.ordered left nil): entries here
come from whichever catalog table columns happen to share a name with
something in the SQL text, which has no positional relationship to the
output column list — unlike the coord path's nestedSchemaByName, there is
no positional fallback to offer.
