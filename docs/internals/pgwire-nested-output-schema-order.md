# Pgwire nested output schema order

Source: internal/server/pgwire/coord_query.go — type nestedFieldSchema struct {, moved 2026-09-11 (#1026)
Superseded: Legacy nestedColumnSchemas now also populates positional structure from result metas via nestedSchemaFromMetas (#965); only its catalog fallback lacks positional identity.

nestedFieldSchema is a query output column's declared ROW/ARRAY/MAP
structure, resolved by output column NAME with an optional positional
fallback — nestedColumnFor's two lookups.

ordered is set only by the coord path (nestedSchemaByName): OutputSchema()
and SQLResult.Columns are two views of the SAME query result and so agree
on position (coordinator.go's SQLResult.Schema doc: "the declared type of
each output column, in Columns order"), the exact invariant
coordColumnMetas already relies on for ITS positional fallback. A renamed
output column (the gather's renamer; an alias; a computed expression) can
lose its name from byName while keeping its slot in ordered, mirroring
coordColumnMetas' rule: "keeps its position but not its name" (#471
resurfacing as #464/#471 fold-in review item FIX 4 — a renamed ROW column
fell back to formatPgComposite's schema-less path, sorted-key order,
instead of its declared field order).

The legacy catalog lookup (nestedColumnSchemas, paraminfer.go) leaves
ordered nil: its entries come from whichever catalog table columns happen
to share a name with something in the SQL text, which has no positional
relationship to the output column list at all — a fallback there would
attach a random table column's schema to an unrelated output position.
