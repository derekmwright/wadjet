# Shapegen nested schema coverage boundary

Source: internal/oracle/shapegen/typematrix.go — return &Schema{Tables: []Table{flat}}, moved 2026-09-11 (#1026)
Superseded: MAP SetValue accepts map[string]any through arrayElements/mapEntryRows and process-killer regressions have been fixed; the nested table remains excluded from this generator, but the stated current crash is stale.

typematrix.Nested is deliberately ABSENT. It would contribute only an id
and a group key here — its four container columns are not generatable —
but the generator emits star projections, and `t.*` over that table
expands to its MAP column, which KILLS THE PROCESS: the columnar decoder
declines MAP, the scan falls back to the row reader, and SetValue rejects
the map[string]any it is handed, on the scan-worker goroutine where no
recover reaches it. A generator that takes the process down reports
nothing about the seeds after the one that did it.

Restoring the table (with its Edges to typemx on id and g, which is what
makes a cross-table join generatable here) is part of fixing that defect:
wadjet.TestTypeMatrixNoProcessKillers fails when its pins stop crashing,
and those pins say so.

ROW FIELD PATHS (#568) are absent for a second, independent reason, and
restoring the table alone does not bring them: this generator QUALIFIES
a column reference with its table alias whenever a table appears twice,
and often when it does not (Gen.name), while the parser accepts only a
TWO-part reference — `rw.f` parses, `t.rw.f` is "trailing input after
the end of the statement". A generated field path would therefore be
unparseable SQL on most draws, which reports nothing about the engine.
Generating them needs either three-part path support in the parser or a
per-column "never qualify" flag honoured by Gen.name and nameOf; until
then the field-path shapes are covered by the fixed corpus
(typematrix.Corpus's rowfield_* entries) and by
wadjet.TestRowFieldPathCarriesTheFieldsDeclaredType.
