# Parquet recursive record assembly

Source: internal/storage/parquet/nested_assembly.go — type nestedKind int, moved 2026-09-11 (#1026)

Record assembly for nested columns: ONE recursive descent over the FILE's
own schema tree, driving every leaf under a column from that leaf's own
definition and repetition levels.

What it replaces (#409): three hand-written assemblers — one per container
kind — that resolved their leaves by a FIXED-DEPTH path and gave up when
the path did not land on a leaf. assembleRowColumn looked up
{col, field} and skipped the field when the lookup missed, so a field that
was itself a container (always a GROUP, never a leaf) was dropped from the
struct. assembleMapColumn looked up {col, "key_value", value} and abandoned
the WHOLE column on a miss, so a MAP of ARRAY or of ROW read back absent.
assembleArrayColumn had a prefix fallback, which is worse than a miss: an
ARRAY of MAP resolved to the MAP's FIRST leaf and answered with the array
of KEYS. Even with the right leaf, assembleMapColumn read the value at the
KEY leaf's entry index, an alignment that only holds while the value is a
single leaf with one entry per map entry.

The depth assumption is the whole defect, so the replacement carries no
depth at all. Every shape — MAP inside ROW, ARRAY of MAP, MAP of ARRAY,
MAP of MAP, ARRAY of ARRAY, to any depth — is the same three cases applied
recursively.

The plan is built from the SchemaNode tree rather than from the catalog's
Column, because the file is what the levels describe: nodeToColumn
(file_reader.go) derives the reported column TYPE from exactly the same
three patterns, so the assembled value's shape and the declared type
cannot drift apart. Level arithmetic is not recomputed here either —
BuildSchemaTree's computeLevels already stamped MaxDefLevel/MaxRepLevel on
every node from the footer's repetition types, and those are the numbers
the page levels are written against.
