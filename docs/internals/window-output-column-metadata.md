# Window output column metadata

Source: internal/engine/exec/window.go — windowOutputColumn, moved 2026-09-11 (#1026)

windowOutputColumn declares one window function's output column. When the
output IS the input column's own type — which is the whole point of
retypeValueColumns below — the input's PARAMETERISATION rides along too.

A bare TypeID is not a type for five of the twenty-two. DECIMAL without its
scale, VECTOR without its dimension, ARRAY/MAP without an element and ROW
without fields are all unusable, and unusable in SILENCE: Vector.SetValue's
ARRAY/MAP arm returns early on a nil Child, its ROW arm on nil Children and
its VECTOR arm on a zero dimension, over a vector whose null mask was
pre-set all-null — so `FIRST_VALUE(arr_col) OVER (...)` wrote nothing and
read back NULL on every row (#406). DECIMAL was the quiet one: SetValue
re-parses the formatted string GetValue produced against the OUTPUT
vector's scale, so a scale-4 column came back through a scale-0 vector as
3 where the row holds 3.0003 — a wrong number, not a missing one.

This is the aggregate's aggInputMeta rule (aggregate.go, #392) applied to
the window: the metadata travels with the type because it is what makes the
boxed value round-trip. The `col.Type != wc.OutputType` guard is the same
one, and for the same reason — metadata is copied only when it describes
the very type being declared.

SUM and AVG are the one family whose (p,s) is NOT the input's. They
accumulate rather than copy, so a sum genuinely exceeds its column's
precision and an average carries digits the column has no room for:
WindowDecimalAggMeta gives them DECIMAL(38,s) and DECIMAL(38,min(s+4,38)),
which is what the GROUPED SUM/AVG over the same column declare (#586,
ADR-0012 item 9). Declaring them at the input's own (p,s) instead would
hand the parquet writer a leaf too small for the value, and would make the
two spellings of one question disagree about their answer's type.

An INTEGER input reaches the same branch through IntegerAccOutputType: its
output type is not its input's either, and `SUM(int8) OVER ()` /
`AVG(int*) OVER ()` are DECIMAL(38,0) / DECIMAL(38,4) with no scale to read
off the input column at all (#987). That is why the accumulating family is
dispatched BEFORE the `col.Type != wc.OutputType` test the copying family
takes: for these two the types differ on purpose, and the old test skipped
the column outright, which would have declared DECIMAL(0,0).
