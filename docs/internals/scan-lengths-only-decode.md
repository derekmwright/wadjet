# Scan lengths only decode

Source: internal/engine/scan/lengths_decode.go — var lengthsOnlyToggle = optswitch.Register("lengths-only-decode", "WADJET_LENGTHS_ONLY_DECODE",, moved 2026-09-11 (#1026)
Superseded: length() over text has counted characters since #856 and is not eligible for byte-length-only decoding; octet_length/bit_length retain byte-count semantics.

Lengths-only column decode — the scan half of the offsets-shape
evaluation class.

When the planner can prove that every use of a byte-array column in the
whole plan is a SHAPE use — LENGTH()/octet_length()/bit_length(), IS
[NOT] NULL, a comparison against the empty string, COUNT(col) — the
column's bytes are never read. Decoding it in full still pays: the
dictionary gather, the arena growth, and the per-page BulkSet memcpy.
ClickBench Q28 (AVG(LENGTH(URL)) ... GROUP BY CounterID) materializes
~9 GB of URL bytes for lengths that are already sitting in the
dictionary offsets and the PLAIN length prefixes.

readColumnNativeLengths walks exactly the same page structure the full
decoder walks but writes only offsets: Offsets[i+1] = Offsets[i] + len_i,
with Data left empty and BytesColumn.ShapeOnly set. Nulls flow through
the definition levels exactly as they do in the full decode, so
LENGTH(NULL) stays NULL rather than becoming 0.

Correctness net: a shape-only column that reaches a VALUE consumer
panics at BytesColumn.Value with a precise diagnosis instead of
returning a wrong answer. The planner analysis
(internal/planner/logical/shape_only_columns.go) is conservative — any
use it cannot classify, and any plan shape it does not fully understand,
leaves the column on the full decode.
