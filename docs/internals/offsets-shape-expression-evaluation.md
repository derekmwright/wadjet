# Offsets shape expression evaluation

Source: internal/engine/expr/shape_funcs.go — byteArrayShaped, moved 2026-09-11 (#1026)
Superseded: length() over text has counted CHARACTERS since #856; only octet_length/bit_length take the byte-count reading.

Offsets-shape evaluation.

A consumer of a variable-length column's SHAPE rather than its CONTENTS
can answer off the offsets array with zero byte access: LENGTH() /
octet_length() / bit_length(), IS NULL / IS NOT NULL, and = '' / <> ''
comparisons. The generic paths all funnel through ColRef.Eval, which
for a TypeString column calls Vector.GetString -> BytesColumn.StringValue
-> string(bc.Value(i)) — a full copy of every value, discarded one line
later. ClickBench Q28 (AVG(LENGTH(URL)) ... GROUP BY CounterID) copies
~9 GB of URL bytes to compute lengths that are offsets[i+1]-offsets[i].

SEMANTICS NOTE (verified, deliberately preserved): our length() is a
BYTE count, not PostgreSQL's character count. fnLength is
float64(len(toString(v))) and vecLength was already an offsets
subtraction. octet_length is therefore an exact alias, bit_length is
8x, and char_length/character_length keep the rune-counting
implementation (fnCharLength) — those must decode bytes and get no
offsets fast path.

Every node here carries a generic fallback and takes it whenever the
resolved column is not a flat byte-array column, so semantics stay
bit-identical with the path it replaces.
