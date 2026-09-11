# Batch decimal literal type

Source: internal/engine/batch/decimal_lit_type.go — func DecimalTextType(s string) (DecimalType, bool) {, moved 2026-09-11 (#1026)

DecimalTextType reports the DECIMAL type a numeric LITERAL names — ADR-0024
item 3's "a numeric literal's (p,s) is its spelling".

PostgreSQL types an unadorned `12.75` as numeric, and the (p,s) a finite
carrier needs for it is read off the digits the user actually WROTE: 12.75
is DECIMAL(4,2), 2 is DECIMAL(1,0), 0.5 is DECIMAL(1,1). That is what makes
`d * 2` a multiply by DECIMAL(1,0) — result scale 2 — rather than by the
INT32 range's DECIMAL(10,0), which would declare eight integer digits nobody
wrote. An integer COLUMN is the other rule and keeps its whole range
(DecimalTypeOf), because a column's values are not one spelling.

TRAILING ZEROS ARE KEPT, and that is the whole reason this reads the text
itself rather than going through decimalParts: `100.0` is DECIMAL(4,1), not
(3,0). PostgreSQL's numeric carries a per-value dscale that the zeros are
part of — `12.75 * 100.0` renders 1275.000, three fraction digits, because
the literal contributed one — and folding them away made the product's
declared scale 2 where PostgreSQL's is 3. They cost nothing and they are
what the user wrote.

The exponent form is normalized first: `1.5e3` is 1500, DECIMAL(4,0), and
`1.5e-3` is 0.0015, DECIMAL(4,4) — the same value written two ways gets the
same type, which is what keeps `d + 1.5e3` and `d + 1500` from declaring
different columns.

ok=false for text that names no number, and for one whose scale or digit
count is past what a DECIMAL can declare — a literal with 40 fraction digits
has no fixed-point type here, and the caller must fall back rather than
truncate it.
