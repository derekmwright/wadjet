# Kernel float input range classification

Source: internal/engine/exec/kernel/numeric_literal.go — func FloatLitText(text string, bits int) (float64, NumConstStatus) {, moved 2026-09-11 (#1026)

FloatLitText reads PostgreSQL's FLOAT input grammar — float4in/float8in,
which are `strtod` plus PostgreSQL's own special-value spellings — and
classifies the failure the way PostgreSQL classifies it.

bits is 32 for `real` and 64 for `double precision`. The value comes back as
a float64 in BOTH cases: the parse itself is always done at double width
(Go's ParseFloat at bitSize 32 reports overflow but is SILENT about
underflow, answering a plain 0 for '1e-46'), and real's range is then
decided by Float32FitOf, whose boundary is real's smallest DENORMAL — the
same boundary PostgreSQL draws, verified live: '1e-45'::real is a value,
'7e-46'::real is 22003, '3.4e38'::real is a value, '3.5e38'::real is 22003.

Three differences from Go's own ParseFloat, each of them PostgreSQL's:

  - UNDERSCORES are refused. Go accepts '1_000' as 1000; PostgreSQL's float
    input does not (22P02, verified live) even though its INTEGER and
    NUMERIC inputs do since 16. Accepting it would answer where PostgreSQL
    errors.
  - HEX floats are accepted WITHOUT a binary exponent. glibc's strtod reads
    '0x10' as 16 and PostgreSQL inherits that ('0x10'::real is 16,
    '0x1p3'::real is 8, '0x.8p1'::float8 is 1 — all verified live); Go
    requires the 'p'. The exponent is supplied when the text omits it.
  - UNDERFLOW to zero is a RANGE error, not a value. Go answers 0 with no
    error for '1e-400'; PostgreSQL raises 22003 ("1e-400" is out of range
    for type double precision). A denormal is NOT underflow on either side
    ('1e-320'::float8 is a value).

The special spellings come from FloatSpecialText, which is PostgreSQL's
float grammar for them and deliberately a second reader beside the DECIMAL
one: float8 accepts a SIGNED NaN and numeric does not (#534).
