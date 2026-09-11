# Boolean cast source domain

Source: internal/engine/expr/cast_bool.go — type castBoolRule int8, moved 2026-09-11 (#1026)

CAST(<x> AS BOOLEAN).

Before this existed, `Cast.Eval`'s switch had no boolean arm at all, so the
destination type was silently DROPPED and the operand came back
unconverted. THREE consumers then read that one unconverted value by three
different rules and answered three different things about the same
expression (#592):

  - the PROJECTION allocated a BOOL output vector (inferCastType maps
    BOOLEAN to batch.TypeBool) and wrote the raw box through
    Vector.SetValue, whose TypeBool arm coerces an int64/int32/float64 to
    `!= 0` — so `SELECT (c)::BOOLEAN` looked correct, by accident, and a
    STRING operand hit the #361 silent-write guard and failed the query;
  - NOT, AND, OR and IS NULL went through `evalBoolNull`, whose
    `toBoolVal` DOES read an integer's truthiness — so those were right;
  - the bare FILTER asked `v.(bool)` and took the failed assertion for
    FALSE, so `WHERE (c)::BOOLEAN` excluded EVERY ROW.

One expression with three readings is the two-path defect class, and it is
worse than an ordinary wrong answer here: TLP-WHERE's partition
(`p` UNION ALL `NOT p` UNION ALL `p IS NULL`) is exactly these three
readings, so the `p` arm contributed nothing and the partition permanently
undercounted by the rows the predicate is TRUE for.

What the cast now answers, per ADR-0012 item 1 and item 5's new entry:

	BOOL              itself
	INT32 / INT64     0 is FALSE, every other value TRUE
	STRING            PostgreSQL's own boolean input function (parseBoolText),
	                  and 22P02 for a string that names no boolean
	NULL              NULL, whatever the source type
	anything else     42846 cannot_coerce, the error PostgreSQL raises

The rule is selected from the operand's DECLARATION, never from the Go box
a row happens to produce, because the box cannot tell the cases apart:
ADR-0012 item 8's boxed-value rule. A DECIMAL column and a STRING column
both box as a Go string, and PostgreSQL answers them differently (42846
against its boolean input function) — reading the box would give
`DECIMAL(9,0)` holding 1 the answer TRUE where PostgreSQL refuses the cast
outright. DATE, IPv4 and MAC box as their raw integer encodings and would
have taken the integer arm for the same reason.
