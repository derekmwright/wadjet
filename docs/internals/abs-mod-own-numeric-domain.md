# Abs mod own numeric domain

Source: internal/engine/expr/numeric_domain_fn.go — var numericDomainScalarFns = map[string]int{, moved 2026-09-11 (#1026)

ABS and MOD answer in their argument's OWN integer or real domain (#768).

The seven "same-domain" math functions already answer exactly over a
DECIMAL (decimal_scalar_fn.go, #668). Over an INTEGER or a REAL they all
declared FLOAT64 and computed through ToFloat64, and for five of them that
is RIGHT — measured on live PostgreSQL 17:

	              int4      int8      float4    float8    numeric
	ABS           integer   bigint    real      double    numeric
	MOD           integer   bigint    (none)    (none)    numeric
	CEIL/FLOOR/   double    double    double    double    numeric
	ROUND/TRUNC/
	SIGN
	SQRT/POWER/   double    double    double    double    numeric
	LN/EXP

So `FLOOR(bigint)` IS double precision there and must stay double here; a
blanket "type this family from its argument" pass would have introduced a
divergence where none existed. Only ABS and MOD preserve the domain, and
this file is only about them.

Two defects, and the second is a wrong VALUE rather than a wrong OID:

  - `ABS(real 0.1)` answered 0.10000000149011612 — the float32 widened to a
    double, whose extra digits are the ones a real never had — where
    PostgreSQL answers 0.1 under OID 700.
  - `MOD(-6, 3)` answered `-0`, math.Mod's signed zero, where integer
    remainder is 0.

Both are the same cause as the OID: the declaration was FIXED, so the kernel
had no domain to compute in. Declaring bigint over a ToFloat64 computation
would have put a right OID on a rounded number, which is why the kernel
moves with the declaration (protocol method 8).
