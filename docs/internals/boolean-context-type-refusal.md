# Boolean context type refusal

Source: internal/planner/physical/validate_boolean.go — checkBooleanContext, moved 2026-09-11 (#1026)

checkBooleanContext refuses a non-boolean expression where SQL requires a
boolean, before any row exists — PostgreSQL's 42804 (#599).

PostgreSQL, measured live on postgres:17-alpine over `cb(id bigint,
c bigint, s varchar, f double precision, n numeric, b boolean)`:

	WHERE c            42804  argument of WHERE must be type boolean, not type bigint
	WHERE NOT c        42804  argument of NOT must be type boolean, not type bigint
	WHERE 1            42804  ... not type integer
	WHERE s            42804  ... not type character varying   (s is varchar THERE;
	                          wadjet's STRING declares OID 25, and PostgreSQL says
	                          "text" for a text column, which is what pgTypeName
	                          answers)
	WHERE c AND b      42804  argument of AND must be type boolean, not type bigint
	WHERE b OR c       42804  argument of OR ...
	HAVING count(*)    42804  argument of HAVING must be type boolean, not type bigint
	JOIN ... ON a.c    42804  argument of JOIN/ON ...
	CASE WHEN 1        42804  argument of CASE/WHEN ...

Wadjet had no type check at all here, and the two evaluators that collapse a
value to a truth value did not agree: `expr.FilterPredicate`'s generic arm
takes a failed `v.(bool)` assertion for FALSE, while `expr.toBoolVal` reads
C truthiness. So `WHERE c` returned 0 rows and `WHERE NOT c` returned the
row holding 0 — not complements of each other under any reading, which is
the two-path shape #592 was with the cast removed.

It is as conservative as the rest of this binder (validate.go's contract):
it refuses only where the expression PROVABLY has a non-boolean type, which
is a column whose DECLARATION the scope carries, a numeric literal, an
arithmetic operator, or an aggregate whose result type is fixed. A function
call, a CAST, a subquery, a container element and anything over a derived
table or CTE column are left alone — a false positive breaks a working
query, a false negative merely leaves the shape where it already was.

An UNKNOWN-typed literal is NOT a type error there: PostgreSQL coerces it
through the boolean input function, so `WHERE 'true'` succeeds, `WHERE NULL`
succeeds, and `WHERE 'abc'` is 22P02 rather than 42804. That half is
handled in the parser (plansql.CoerceBooleanLiterals), where the literal can
be turned into the boolean it names.
