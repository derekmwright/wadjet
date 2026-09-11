# Set operation numeric literal typing

Source: internal/planner/physical/set_op_arm_decls.go — litDeclType, moved 2026-09-11 (#1026)

```go
// litDeclType is a numeric LITERAL's own type, as PostgreSQL reads its
// SPELLING, with the plain decimal text that spelling expands to.
//
// PostgreSQL's rule, verified live against 17.11 with pg_typeof:
//
//	1.23456  1.  0.0        -> numeric   (a decimal point)
//	1e2  1.5e1  1.5e-2      -> numeric   (an exponent, WITH or without a point)
//	1  1234567890           -> integer
//	12345678901             -> bigint
//	123456789012345678901   -> numeric   (too wide for bigint)
//
// So a literal is numeric when it carries a decimal point OR an exponent, and
// an INTEGER literal is numeric only when no integer type holds it. The
// integer forms answer false here and stay on the ladder's integer rung, where
// an integer arm contributes its whole range's digits.
//
// The (p,s) is the digits the literal EXPANDS to — PostgreSQL's numeric
// constant carries typmod −1 and an exact value, and a finite carrier needs a
// declaration wide enough to hold that value without moving it. `1e2` is
// numeric(3,0), `1.5e-2` is numeric(3,3), `0.5` is numeric(1,1) (a leading
// zero holds no place), and trailing zeros count because they are digits the
// query wrote and a set operation must not drop a scale it stated.
//
// It is deliberately NOT wired into nodeDeclaredType's Lit case, which still
// answers FLOAT64 for a fractional literal everywhere else: the declared type
// of a literal in an ARITHMETIC expression is being decided alongside
// ADR-0024 item 3's decimal arithmetic. A set-operation ARM is the one site
// where the literal's own type is the whole answer — the arm produces the
// literal and nothing else.
```
