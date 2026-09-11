# Math function domain refusals

Source: internal/engine/expr/math_domain_refusal.go — func raiseLogarithmDomain(v float64) {, moved 2026-09-11 (#1026)

The math functions' DOMAIN refusals, which used to be a NULL or an infinity.

#840 named three (LN(0), SQRT(-1), and the temporal cast); the census the
arc ran over the whole function table found the class is nine sites in one
file, all with the same shape: the argument is outside the function's
domain, PostgreSQL raises, and this engine manufactured a value. A NULL
there is a wrong ANSWER — `WHERE LN(x) IS NULL` counted the rows where x is
zero as if x had been NULL — and an infinity is worse, because it
propagates arithmetically and nothing downstream can see where it came from.

PostgreSQL uses FOUR codes here and they are four different answers to a
client. Every one measured live on postgres:17.11:

	LN(0), LOG(0), LOG(b,0), LOG(0,x)   2201E  cannot take logarithm of zero
	LN(-1), LOG(-1), LOG(-1,x)          2201E  cannot take logarithm of a negative number
	LOG(1, x)                           22012  division by zero
	SQRT(-1)                            2201F  cannot take square root of a negative number
	POWER(0, -1)                        2201F  zero raised to a negative power is undefined
	POWER(-1, 0.5)                      2201F  a negative number raised to a non-integer power
	                                           yields a complex result
	POWER(2, 10000), EXP(1000)          22003  value out of range: overflow
	POWER(2, -10000), EXP(-1000)        22003  value out of range: underflow
	ASIN(2), ACOS(2)                    22003  input is out of range
	MOD(1, 0)                           22012  division by zero

NaN and the infinities are VALUES, not domain failures, and PostgreSQL
passes them through: SQRT('NaN') is NaN, LN('Infinity') is Infinity,
SQRT('Infinity') is Infinity, SQRT(-0.0) is -0. Each helper below therefore
tests the failing condition and nothing else, so a NaN never reaches a
refusal — the boundary `math_domain_test.go` attempts from the outside.
