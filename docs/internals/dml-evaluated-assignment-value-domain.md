# Dml evaluated assignment value domain

Source: wadjet/dml.go — func assignEvaluatedValue(v any, col parquet.Column, srcFloat bool) (any, error) {, moved 2026-09-11 (#1026)

assignEvaluatedValue applies PostgreSQL's ASSIGNMENT CAST to a value the
expression engine produced, turning it into the box the target column's
writer stores.

The rule it exists to enforce, and the one whose absence was a silent wrong
answer: **an evaluated value is a VALUE, never a carrier.** ADR-0018 §4
defines a STORED integer box in a DECIMAL column as the already-unscaled
carrier — the int64 325 in a DECIMAL(9,2) column is 3.25 — and an evaluated
int64 is nothing of the sort, it is the number itself at scale 0. Handing
one straight to DecimalValueFromBox reopened exactly the trap the #647 arc
closed: `UPDATE t SET d = n` with n = 10 stored 0.10, `SET d = 1 + 1` stored
0.02, and both returned success (#678 review R1).

The whole matrix below was read off postgres:17-alpine rather than
remembered; each arm names the rows it implements.

	target INT64      5 -> 5    2.4 -> 2    2.5 -> 3    -2.5 -> -3
	                  d (numeric 1.50) -> 2    1 + 1.4 -> 2    ABS(0-3) -> 3
	                  3000000000 into INT32 -> 22003
	target NUMERIC    5 -> 5.00    2.567 -> 2.57    n (bigint 10) -> 10.00
	                  1 + 1 -> 2.00    d * 2 -> 3.00    n + 1 -> 11.00
	                  99999999.99 into (9,2) -> 22003
	target FLOAT8     n -> 10    d -> 1.5    1 + 1 -> 2
	target TEXT       5 -> '5'   n -> '10'   d -> '1.50'   UPPER(s) -> 'X'
