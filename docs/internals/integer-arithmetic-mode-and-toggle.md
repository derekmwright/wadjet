# Integer arithmetic mode and toggle

Source: internal/engine/expr/binop_numeric.go — var intArithToggle = optswitch.Register("int-arith", "WADJET_INT_ARITH",, moved 2026-09-11 (#1026)
Superseded: The integer-only/otherwise-float description predates DECIMAL mode and the expression-type cases now recognized by operandIsInt.

Integer-preserving arithmetic (+, -, *, %) for column operands.

compileBinOp could only choose the int64 path when BOTH operands were
compile-time int-native, and column types are unknown at compile time —
so `ClientIP - 1` (any column arithmetic) fell to the float64 path and
produced float64 values. That is both a semantics wart (SQL integer
arithmetic yields integers; DuckDB agrees) and a performance tax:
float-typed results force GROUP BY keys off the typed-int aggregation
paths (ClickBench Q36's four ClientIP-derived keys profiled as
Float64bits boxing inside generic-SoA key serialization).

BinOpNumeric resolves its mode ONCE against the first batch, using the
operands' actual column types: all-plain-integer (Int64/Int32) columns
and integer literals → int64 arithmetic; anything else → exactly the
old float64 behavior. Int64 overflow is a query ERROR — PostgreSQL's
`bigint out of range`, 22003 (#637) — never the wrapped number Go's
operators answer: a wrapped total is a different number wearing the right
type, and nothing downstream can see that it is wrong.

Division over integer operands truncates toward zero — PostgreSQL
semantics (#369, ADR-0012; the original float-`/` pin followed DuckDB,
which ADR-0012 overturns). Unlike the +,-,*,% typing, that truncation is
SEMANTICS and must not ride this kill switch: with the switch off the
operands evaluate through the float delegate and the quotient is
truncated there (divTrunc), so both settings answer 3 for 7/2.
