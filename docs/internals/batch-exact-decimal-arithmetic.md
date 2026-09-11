# Batch exact decimal arithmetic

Source: internal/engine/batch/decimal_arith.go — type DecimalStatus uint8, moved 2026-09-11 (#1026)

Exact fixed-point arithmetic over the Int128 carrier.

The contract is docs/adr/0024: DECIMAL is a finite 128-bit fixed-point type,
a value with no exact carrier at its declared type is an error and never a
saturated, wrapped or float-narrowed answer, and scale reduction rounds HALF
AWAY FROM ZERO (PostgreSQL's numeric rounding).

Every function here answers the same two questions: the EXACT value the two
operands name at the requested output scale, and whether that value has an
Int128. A status other than DecimalOK is never "close enough" — it means
the caller must raise the SQLSTATE the status names rather than show anyone
the first return.

**DecimalOK means the value fits the CARRIER, not the declared (p,s).** The
two bounds are different: 9 + (10^38-1), whose result type by ADR-0024 item
3 is DECIMAL(38,0), is 10^38+8 — an Int128 the carrier holds happily and a
value DECIMAL(38,0) cannot declare. A wiring site that must honour item 4
("a value with no exact carrier AT ITS DECLARED TYPE is a 22003 error")
wants the DecimalAddAt / SubAt / MulAt / DivAt / ModAt wrappers below, which
fold DecimalFitsPrecision in; the bare ops leave that bound to the caller
because some callers (an intermediate, an unconstrained result) have none.

Shape of each op: an Int128 fast path (no allocation, no math/big), and an
exact big.Int fallback taken only when an INTERMEDIATE overflows the carrier.
The fallback is not a second opinion — it is the same rule computed at a
width the intermediate needs, so `a*b` at a large product scale still answers
when the result at outScale fits. Both paths round once, at outScale.
