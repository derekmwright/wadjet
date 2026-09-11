# Parquet float to decimal rendering

Source: internal/storage/parquet/decimal_value.go — func decimalValueFromFloatBits(f float64, bitSize, precision, scale int) (Decimal128, error) {, moved 2026-09-11 (#1026)
Superseded: CAST AS DECIMAL is implemented; expr/cast_decimal.go reaches the checked decimal converters, so the claim that SQL cannot reach this is stale.

decimalValueFromFloatBits is DecimalValueFromFloat with the width of the box
the float ARRIVED in. bitSize picks the float32 or the float64 spelling, so
a REAL holding 0.1 stores as 0.1 and not as the 0.10000000149011612 its
widening to float64 makes exact — the same rule batch.setCheckedDecimalFloat
follows for the row-to-batch side of the same conversion.

A RECORDED DIVERGENCE, verified live on postgres:17-alpine: PostgreSQL's
float8 -> numeric cast renders the float with %.15g, so
`4611686018427387904::float8::numeric` is 4611686018427390000 there and
4611686018427388000 here — wadjet keeps the 17 significant digits that
identify the float, PostgreSQL keeps 15. Shortest-round-trip is chosen
deliberately: it is the only rendering that names the float it came from,
and it is what the row-to-batch twin already does, so the two paths cannot
disagree about one value. Nothing in SQL reaches this today — a float box
arrives through the embedded/HTTP API, and `CAST(x AS DECIMAL(p,s))` is
still ADR-0024 item 6's declared-STRING no-op — so when the CAST evaluator
lands (#555) it has to decide separately whether the SQL cast follows
PostgreSQL's %.15g.

The rendering goes into a STACK buffer: strconv.FormatFloat would allocate a
string per value, and ingest of a float-boxed decimal column is one of these
per row. 32 bytes covers every shortest 'g' rendering a float64 has (17
significant digits, a sign, a point and a four-character exponent).
