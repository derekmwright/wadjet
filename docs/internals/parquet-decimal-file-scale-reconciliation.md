# Parquet decimal file scale reconciliation

Source: internal/storage/parquet/decimal_reconcile.go — func DecimalRescale(d Decimal128, fromScale, toScale, precision int) (Decimal128, error) {, moved 2026-09-11 (#1026)
Superseded: The equal-scale path is not an unchecked identity: DecimalValueFromUnscaled still enforces catalog precision.

DecimalRescale moves an unscaled DECIMAL carrier from the scale the FILE
declares for it to the scale the CATALOG declares for the column, and holds
the result to the catalog's precision.

ADR-0018 is the charter: a parquet file's own numbers are INPUT, not fact.
For a DECIMAL that is not a figure of speech — the column chunk carries only
the unscaled integer and the SCHEMA carries the scale, so half of every
value lives in a declaration. When two files of one table declare that half
differently (a foreign writer, a pre-#647 write path, an unrepaired #608
file), reading both under one declaration means a different NUMBER, silently:
12.7500 written at scale 4 reads back as 1275.00 under a scale of 2 (#707).

The catalog is the authority for a table's type, so the file's carrier is
moved to the catalog's scale rather than reinterpreted under it. PostgreSQL
decides what "moved" means and it is the ASSIGNMENT cast, verified live on
postgres:17-alpine:

	12.7567::numeric(15,2)   -> 12.76     rounds half AWAY FROM ZERO
	(-12.7550)::numeric(15,2) -> -12.76
	12.75::numeric(15,4)     -> 12.7500   widening is exact
	123456789012.3456::numeric(9,2) -> 22003 numeric field overflow

which is exactly DecimalValueFromText's contract, so this routes through it
rather than growing a second scaling rule beside the one ADR-0024 already
gates. `Text` renders the carrier at the file's scale and the resolver reads
it back at the catalog's; the two are inverse by construction, which is why
a scales-agree call is the identity and returns before either runs.

The cost of going through text is deliberate. This fires only when a file's
declaration DISAGREES with the catalog's — a repair, not a read — and every
ordinary file takes the equal-scales exit above with no work at all. Buying
a second hand-rolled 128-bit divide for a path that by definition runs on
files this writer did not produce is the trade ADR-0018 §3 exists to refuse.
