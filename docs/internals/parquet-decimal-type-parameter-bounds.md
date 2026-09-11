# Parquet decimal type parameter bounds

Source: internal/storage/parquet/schema.go — func ParseDecimalParams(s string) (precision, scale int, err error) {, moved 2026-09-11 (#1026)

ParseDecimalParams extracts precision and scale from a type string like
"DECIMAL(10,2)", and REFUSES a declaration this carrier cannot honour.
A bare DECIMAL with no parameters is (38, 0).

The bounds are 1 <= precision <= 38 and 0 <= scale <= precision.

  - The precision bound is a DOCUMENTED DIVERGENCE from PostgreSQL, which
    accepts numeric(p, s) up to p = 1000 because its numeric is unbounded.
    Wadjet's DECIMAL is a 128-bit unscaled integer (ADR-0024 item 1) and 38
    digits is its whole range, so `DECIMAL(50,2)` is a column no value can
    satisfy. It used to be ACCEPTED, and the writer then emitted a 16-byte
    FIXED_LEN_BYTE_ARRAY leaf annotated DECIMAL(50, s) — an annotation the
    payload cannot hold, in a file the Apache implementation refuses to open
    (R8/#647). Refusing the DECLARATION is the only honest answer: the
    alternative is a column that lies about itself in every file it writes.
  - The scale bound is the PARQUET FORMAT's, not wadjet's: the DECIMAL
    logical type requires 0 <= scale <= precision. PostgreSQL accepts scale
    from -1000 to 1000, and `numeric(9,10)` (a value below 0.1 with nine
    significant digits) is legal there; there is no parquet annotation for
    it, so it is refused here too.

The SQLSTATE is 22023 invalid_parameter_value, which is what PostgreSQL
raises for a precision outside ITS bound ("NUMERIC precision 1001 must be
between 1 and 1000", verified live on postgres:17-alpine).
