# Date filter literal refusal

Source: internal/engine/exec/filter.go — dateConstError, moved 2026-09-11 (#1026)

dateConstError is decimalConstError's counterpart for a DATE value whose
day count does not fit the int32 the DATE column encoding stores
(kernel.DateLiteralDays / #451). PostgreSQL raises 22008
(datetime_field_overflow) for a date outside its own representable range,
and ADR-0012 item 1 makes PostgreSQL the authority on error-versus-not, so
this is its SQLSTATE.

A DATE STRING literal reaches it three ways (#560): a well-formed date
whose day count does not fit int32 (#451's original case), a well-formed
but nonexistent calendar date ('2026-02-30', month 13, day 32), and a
string that is not a date at all ('not-a-date'). PostgreSQL raises 22008
(datetime_field_overflow) for the first two and 22007
(invalid_datetime_format) for the last; kernel.IsDateSyntaxError says which
so this picks the matching SQLSTATE, rather than the old (0, nil) that made
`d = '2026-02-30'` silently answer the count of 1970-01-01 rows.

#451's own reported literal is NOT an error and never was: `d =
'9999-12-31'` — the common SCD-2 end-of-time sentinel — used to compare as
2262-04-11 because parseDateToDays computed its day count through a
time.Duration, which SATURATES at ±math.MaxInt64 nanoseconds (~292 years)
rather than reporting an overflow. That is fixed in the arithmetic:
kernel.parseDateToDays counts civil days from t.Unix(), and 9999-12-31 is
2,932,896 days — about 700× inside int32 — so it is simply CORRECT.

The guard is also live for a caller that hands kernel.toDateInt32 a RAW
day count — an int64 or int compared against a DATE column, which no
parser bounds — and it is what keeps ResolveFilterKernel's "nil kernel,
caller raises" convention honest for the type.
