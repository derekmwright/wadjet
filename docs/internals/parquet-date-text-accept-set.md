# Parquet date text accept set

Source: internal/storage/parquet/date_parse.go — func ParseDateDays(s string) (int32, error) {, moved 2026-09-11 (#1026)
Superseded (2026-10-02, arc PW round 2): the trailing-time validator (isTimeOfDay) and the TIMESTAMP layout list are gone. ParseDateDays and ParseTimestampMillis / ParseTimestampWallClock / ParseTimestampZone read ONE grammar, parseTemporalText: the date below, then whitespace or 'T'/'t', a clock H[H]:M[M][:S[S][.digits]], and a zone 'Z' or ±H[H][:M[M][:SS]] / ±HMM / ±HHMM / ±HHMMSS (a space before it or not; negative offsets included). DATE drops the validated clock; TIMESTAMP keeps it, rolling 24:00:00 and second 60 forward. Hour > 24 (or 24 with anything after it), minute > 59, second > 60 (or 60 with a fraction) and a field wider than nine digits are 22008; a displacement past ±15:59:59 or a zone minute past 59 is 22009 (ZoneRange). `12:00:00ZZ` and `12:00:00+`, which the old validator accepted, are 22007 as on PostgreSQL. TIMESTAMP is bounded by PostgreSQL's TIMESTAMP range (294277-01-01 is 22008). The PostgreSQL forms outside the grammar (zone names, AM/PM, special values, BC, month names, Julian days, ISO-basic clocks) are refused 22007 — catalog temporal r25.

ParseDateDays converts a DATE string to days since 1970-01-01, or returns a
classified DateParseError. It is the single string→date conversion for the
engine: the filter kernel (kernel.parseDateToDays), the parquet writers
(parseDateForWrite / the native writer's leaf), the ingest boundary
(ingest.checkType via ValidateDateString) and the row→batch builder
(batch.parseDateString) all route through it, so the accept-set and the
error classification are decided in exactly one place.

The accept-set is the UNAMBIGUOUS year-first spellings a client sends —
those PostgreSQL's default DateStyle (ISO, MDY) parses one, deterministic
way, so wadjet can match its value exactly: a four-digit (or wider) leading
YEAR with a '-', '/' or '.' separator ("2026-01-02", "2026-1-2",
"2026/01/02", "2026.1.1"), the compact 8-digit form ("20260102"),
surrounding whitespace, and a trailing time-of-day (space- or T-separated,
optional 'Z'/offset, truncated to the date, matching a timestamp text cast
to date). It rejects — never silently reads as the epoch or a guessed year
— a string that is not a date at all (22007) and a well-formed but
nonexistent or out-of-range calendar date such as 2026-02-30, month 13 or
day 32 (22008).

Two of those refusals exist because PostgreSQL refuses them and they used to
be SUPERSET accepts here, on the WRITE path, so wadjet STORED a date no
PostgreSQL client could have written (#641): YEAR ZERO in every spelling
(22008 — PostgreSQL's calendar puts 1 BC immediately before 1 AD), and a
MONTH field of exactly three digits (see threeDigitMonthKind, which is also
why a FOUR-digit month and a three-digit DAY are still accepted).

The accept-set is still narrower than PostgreSQL's in the other direction,
and every one of those is a REFUSAL rather than a different value: the
two-field day-of-year form ('2026-003' is 2026-01-03 there), the BC suffix,
and the DateStyle-dependent spellings deferred to #639.

Not accepted, and deliberately ERRORING rather than guessing (#639): any
spelling whose field ORDER PostgreSQL decides from DateStyle rather than
from the digits — a short leading field it reads as the MONTH ("5/6/7" is
2007-05-06, "01/02/2026" is 2026-01-02, "31/1/2" is month 31 → rejected),
two-digit years, DMY, and month names ("Jan 2 2026"). The invariant is that
each ERRORS: wadjet never accept-and-stores a DATE whose value would differ
from PostgreSQL's, so no unsupported spelling can become 1970-01-01 or a
wrong year.
