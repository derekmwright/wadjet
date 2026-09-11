# Parquet three digit month classification

Source: internal/storage/parquet/date_parse.go — func threeDigitMonthKind(month string) dateFieldsKind {, moved 2026-09-11 (#1026)

threeDigitMonthKind refuses a middle field of EXACTLY three digits, which is
the one width PostgreSQL will not read as a month.

The rule is PostgreSQL's DecodeNumber (datetime.c), and it is narrower than
"wider than two digits": a three-digit field with only the YEAR decided so
far, whose value is 1..366, is a DAY OF YEAR. So '2026-003' is 2026-01-03
there — but in a THREE-field date the day-of-year leaves the third field
nowhere to go and PostgreSQL answers 22007, while a three-digit value
OUTSIDE 1..366 is not a day of year at all, falls through to the month slot
and is 22008. Measured live on postgres:17-alpine:

	'2026-003-12'   22007       '2026-012-12'   22007       '2026-366-12'  22007
	'2026-000-12'   22008       '2026-367-12'   22008       '2026-999-12'  22008
	'2026-0003-12'  2026-03-12  '2026-00003-12' 2026-03-12
	'2026-01-003'   2026-01-03  '2026-01-0003'  2026-01-03

FOUR or more digits is a year-shaped token PostgreSQL accepts as the month,
and a three-digit DAY is accepted too — the year and the month are already
decided by then, so the day-of-year branch cannot fire. Both keep working
here unchanged, which is why this tests the width EXACTLY rather than
bounding it: refusing len > 2 would have refused input PostgreSQL takes,
which is the divergence ADR-0012 item 1 forbids, in exchange for closing one
it permits.

wadjet used to read every all-digit middle field as a month, so
'2026-003-12' stored 2026-03-12 for a string PostgreSQL rejects (#641).
dateFieldsOK here means "not this case", not "valid".
