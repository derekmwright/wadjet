# Parquet string type length parser

Source: internal/storage/parquet/schema.go — func StringTypeLength(name string) (n int, err error, ok bool) {, moved 2026-09-11 (#1026)

StringTypeLength reads a PARAMETERIZED string type name — `VARCHAR(255)`,
`CHAR(4)`, `CHARACTER VARYING(10)` — and validates the parameter.

It is ONE reading, and that is the whole point of it living here. The review
of #838's first pass found the CAST door refusing `VARCHAR(0)` with 22023
while the DDL door CREATED the table: one type name, two dispositions across
two doors, which is the defect class this arc exists to close. The refusal
has to be where both doors can read it, below the expression layer and below
the planner — the same argument `ParseDateDays` settles for dates.

PostgreSQL 17.11's own rules and messages, measured:

	VARCHAR(0), CHAR(0)        22023  length for type varchar|char must be at least 1
	VARCHAR(1e8), CHAR(1e8)    22023  length for type varchar|char cannot exceed 10485760
	VARCHAR(abc), VARCHAR(-1)  42601  syntax error at or near "abc"|"-"
	TEXT(5)                    42601  type modifier is not allowed for type "text"
	VARCHAR(10485760)          accepted — the exact maximum

The type NAME in the 22023 message is PostgreSQL's internal one: `char` for
all of CHAR / CHARACTER / NCHAR, `varchar` for the varying spellings. TEXT
is deliberately NOT a length-carrying name here, because PostgreSQL allows
no modifier on it at all.

ok=false means the name is not a parameterized string type and the caller's
own rules stand; a non-nil error is the refusal, which every caller must
PROPAGATE — it is the answer to the query.
