# Kernel postgres integer input

Source: internal/engine/exec/kernel/int_literal.go — func parseIntText(s string) (int64, NumConstStatus) {, moved 2026-09-11 (#1026)
Superseded: The decimal-leading-zero example 017 is seventeen, not seven; the value 007 is seven.

parseIntText reads PostgreSQL's INTEGER type input (pg_strtoint32_safe /
pg_strtoint64_safe as of 16), which is a strict superset of Go's base-10
grammar in three places, all of them observable (#634, split out of #536):

	'0x1A'  -> 26      '0o17'  -> 15      '0b101' -> 5
	'1_000' -> 1000    '0x1_A' -> 26      '0x_1A' -> 26
	'007'   -> 7       (decimal seven, NOT octal fifteen)

Neither Go base matches. `strconv.ParseInt(s, 10, 64)` — what this replaced
— refuses the radix and underscore forms, which is a PG-SUPERSET REGRESSION:
it raises 22P02 for input PostgreSQL answers, and ADR-0012 item 1 says the
binder never refuses what PostgreSQL accepts. `ParseInt(s, 0, 64)` is wrong
the other way: it reads '017' as octal 15 where PostgreSQL reads decimal 7.

The underscore rule is PostgreSQL's exactly, including its one asymmetry,
verified live on postgres:17-alpine: an underscore must be FOLLOWED by a
digit ('1000_' and '1__000' are 22P02) and may not be FIRST in a decimal
('_1000' is 22P02) — but it MAY be first after a radix prefix ('0x_1A' is
26), because the prefix already stands in front of it. PostgreSQL's own
source has the "underscore may not be first" check in the decimal branch
alone, and this reproduces that rather than tidying it.

Whitespace is pgIntWhitespace — C isspace() in the default locale, NOT
strings.TrimSpace, which also strips NBSP. PostgreSQL rejects an
NBSP-prefixed integer (verified live), so trimming it would accept input
PostgreSQL refuses.

Overflow reports NumConstRange (22003, "value \"X\" is out of range for type
bigint"), never a wrapped value. The accumulation runs NEGATIVE for
PostgreSQL's own reason: two's complement is asymmetric, so
-9223372036854775808 has no positive counterpart and accumulating
positively would refuse the one value int64's minimum bound sits on.
