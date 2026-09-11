# Kernel integer filter constant grammar

Source: internal/engine/exec/kernel/compare.go — func Int64FilterConst(v any) (int64, IntConstStatus) {, moved 2026-09-11 (#1026)
Superseded: parseIntText now uses PostgreSQL radix/separator grammar, not Go base-10. The example 017 denotes decimal seventeen, not seven.

Int64FilterConst resolves an integer-column filter constant to an int64,
reporting a non-OK status for a text literal that is not a usable integer.

An integer box (int64/int32/int from a parameter or a folded literal)
arrives already in the domain. A SQL text literal, though, is a STRING here
— and the old toInt64 read a string through parseTimestampString, so `k =
'abc'` (and even `k = '42'`, which no timestamp layout matches) coerced to
0 and MATCHED every row holding zero (#536, the integer rung of #463's
silent-sentinel ladder). It is read through Go's base-10 integer grammar
now, so '42' compares as 42 and 'abc' names no integer (IntConstSyntax): the
caller (ResolveFilterKernel's integer arms return a nil kernel; the row path
panics) refuses the query the way PostgreSQL does, rather than answering the
zero rows.

The grammar is PostgreSQL's own, not Go's: parseIntText reads the 0x/0o/0b
radix prefixes, the underscore digit separators and the leading-zero
decimals PostgreSQL 16+ accepts (`'0x1A'` = 26, `'1_000'` = 1000, `'007'` =
7), which Go's base-10 reader refused and Go's base-0 reader would have
misread ('017' is decimal seven there, not octal fifteen). Refusing input
PostgreSQL answers was a PG-superset regression (#634); it is closed.

TIMESTAMP is deliberately NOT routed here: its string literal IS a timestamp
and must keep reading through parseTimestampString — a quoted numeric string
against a TIMESTAMP column is #493's territory, not this fix's.
