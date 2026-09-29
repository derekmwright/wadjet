# Equi join key common types

Source: internal/planner/physical/join_key_types.go — joinKeyCommonType, moved 2026-09-11 (#1026)

```go
// The COMMON TYPE of an equi-join key pair (#615, #650, #663).
//
// A comparison resolves its two operands to one type before comparing them;
// a hash key was built from each side's own storage encoding. ADR-0023 says
// those two must name one relation — "compares equal" and "keys alike" — and
// across numeric widths they did not: `a.i = b.d` in a WHERE clause was right
// and the same predicate as a JOIN key matched almost nothing, `numeric IN
// (SELECT bigint)` panicked in the integer fast path, and an `int = float`
// key in a three-relation join panicked inlineIntProbe.
//
// PostgreSQL's answer for a JOIN key is OPERATOR resolution, not the set
// operations' `select_common_type` — the two ladders are different and both
// are pinned by internal/coordinator's numwidth fixture. Read off EXPLAIN
// VERBOSE on postgres:17.11:
//
//	int4    = int8     ->  int8      (int48eq; no cast on either side)
//	int     = float4   ->  ((int)::float8) = float4      -> float8
//	int     = float8   ->  ((int)::float8) = float8      -> float8
//	int     = numeric  ->  ((int)::numeric) = numeric    -> numeric
//	float4  = float8   ->  float48eq                     -> float8
//	numeric = float4   ->  float4 = ((numeric)::float8)  -> float8
//	numeric = float8   ->  float8 = ((numeric)::float8)  -> float8
//	numeric = numeric  ->  numeric, exact, at either declared scale
//	date    = timestamp ->  date_eq_timestamp: the date at its midnight (#1378)
//
// so float4 is NOT a rung: everything that meets it except another float4
// goes to float8. (A set operation over the same pair narrows to real
// instead, because real is a PREFERRED type of the numeric category for
// `select_common_type` and merely a resolvable one for an operator. That path
// is setOpWiden and is unchanged by this.)
//
// The DECIMAL rung needs no (p,s). batch.AppendDecimalKey is scale-
// normalized, so 2, 2.00 and 2.0000 are one key already (#474) and an integer
// keyed at scale 0 lands on the DECIMAL holding the same quantity. That is
// also why nothing here can overflow: a key is the value's digits, not a
// column.
```

The one rung outside the numeric ladder is a DATE against a TIMESTAMP
(#1378): `batch.TemporalCommonType` resolves the pair to TIMESTAMP, and
`exec.AppendWidenedKeyValue` keys the DATE side as its midnight's epoch
milliseconds — the eight bytes the TIMESTAMP side keys as itself — so the
build, the probe, the bloom filter and the shuffle partition hash agree.
`exec.joinKeyUsesIntPath` keeps the pair off the integer fast path, which
reads each side's raw integer (a day count against milliseconds).
