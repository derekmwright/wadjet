# Bounded string cast character contract

Source: internal/engine/expr/cast_string_length.go — type castStringState struct {, moved 2026-09-11 (#1026)

`CAST(x AS VARCHAR(n))` and `CAST(x AS CHAR(n))` — the VALUE half of #838.

`castDestType` maps CHAR / VARCHAR / TEXT / STRING onto one unparameterized
`batch.TypeString`, so `n` was parsed by the SQL parser and then dropped.
Cast.Eval's string arm never even saw the parameterized spelling: its switch
matches the lowered type name exactly, and `varchar(4)` matches no case
label, so the whole cast fell to `default: return v` and returned its
operand untouched. A client casting to bound a column's width got a longer
string than PostgreSQL gives it — a wrong VALUE, not just wrong metadata,
and ADR-0012 item 5 fixes the order: the length is ENFORCED before it is
DECLARED, because declaring a bound nothing enforces is the first of two
lies rather than the end of one.

Measured live on postgres:17.11:

	CAST('abcdef' AS VARCHAR(4))    abcd
	CAST('abcdef' AS CHAR(4))       abcd
	CAST('éàüxyz' AS VARCHAR(3))    éàü      -- CHARACTERS, six octets
	CAST(12345 AS VARCHAR(3))       123      -- the rendering is truncated
	CAST('abcdef' AS VARCHAR(0))    22023 length for type varchar must be at least 1

CHAR(n) is TRUNCATED and not PADDED here, and that is a decision rather than
an omission. PostgreSQL's bpchar pads the stored value to n but strips
trailing blanks for `length()`, for `||`, and for every comparison —
`CAST('ab' AS CHAR(4)) = 'ab'` is true there, `length` is 2 and `… || 'x'`
is `abx`, all verified live. This engine has one TypeString and no bpchar,
so padding would leak blanks into GROUP BY keys, join keys and equality,
where PostgreSQL strips them: it would turn a rendering divergence into a
WRONG ROW SET. Truncating alone keeps every one of those three agreeing with
the server and leaves one residual — the rendered value of a SHORT CHAR(n)
is `ab` where PostgreSQL prints `ab  ` — which ADR-0012's list records.
