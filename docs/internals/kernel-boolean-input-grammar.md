# Kernel boolean input grammar

Source: internal/engine/exec/kernel/bool_literal.go — func ParseBoolText(s string) (val, ok bool) {, moved 2026-09-11 (#1026)

ParseBoolText is `parse_bool_with_len` (src/backend/utils/adt/bool.c), which
is what PostgreSQL's `text::boolean` runs — the boolean INPUT grammar, not a
rendered-bool string match.

It accepts, case-insensitively and after trimming C `isspace` whitespace,
any non-empty PREFIX of "true", "false", "yes" or "no", plus "on"/"off" and
the single characters "1" and "0". The prefix rule is not decoration:
`'tr'::boolean` and `'fals'::boolean` answer on live PostgreSQL 17, and a
stricter reader would raise 22P02 for values PostgreSQL accepts. "o" alone
is the one prefix REFUSED, because it cannot choose between "on" and "off".

This is the ONE binding for the grammar (#574): both comparison paths read a
BOOL-column-versus-text-literal through it — the vectorized kernel here
(ResolveFilterKernel's TypeBool arm and inFilterBool) and the row-at-a-time
expr.compare — so they can no longer disagree with each other or with
PostgreSQL. internal/engine/expr.parseBoolText delegates here rather than
keeping a second copy. Before this, kernel.toBool read every string as
false (so `bo = 't'` matched the FALSE rows) while expr.compare rendered the
bool as "true"/"false" and matched only those exact spellings — two wrong
answers in opposite directions, ADR-0012 item 8's two-path split one type
over from the boxed-pair fixes.
