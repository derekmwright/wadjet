# Boxed literal refusal lifetime

Source: internal/engine/expr/decimal_literal.go — type refuseArm struct {, moved 2026-09-11 (#1026)
Superseded: refuseArm now caches quotedLitMask across supported column types; it is no longer a DECIMAL-only refusal.

refuseArm is the plan-shaped half of #463's refusal — SQLSTATE 22P02, never
a value — for ONE operand pair: the bare-column operand, and the other
operand's source text when that text does not name a number. A nil col
means no refusal is possible for this pair whatever the column turns out to
be, which is the overwhelmingly common case and the one that has to be free.

It exists because the refusal's two questions have very different lifetimes.
"Is the literal a number?" is fixed for the query — `kernel.DecimalLiteral.
Numeric()` walks the digits from scratch — and "is the column a DECIMAL?" is
fixed for the query too, once one batch has answered it. Asking both PER ROW
is what the first #505 fix did, and it cost a `NewDecimalLiteral` allocation
plus a full text parse on every row of every batch at three sites that are
otherwise allocation-free: +35% on a simple CASE over a DECIMAL column, +25%
on IS DISTINCT FROM, and +200% with 7x the bytes on an exponent-form literal
— reintroducing exactly the regression `decimal_order_bench_test.go` was
written to hold shut (it is why `decimalLitCmp.numeric` is a cached slice
rather than a per-row `Numeric()` call).

This is the refusal half of the boxed comparison's job, for the three sites
(#465) that carry a literal's exact text into that comparison but never call
Numeric() on it: Case's simple-CASE arm, IsDistinctFrom, and pickExtremum
(GREATEST/LEAST). boxedPair's literal arm only fires for a literal ALREADY
known to be numeric (compileLit sets Lit.Text for exactly that shape) — a
non-numeric string like 'abc' carries no Text, so no arm matches it and the
comparison falls through to compare()'s ordinary string comparison instead
of refusing, which is #463's exact failure mode on the boxed path (#505).

bindDecimalCmp's `d op lit` shape does not need this: NewCmp binds it at
construction time and decimalLitCmp.order already refuses there. This is
for the three sites that reach a DECIMAL column with no such binding.
