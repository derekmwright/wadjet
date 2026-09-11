# Decimal choice scale and constant fallback

Source: internal/engine/expr/choice_decimal.go — foldDecimalMetas, moved 2026-09-11 (#1026)

foldDecimalMetas resolves a choice's DECIMAL (p,s) from its arms'
contributions, and DROPS the CONSTANTS when the whole set does not fit.

The scale is max over the arms — ADR-0012 item 12's rule, and the only
choice that moves no value, since a narrower one drops digits a wider arm
holds. A LITERAL is an arm like any other for that purpose: `CASE … THEN
numeric(9,2) ELSE 0.125 END` needs scale 3 or the 0.125 has nowhere to go,
and PostgreSQL answers 0.125 there.

The cost, and it is recorded rather than hidden: PostgreSQL's fold carries
typmod -1, so its columns' rows keep their OWN scale (1.0001) while a
single-scale vector renders them at the fold's (1.00010). Same number, extra
zeros. The alternative — taking the scale from the declared operands alone —
keeps those rows byte-identical and REFUSES the literal, which turns queries
PostgreSQL and this engine both answer into 22003. A trailing zero is not a
wrong number; a refused query is no answer at all.

`constrained` is the fallback that makes an UNSELECTED literal free: when
the full fold does not fit the carrier, the constants are dropped and the
DECLARED operands decide alone. Without it `COALESCE(numeric(15,2),
numeric(38,10), '<forty digits>')` fell back to the FIRST arm's (15,2) and
then failed on the rows the numeric(38,10) column supplied — an arm no row
selects costing the query its answer, which is the one thing this fold must
never do.
