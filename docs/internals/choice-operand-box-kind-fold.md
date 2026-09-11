# Choice operand box kind fold

Source: internal/engine/expr/boxed_pair.go — joinOperandKinds, moved 2026-09-11 (#1026)

joinOperandKinds is classifyOperand over a set of alternatives that one
value is chosen from.

The join folds the alternatives through PostgreSQL's own numeric ladder for
every pair EXCEPT one: a DECIMAL arm keeps the kind decimal, because the
kind's claim is about the BOX ("a string from here is decimal text") and
that stays true however wide the fold's TYPE is. The TYPE question — which
PostgreSQL answers float8 for `numeric ∪ float8` — is
extremumArms.commonKind's, and it is what the LITERAL is coerced to; the two
are deliberately separate answers to separate questions. A QUOTED literal
alternative contributes
nothing and takes the others' type, the way PostgreSQL resolves an
unknown-typed literal from its context — `COALESCE(d, 'text')` is a numeric
expression there, not an ambiguous one. Any other disagreement leaves the
box ambiguous again and yields boxUnknown.

A NULL alternative is SKIPPED outright. `COALESCE(d, NULL)` is a DECIMAL
expression, and reading the NULL literal as its own kind poisoned the join
to boxUnknown — which is how a DECIMAL column wrapped in a COALESCE started
comparing as rendered text (#504 review, B2). NULL never reaches a
comparison anyway: every caller short-circuits a nil operand first.
