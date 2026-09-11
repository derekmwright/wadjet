# Extremum whole call literal refusal

Source: internal/engine/expr/decimal_literal.go — func (r *extremumRefusal) check(b *batch.RecordBatch, folded batch.TypeID, foldedOK bool) {, moved 2026-09-11 (#1026)

check is refuseArm.check for the WHOLE call, not for one (best, candidate)
pair — because that is what PostgreSQL asks.

GREATEST/LEAST resolve ONE common type over EVERY argument
(select_common_type) and coerce the unknown-typed literal to THAT, so the
type in the message is not a property of whichever pair the values selected.
Verified live on postgres:17-alpine over a table with a bigint, a real and a
double column:

	GREATEST(bigint, 'abc')                -> ... for type bigint
	GREATEST(bigint, 'abc', double)        -> ... for type double precision
	GREATEST(real,   'abc', bigint)        -> ... for type real

Refusing against the pair's column instead named bigint for the second and
third, which is a different type in the message for the same query — and it
re-introduced #517's own finding one level down: a refusal that depends on
which operand won a comparison is not a type rule.
folded is the CALL's common type when the arms could fold one
(extremumArms.commonKind), and foldedOK=false when they could not — an
argument whose declaration this layer cannot read. Only the first case can
refuse a literal that SOME numeric type accepts, because a fold that missed
an argument is a LOWER BOUND on PostgreSQL's: `GREATEST(k, '3.1', d_val)`
folds to double there and ANSWERS, and refusing it against the columns this
layer happened to see was a PG-superset regression.

Where the fold failed, only a literal EVERY numeric type refuses is safe to
raise on — `GREATEST(k, 'abc', <a subquery>)` refuses whatever the subquery
turns out to be — and the type NAMED in that message is the column-only
fold, which can differ from PostgreSQL's when the unreadable argument would
have widened it. A missed refusal is the conservative side; the plan-time
binder catches the shapes it can prove.
