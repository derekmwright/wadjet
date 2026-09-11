# Update set resolution and evaluation

Source: wadjet/dml.go — func ResolveDMLSetClauses(clauses []plansql.SetClause, target plansql.DMLTarget, schema []parquet.Column) ([]DMLAssignment, error) {, moved 2026-09-11 (#1026)
Superseded: The constant path now uses dmlLiteralText, including the unary literal forms it recognizes, rather than only a direct *plansql.Lit.

ResolveDMLSetClauses resolves an UPDATE's SET list against the table's
schema, before anything executes.

Two defects met here (#678). `UPDATE t SET nosuchcol = 1` reported
"UPDATE 1": the assignment was dropped into a map nothing read and the
matched rows were rewritten unchanged, where PostgreSQL raises 42703. And
the value was read ONLY as a literal, through a converter whose STRING arm
cannot fail — so `SET s = UPPER(s)` stored the seven characters "UPPER(s)"
into the column. PostgreSQL evaluates it, and so does this now.

Whether a SET value is a literal is decided from its PARSE, not from
whether a conversion succeeded, because for a STRING column the conversion
always succeeds and the literal path always won. A `*plansql.Lit` takes the
constant path — which is what keeps #647's declaration checks
(ConvertValueForColumn's DECIMAL precision, the temporal accept-sets)
running on the values that have them; anything else is compiled and
evaluated per row against the file's own batch, which carries the table's
declared types.

A SET VALUE resolves against the same relation name the WHERE does, so
`UPDATE pr AS a SET n = a.n + 1` reads a.n and `SET n = pr.n` under that
alias is 42P01 — PostgreSQL's answer for both (#686).
