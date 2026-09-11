# Row and quantified compilation boundary

Source: internal/engine/expr/row_and_quantified.go — evalBool3, moved 2026-09-11 (#1026)

Row-value comparison and the quantified comparisons (`= ANY`, `<> ALL`,
`= SOME`).

Both spellings PARSED and neither COMPILED. `compileWithCtx`'s `default:`
arm returned `&Lit{Val: node.String()}` — the expression's own SQL text as a
string constant — for every node type it had no case for, and it had no case
for `*plansql.AnyAllExpr` or `*plansql.TupleNode`. Two different silent
wrong answers came out of that one line (#710):

	WHERE id = ANY(ARRAY[1,2])   the whole predicate became the STRING
	                             "id = ANY (ARRAY[1, 2])", so the filter's
	                             bool assertion failed and NO row matched.
	WHERE (id, n) = (1, 10)      each OPERAND became a string, and a real
	                             comparison compared "(id, n)" against
	                             "(1, 10)" byte-wise — a genuine false.

Six spellings answered zero rows on the ordinary query path, and DML
inherited every one of them: `DELETE … WHERE id = ANY(ARRAY[1,2])` reported
`DELETE 0` where PostgreSQL deletes two rows.

It is #610 recurring at the same `default:`, for the node types that fix did
not enumerate — which is why the arm now FAILS instead of guessing.
