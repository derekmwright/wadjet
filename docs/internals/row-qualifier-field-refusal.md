# Row qualifier field refusal

Source: internal/engine/expr/expr_leaf.go — ResolveColumnRef, moved 2026-09-11 (#1026)

A reference whose QUALIFIER names a ROW column of this batch is a FIELD
PATH and nothing else (ADR-0022): if the field is not one of that
container's children the answer is "no such thing", never some other
column that happens to share the field's name. rowColumnNamed is the
whole of that test — it finds a ROW spelled `parts[0]` or
`<qualifier>.parts[0]`, including the AMBIGUOUS case the lookup above
declines.

Asking `parentIdx >= 0` here instead was INVERTED: the ROW arm above
has already returned, so this point is reached only when the qualifier
names a column that is NOT a ROW — an ordinary scalar an arm happens to
have called `c` — and the refusal then swallowed every qualified
reference whose qualifier collided with such a name:

	WITH c AS (SELECT id, a * 2 AS dv FROM decpair)
	SELECT COUNT(*) FROM decpair t
	JOIN (SELECT id, b AS c FROM decpair) z ON z.id = t.id
	JOIN c ON c.id = t.id WHERE c.dv > 1
	-- PostgreSQL 5 · single 5 · DAG broadcast 5 · DAG shuffled 0

The same query with the arm publishing `zz` answered 5, which is what
says the collision was the whole of it.
