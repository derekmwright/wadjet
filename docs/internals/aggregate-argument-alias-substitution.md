# Aggregate argument alias substitution

Source: internal/planner/physical/window_alias_respell.go — respellAggInputExpr, moved 2026-09-11 (#1026)

respellAggInputExpr rewrites an aggregate's ARGUMENT expression so that every
column reference in it names what the stage BELOW the aggregate really
emits.

walkStages emits no stage for an ordinary Project, so a derived table's
SELECT list never happens on the DAG (ADR-0025). The aggregate's argument is
shipped to the worker as TEXT and compiled there against the batch the stage
hands it — which carries the SCAN's columns, not the derived table's names.
`SUM(CASE WHEN s = 'x' THEN twice ELSE 0 END)` over
`(SELECT s, id * 2 AS twice FROM t)` therefore read `twice` off a batch that
has no such column, `expr.ColRef.Eval` answered nil for every row, and the
SUM came back as the total of the CASE's ELSE branch — 0 where PostgreSQL
answers 2. It is TPC-H Q08's exact shape and it is type-independent: a plain
rename triggers it too, and a rename that SHADOWS a base column answered a
different wrong number rather than a zero (#702).

resolveAggInputName already does this for an argument that IS a name; this
is the same resolution applied one level down, to each reference inside an
argument that is an EXPRESSION. Both outcomes it can report are used:

	a RENAME       — the reference becomes the source column;
	a COMPUTED     — the reference becomes the expression that defines it,
	  alias          PARENTHESIZED, because the definition is substituted into
	                 a larger expression and `id * 2` spliced bare into `x * 3`
	                 would re-associate.

The single-process pipeline runs that Project as a real operator, so it is
already right and this rewrite is DAG-only: it is applied to the stage spec's
text, never to the logical node the local engine executes.
