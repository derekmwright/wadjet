# Sql published output column names

Source: internal/planner/sql/output_column_name.go — func OutputColumnName(col SelectColumn) string {, moved 2026-09-11 (#1026)

OutputColumnName is the name PostgreSQL publishes an unaliased SELECT item
under — its `FigureColname`, decided from the parsed AST and never from the
expression's TEXT.

The whole rule, measured on PostgreSQL 17 over 49 spellings (#732):

	g, t.g, (g)                       → g            (the column)
	(c_row).b                         → b            (the FIELD)
	abs(g), count(*), sum(g) OVER ()  → abs, count, sum   (the function)
	CASE …, COALESCE, NULLIF, GREATEST → case, coalesce, …
	EXISTS (…)                        → exists
	ARRAY[…]                          → array
	EXTRACT(YEAR FROM d)              → extract
	CAST(g AS int), g::int            → g            (the ARGUMENT)
	CAST('2020-01-01' AS date)        → date         (the TYPE, only when the
	                                                  argument has no name)
	(SELECT g FROM … LIMIT 1)         → g            (the subquery's column)
	g + 1, -g, 1, 'abc', g IS NULL,
	g = 1, g BETWEEN 1 AND 2,
	g IN (1,2), g || 'x', (SELECT 1)  → ?column?

A CAST is the one PostgreSQL gets asked about most and the one most often
guessed wrong: it is named after its ARGUMENT, and reaches for the type only
when the argument itself is unnamed. The brief for arc E3 said "a CAST → the
TYPE" and the measurement said otherwise.

Several `?column?` in one SELECT list is legal and is what PostgreSQL does:
`SELECT g + 1, g + 2` returns two columns of that name. Output slots have
identity by POSITION (#556/#557), so a duplicate published name is not a
collision.

It returns "" for a STAR, which has no single name.
