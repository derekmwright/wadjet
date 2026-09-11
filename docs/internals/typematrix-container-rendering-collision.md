# Typematrix container rendering collision

Source: internal/oracle/typematrix/typematrix.go — func arrayValue(i int) any {, moved 2026-09-11 (#1026)
Superseded: The small-input set-operation key now uses structural container encoding in set_op_key.go; the present-tense #612 wrong-answer example describes the old implementation.

arrayValue returns an ARRAY(STRING) whose length cycles 0..2 so a
zero-length array and a NULL array are both present and must stay distinct.

ids 101 and 102 break the pattern and hold ARRAY['a b'] and ARRAY['a','b'].
They are two DIFFERENT values that share a `%v` rendering (`[a b]`), which
is the one thing a container fixture built from `a%05d-%d` elements can
never contain: no two of those render alike, so an engine keying a
container by its RENDERING rather than by its structure agreed with itself
on every row, and no corpus entry over this table could see it. That is not
hypothetical — `SELECT DISTINCT a` answers 2 and `SELECT a UNION SELECT a`
answers 1 over a small table today, and `INTERSECT` of the two disjoint
one-row sets answers a row (#612).

The pair does NOT make this corpus catch #612 on its own: at this table's
size a set operation lowers to a GroupByAll aggregate, which keys through
the columnar encoding and is right, and only the small-input `rowHashKey`
path renders. The pair is here because it is the shape every future
container keyer has to be held to, and because a corpus in which no two
container values render alike cannot hold anything to it. #612 is pinned at
the layer where the rendering path is reachable.

Both ids are outside the c_arr NULL stride (103), so both are present.
