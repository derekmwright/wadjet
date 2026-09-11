# Order by expression materialization

Source: internal/planner/logical/order_by_keys.go — hiddenSortProjection, moved 2026-09-11 (#1026)

ORDER BY over an expression, and the family it closes.

A Sort reads columns by NAME. Everything upstream of it — the SELECT-list
Project, the aggregate, the scan — decides which names exist, and a sort
key that names none of them used to match nothing and return the input
untouched: right rows, arbitrary sequence, no error. #313 and #316 were two
spellings of that failure (an alias no stage emitted); this is the third and
widest one. `ORDER BY year(d)` and `ORDER BY -id` name no column at all
because nothing ever computed them, and `ORDER BY b` over `SELECT a` names a
column the Project already dropped. All three came back unsorted, and adding
the term to the SELECT list "fixed" each one — the tell that the sort was
keying on the projection's output names all along.

Two rules close it:

 1. A term the Sort's input does not carry is MATERIALIZED as a hidden
    column on the SELECT-list projection — evaluated where the expression's
    inputs still exist, sorted on, then dropped before the rows reach the
    client (Projection.Hidden).

 2. A term that can be neither resolved nor materialized is an ERROR. Sorting
    is not advisory: an engine that cannot honour an ORDER BY must say so
    rather than hand back an arbitrary order that looks like an answer. The
    shapes that cannot be materialized are named explicitly in
    hiddenSortProjection — none of them fails quietly.
