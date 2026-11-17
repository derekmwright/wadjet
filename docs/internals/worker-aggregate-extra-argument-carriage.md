# Worker aggregate extra argument carriage

Source: internal/worker/filter_compile.go — passArg := func(col string) {, moved 2026-09-11 (#1026)

The SECOND argument of a two-column aggregate — CORR/COVAR_*(x, y),
MIN_BY/MAX_BY(value, ordering) — is a column of this projection's
OUTPUT too, and it was only ever added by the `InputExpr == ""` branch
above, which a COMPUTED first argument skips. So `MIN_BY(a*2, id)`
reached HashAggregate with `input has: a * 2`, the ordering column gone
from the stream, and both DAG arms failed loud on a query the
single-process path answers (#713). A projection NARROWS to its
outputs: every argument the aggregate will read has to be one of them.

Only a bare column REFERENCE is passed through. A computed second
argument (`MIN_BY(a, id*2)`) is not materialized by this projection,
so emitting a pass-through of a name nothing produces would replace a
loud failure with a column of NULLs, which is the trade this file
exists to refuse. The stage DAG therefore refuses such a call
(`aggregate input "id * 2" is not a column of its input`). The
single-process path materializes every argument that is not a bare
column below the aggregate since #1694 (physical buildAggregate) and
answers it; carrying the same rule to the worker needs the extra
arguments' expressions and declared types on the stage spec, which it
does not have yet.
InputCol2 and InputCol3 take the SAME rule, spelled once: a name the
expression parser cannot read at all is still passed through (a
delimited identifier reaches here as its bare spelling), and only a
name it reads as something OTHER than a bare column reference is
declined. Writing the third argument's arm separately is how the two
would come to disagree.
