# Window min max result types

Source: internal/engine/exec/window.go — WindowMinMaxType, moved 2026-09-11 (#1026)

WindowMinMaxType is the output type MIN/MAX over a window declare for an
input column of type in, and whether they may re-declare at all.

The output type IS the input type, for every type the engine has: MIN/MAX
return one of their input's values untouched, so the only declaration that
can hold the answer is the one the value came out of. That is
minMaxOutputType's rule (aggregate.go) and MIN_BY's before it (#392), and
the two must agree — `MIN(c) OVER (PARTITION BY g)` and `MIN(c) … GROUP BY
g` are the same question asked twice, and a client that reads both in one
result set gets two column types for one answer if they disagree.

This used to be an ALLOW-LIST of ten types, and everything else kept the
planner's float64 declaration on the reasoning that the in-memory MIN/MAX
deque chose its answer with compareAny over Vector.GetValue's box, which
has no type tag to route a CIDR to kernel.CidrOrderKey. Both halves of
that have since stopped being true: the deque compares COLUMNAR
(kernel.CompareValuesAt, right here in computePartitionColumnar) and the
spill and global-window paths resolve newBoxedCompare from the declaration
(compare_boxed.go). What the declining left behind was not a safe
fallback but a FAILED QUERY — Vector.SetValue's #361 guard reporting
"cannot store string into FLOAT64 vector" for a shape BI tools generate
routinely, over twelve of the twenty-two types (#569): the eight scalars
CIDR/UUID/IPV6/IPV4/MAC/DECIMAL/BYTES/BOOL, and ARRAY/ROW/MAP/VECTOR,
while the plain aggregate over the identical column answered correctly.

Two types' window output differs from the grouped aggregate's — INT32 and
FLOAT32. The grouped MIN/MAX widens INT32 to INT64 and FLOAT32 to FLOAT64
because its accumulator is the wider type; the window copies an input value
rather than accumulating one, so nothing forces the widening and it keeps
INT32 and FLOAT32. Those narrower declarations are the PostgreSQL-correct
ones: `min(int4)` is `int4` and `min(real)` is `real` there, both ways.

The bool result is kept, rather than returning a bare type, because the
planner's caller has a second question the exec caller does not: whether
to leave windowOutputType's fallback standing for an input type it could
not resolve at all. Every type the engine has answers true.

Exported because the physical planner declares from the catalog with this
same function; two lists would drift.
