# Min max output declarations

Source: internal/engine/exec/agg_output.go — minMaxOutputType, moved 2026-09-11 (#1026)

minMaxOutputType maps a MIN/MAX input column type to its output type.
ok=false means "keep the planner-declared type" — reached only by a type
with no MIN/MAX kernel at all. It is a second return rather than a zero TypeID
because parquet.TypeBool IS zero, and a caller reading a real BOOL
declaration as "undeclared" is how #354 and #371 went wrong.

MIN/MAX return a value taken from the input column, so the rule is "the
input's own type", the same rule #392 settled for MIN_BY/MAX_BY. The
switch used to name seven types and return 0 for the rest, which left the
planner's FLOAT64 declaration standing: `MIN(mac_col)` answered
1.877e+14 instead of an address, `MIN(port_col)` answered a float, and
IPV6/CIDR/UUID/BOOL — whose kernels answered NULL anyway until #417 —
would have landed a string or a bool in a FLOAT64 vector and raised the
#361 guard on the parallel-emit goroutine.

DECIMAL is in the list since #455: the accumulator was always an exact
Int128 at the column's scale, and outputSchema copies that scale onto the
declaration, so MIN/MAX of a DECIMAL answers with a value the column
actually holds instead of a float64 that cannot hold it.

ARRAY/ROW/MAP/VECTOR are NOT in that list: containers answer a real
value now (#426, containerMinMaxState), boxed by GetValue and written
back with SetValue, so the only declaration that can hold it is the
input's own — the same rule #392 settled for MIN_BY/MAX_BY.

physical.minMaxDeclaredType mirrors this function and must change with it.
