# Kernel ipv6 stored row key

Source: internal/engine/exec/kernel/compare.go — func IPv6RowKey(s string) (string, bool) {, moved 2026-09-11 (#1026)

IPv6RowKey re-keys a TypeIPv6 column's RENDERED text back into the raw 16
bytes the column actually stores, which is what the vectorized kernel
compares (ResolveColColFilterKernel's TypeIPv6 arm reads BytesData directly)
and what a byte comparison orders as the address's own big-endian value.

It exists because the two evaluation sites read the column through different
doors. The kernel has the vector and reads the 16 bytes; the row-at-a-time
evaluator has ColRef.Eval's BOX, which for TypeIPv6 is the address's TEXT
(Vector.GetValue renders it through batch.FormatIPv6, PostgreSQL's inet
output rather than Go's — a v4-mapped address prints `::ffff:10.0.0.1`
there and `10.0.0.1` in Go, #580). Comparing that text
lexically is not the address's order — "2001:db8::9" sorts ABOVE
"2001:db8::10" as text and BELOW it as an address — so `WHERE a < z`
answered one thing through the scan and the opposite through a projection
or a later DAG stage's re-parsed filter (#565, #492's finding one type
over).

The round trip is exact: Vector.SetValue stores `net.ParseIP(s).To16()` and
GetValue renders that back, so parsing the rendering recovers the identical
bytes — including for a v4-MAPPED address, which Go renders as a dotted quad
and re-parses to the same v4-mapped 16 bytes, keeping the row on the v6 side
of PostgreSQL's family split the way the stored bytes already put it. That
is why this is NOT IPv6LitKey: a LITERAL dotted quad is a v4 address and
keys BELOW every v6 row (PostgreSQL compares family first), while a STORED
one is a v4-mapped v6 address and keys among them.

ok is false for a rendering that names no address, which a 16-byte column
does not produce — GetValue answers "" only for a value that is not 16 bytes
wide, which SetValue never writes.
