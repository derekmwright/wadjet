# Boxed partial merge key framing

Source: internal/engine/exec/sort.go — appendKeyValue, moved 2026-09-11 (#1026)

appendKeyValue writes a value to a byte buffer without fmt.Sprint overhead.

This is the k-way MERGE key for drained partial aggregate runs
(appendSerializedKey, aggregate_partial_drain_cursor.go), so a type that
falls through here does not merely sort oddly — every group of that type
merges into ONE. The default used to be the constant string "<unknown>",
which did exactly that to a BYTES group key: distinct in memory, collapsed
into a single group the moment memory pressure forced a drain, so the same
query answered differently depending on how much memory it had.

Boxed forms reaching here come from Vector.GetValue: bool, int32, int64,
float32, float64, string (STRING and every type that renders as text —
IPV6, CIDR, UUID, IPV4, MAC, DATE, DECIMAL), []byte (BYTES), []any (ARRAY),
map[string]any (ROW, MAP) and []float32 (VECTOR).

The encoding must be INJECTIVE, not merely deterministic. Two group keys
that share bytes are one group after a drain, and the query answers
differently depending on how much memory it had — the same failure the
"<unknown>" constant caused, reached by a subtler route. `%v` is not
injective for any container (ARRAY["a b"] and ARRAY["a","b"] both print
`[a b]`; ROW{a:"b c:d"} and ROW{a:"b",c:"d"} both print `map[a:b c:d]`),
and a raw byte run is not injective against serializeKey's single 0x00
separator (BYTES "a\x00" ‖ "b" and BYTES "a" ‖ "\x00b" are the same five
bytes). So every variable-width form is length-prefixed and every
container walks its elements, mirroring appendColumnValue's framing.

The fixed-width text forms — the integers, floats and bools — are
unchanged: they contain no 0x00, so the separator still delimits them, and
appendTypedIntKey (aggregate_partial_drain_cursor.go) writes the same bytes
for an int-mode key without boxing it.
