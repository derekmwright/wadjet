# Boxed scalar order equivalence

Source: internal/engine/exec/compare_boxed.go — boxedValueCompare, moved 2026-09-11 (#1026)

boxedValueCompare returns the null-blind comparator for col's declared
type. Only the types whose box loses information the order needs are
resolved here; everything else is compareAny, whose dynamic dispatch is
already the columnar order.

The ADDRESS types below join the containers and DECIMAL because
Vector.GetValue renders them for DISPLAY and display order is not address
order (#569, the windowed MIN/MAX half of the split #492/#520/#565 closed
for the filter and the sort):

	IPV4  "9.0.0.1" > "10.0.0.1" as text, < as an address
	IPV6  "2001:db8::9" > "2001:db8::10" as text, < as an address
	CIDR  '9.255.255.255/32' vs '10.0.0.0/8', and the /mask ranks too
	MAC   agrees today, and is re-keyed anyway — see boxedMACCompare

The types NOT listed here order correctly under compareAny, and each for a
reason worth stating rather than re-keying at a cost:

  - UUID renders FIXED-WIDTH LOWERCASE HEX with its dashes at fixed
    positions, and hex digits ascend in ASCII, so the text order IS the
    raw-byte order sortCompareString gives the column.
    TestBoxedCompareAgreesWithColumnarForEveryFlatType pins that
    equivalence, so a rendering change cannot quietly break it.
  - BYTES boxes as []byte and takes compareAny's bytes.Compare arm — the
    same bytewise order sortCompareString gives the column.
  - BOOL boxes as a bool: false < true, compareAny's bool arm.
  - STRING/the integer-backed types/DATE/TIMESTAMP/DURATION/PORT/PROTOCOL
    box as the value the kernel compares, or as a byte-ordered rendering of
    it (DATE's ISO form).
  - VECTOR boxes as []float32 and takes kernel's float order element-wise.
