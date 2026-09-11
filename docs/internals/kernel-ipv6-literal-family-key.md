# Kernel ipv6 literal family key

Source: internal/engine/exec/kernel/compare.go — func IPv6LitKey(s string) (key string, ok bool) {, moved 2026-09-11 (#1026)

IPv6LitKey re-keys an IPv6 filter literal into the form a TypeIPv6 column's
rows compare against: the address's raw 16 bytes, which a byte comparison
orders exactly as the address's own big-endian numeric value.

A v4-shaped literal is not that, and is not a v4-MAPPED v6 address either.
PostgreSQL's inet compares the FAMILY first and puts every v4 address below
every v6 one (`'255.255.255.255'::inet < '::'::inet` is true), including
below a v4-mapped v6 address, which it still calls family 6
(`family('::ffff:10.0.0.2'::inet)` answers 6). The key for a v4 literal is
therefore the EMPTY string: it is shorter than, and a prefix of, every
16-byte row value, so it compares strictly below all of them and equals
none — PostgreSQL's family rule, with no per-row re-keying.

Reading a v4 literal as its v4-mapped 16 bytes instead — which is what
the TypeIPv6 kernel arm used to do, through a plain net.ParseIP —
placed it in the MIDDLE of the v6 range (below 2001:db8:: and above ::1),
while the row-at-a-time path fell through to a lexical text comparison
entirely: two paths, two orders, neither PostgreSQL's.

ok is false for a literal that is no address at all; the caller raises the
query error, the same as CidrSortKey's.
