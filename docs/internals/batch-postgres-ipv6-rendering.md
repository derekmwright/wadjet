# Batch postgres ipv6 rendering

Source: internal/engine/batch/vector.go — func FormatIPv6(raw []byte) string {, moved 2026-09-11 (#1026)

FormatIPv6 renders a 16-byte IPv6 address the way PostgreSQL's inet output
function does, which is NOT what net.IP.String() does for two families:

	::ffff:10.0.0.1   Go collapses a v4-MAPPED address to its bare dotted quad
	                  (`10.0.0.1`), a value the engine itself says the column
	                  does not equal — `a = '10.0.0.1'` is false and
	                  `a = '::ffff:10.0.0.1'` is true, both correctly (#580).
	::1.2.3.4         Go renders a v4-COMPATIBLE address in hex (`::102:304`)
	                  where the server prints the embedded quad.

PostgreSQL's rule, measured on 17.11 rather than remembered: take the FIRST
longest run of zero 16-bit words of length >= 2 and write it `::`; render
the trailing four bytes as a dotted quad when that run starts at word 0 and
is either six words long (`::a.b.c.d`) or five words long with word 5 equal
to 0xffff (`::ffff:a.b.c.d`). Everything else is lower-case hex groups. The
zero-run choice is Go's too, so only the dotted-quad rule differs.

Measured cells (`SELECT '<lit>'::inet` on 17.11): `::ffff:10.0.0.1`,
`::ffff:0.0.0.0`, `::ffff:255.255.255.255`, `::1.2.3.4`, `::2`, `::1`, `::`,
`0:1::`, `1::`, `2001:db8::1:0:0:1`, `::ffff:0:102:304`, `64:ff9b::102:304`.

The comparison and ordering value is untouched: this is the printed form
only, and `net.ParseIP` reads the text back to the same sixteen bytes, which
is what kernel.IPv6RowKey and exec.boxedIPv6Compare rely on.
