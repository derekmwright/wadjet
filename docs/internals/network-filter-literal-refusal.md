# Network filter literal refusal

Source: internal/engine/exec/filter.go — networkConstError, moved 2026-09-11 (#1026)

networkConstError is decimalConstError's counterpart for the network types
whose kernel arm refuses a literal it cannot read as an ADDRESS: TypeCIDR
(kernel.CidrSortKey), TypeIPv6 (kernel.IPv6LitKey), and — since #519 closed
the same gap one type over — TypeIPv4 (kernel.IPv4LitKey), TypeMAC
(kernel.MACLitKey) and TypeUUID (kernel.UUIDLiteralToRaw).

All five used to answer instead of refusing, and the answers were silently
wrong in different directions: the CIDR arm returned a match-nothing
kernel, so `c_cidr <> 'garbage'` dropped every row; the IPv6 arm read an
unparseable literal as the empty raw address, which every stored address
compares ABOVE; the IPv4/MAC arms read it as the encoded zero, which
MATCHED every row holding the address 0.0.0.0 / 00:00:00:00:00:00; the
UUID arm read it as the empty string, which matches nothing for `=` and
EVERY row for `<>`. PostgreSQL refuses `'garbage'::inet` /
`'garbage'::macaddr` / `'garbage'::uuid` with 22P02, and ADR-0012 item 1
makes PostgreSQL the authority on error-versus-not, so this is its
SQLSTATE and its wording.

The row-at-a-time path raises the same error for the same literal
(expr.CmpNetworkLit's CIDR/IPv6 arms and expr's Cmp binding via
decimalLitCmp.refuseNonAddress, which covers IPv4/MAC/UUID too): one path
erroring while the other answers is the two-path defect class.
