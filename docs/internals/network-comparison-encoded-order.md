# Network comparison encoded order

Source: internal/engine/expr/expr_compare.go — type CmpNetworkLit struct {, moved 2026-09-11 (#1026)

CmpNetworkLit compares a bare column against a string literal that parses
as an IPv4 address, a MAC address, an IPv6 address, or a CIDR network,
without per-row parsing or boxing — CmpTemporalLit's counterpart for
network types, and for the same reason: ColRef.Eval boxes a TypeIPv4/
TypeMAC column as its raw encoded int64 (the representation arithmetic and
column-to-column ordering comparisons depend on — see networkTextFuncs) and
a TypeIPv6/TypeCIDR column as rendered TEXT (Vector.GetValue's default
case), so `ip_col = '10.0.0.1'` boxed the column as a decimal digit string
and the literal as itself, and `ipv6_col < '2001:db8::10'` boxed the
column's address as text and compared it LEXICALLY against the literal —
neither is the address's own order (issues found via README verification
and #492). Column type is unknown at compile time, so the literal is
pre-parsed into every encoding here and the right one picked per batch
from the column's resolved type; a non-network column (or a network
column whose type doesn't match the literal's parse) delegates to the
generic compare() with the original operand order, keeping semantics
bit-identical with Cmp in every sub-case — matching CmpTemporalLit's own
contract. Comparing the pre-parsed encodings (not as formatted strings) is
also what keeps ordering (<, >) correct: IPv4's big-endian uint32, MAC's
packed 48 bits, and IPv6's raw 16 bytes all sort the same as the address
itself, and CIDR's structural key (kernel.CidrSortKey) sorts the same as
PostgreSQL's inet order — where a dotted-quad, colon-hex, or CIDR-notation
STRING would sort lexically and disagree with it (e.g. "9.0.0.1" >
"10.0.0.1" as text).
