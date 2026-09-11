# Kernel cidr inet sort key

Source: internal/engine/exec/kernel/compare.go — func CidrSortKey(s string) (string, bool) {, moved 2026-09-11 (#1026)

CidrSortKey re-keys a CIDR/inet TEXT value ("192.168.1.0/24", "10.0.0.1/8",
or a bare "10.0.0.1") into PostgreSQL's `inet` order — network_cmp — as a
byte string two keys compare LEXICALLY in exactly that order.

PostgreSQL's network_cmp_internal compares, in this sequence:

 1. the address FAMILY (v4 before v6),
 2. the common bits under the SMALLER of the two prefix lengths,
 3. the prefix length itself,
 4. the FULL, UNMASKED address.

The key is [family][address masked to its own prefix, full width][prefix
length][full unmasked address], which reproduces that order exactly. Step 2
needs both operands and no single-value key can hold it directly, but the
masked address is equivalent: if the first min(len) bits differ, both keys
retain the differing bit and compare the same way; if they agree, the
shorter prefix's key has zeros where the longer one may have ones, so it
sorts first — which is step 3's answer — and when those bits are zero too
the keys tie and the explicit prefix-length byte decides. The trailing full
address is step 4.

Verified against live PostgreSQL 17 over host-bearing and canonical values,
v4 and v6, at mixed prefix lengths — the whole table is
TestCidrSortKeyMatchesPostgresInetOrder's fixture. Three of its consequences
are worth naming because a simpler key gets them wrong:

	'9.255.255.255/32' < '10.0.0.0/8'   — common bits decide before the mask
	'192.168.1.5/24'   < '192.168.1.0/32' — the MASK outranks the address
	'10.0.0.0/8'       < '10.0.0.1/8'   — host bits are kept, and ordered last

That last one is why the key cannot be built from net.ParseCIDR's MASKED
network alone, which is what this function did when #492 introduced it:
keying only ipnet.IP threw the host bits away, so '10.0.0.1/8' and
'10.0.0.0/8' became the SAME value and `= '10.0.0.1/8'` answered rows
holding a different address. Wadjet's CIDR column is unvalidated text
(internal/storage/ingest), and host-bearing prefixes are ordinary in the
network data this type exists for, so those are not edge values.

A BARE address with no "/" is a /32 (v4) or /128 (v6), which is what
PostgreSQL's inet does with the same input — `'10.0.0.1'::inet =
'10.0.0.1/32'::inet` is true. A v4-MAPPED v6 address ("::ffff:10.0.0.2")
keeps the v6 family, also matching PostgreSQL (`family()` answers 6).

ok is false when s is not an address at all. Callers must turn that into a
query ERROR, never a match-nothing kernel: see ResolveFilterKernel's
TypeCIDR arm.

Exported — unlike this file's other literal parse helpers
(parseIPv4ToInt64, parseMACToInt64), which internal/engine/expr duplicates
locally rather than importing — because this one is not a trivial
re-encode: expr.CmpNetworkLit's CIDR literal and this kernel's per-row CIDR
key MUST agree bit for bit, and two structural parsers maintained
separately is exactly the shape #492 already is (the kernel path numeric,
the expr path lexical). One implementation, shared, is what keeps them from
drifting apart again.
