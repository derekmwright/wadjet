# Typematrix cidr value shapes

Source: internal/oracle/typematrix/typematrix.go — func cidrValue(i int) string {, moved 2026-09-11 (#1026)

cidrValue is row i's CIDR text. It cycles through four shapes on purpose:
a CANONICAL /24, a HOST-BEARING /24, a host-bearing /8 and a /32 host
route.

The fixture used to be canonical /24s alone ("192.168.<i%256>.0/24"), which
made three of the four things PostgreSQL's inet order decides invisible to
this corpus: the mask length never varied, so "the shorter mask sorts
first" was never exercised; the host bits were always zero, so a key built
from the MASKED network alone — which is what #492's first CidrSortKey did
— could throw them away and still agree with itself; and no two rows shared
a network, so `= '10.0.0.1/8'` could not answer a DIFFERENT address's row.
Wadjet's CIDR column is unvalidated text (internal/storage/ingest) and
host-bearing prefixes are ordinary in the network data this type exists
for, so the canonical-only fixture was not the conservative choice.

id=700 lands on case 0 and keeps its old value, "192.168.188.0/24", so
networkLit's equality literal is unchanged.

id=298 spells id=299's own /32 address BARE (no "/32") instead of taking
its own case's shape. Both land inside union_c_cidr's `WHERE id < 300` arm,
so a query that unions the fixture against itself holds one address two
ways: PostgreSQL's inet calls a bare address and its own /32 host route ONE
value (`'10.0.0.1' = '10.0.0.1/32'`), and text order calls them two
distinct strings. That pair is what made #546 visible: the single-process
set operation's dedup (`rowHashKey`, keyed on the boxed value's raw text)
and the stage DAG's (a `GroupByAll` aggregate keyed through
`kernel.CidrOrderKey`, #520) answered the identical UNION DIFFERENTLY. Both
now key by inet (`physical.keyValueText`'s TypeCIDR arm), so these two rows
are what keeps that agreement gated rather than what records its absence.
