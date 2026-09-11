# Typematrix network equality literals

Source: internal/oracle/typematrix/typematrix.go — var networkLit = map[string]string{, moved 2026-09-11 (#1026)

networkLit gives each network-native column a literal EQUAL to id=700's
value under allData's per-column formula (id=700 lands on a non-NULL
stride position for all six: 700 mod 59/61/67/71/73/79 never hits the
stride's NULL slot). EQUALITY only. ORDERING is a separate consumer class
(networkOrdLit, below): an ORDERING literal comparison (<, >) against
TypeIPv6 or TypeCIDR used to compare the column's rendered TEXT lexically
instead of the address's numeric/structural order, and disagreed outright
between this expr path and the stage DAG (#492, fixed) — this corpus now
gates it instead of skipping it as a known bug.

c_port/c_proto carry the UNQUOTED spelling here (a bare numeric literal,
`c_port = 1724`), because that is how a `PORT = <int>` / `PROTOCOL = <int>`
predicate is actually written. The QUOTED spelling used to be a defect and
is now a gate of its own: networkQuotedLit below.

It used to say the quoted form "hits a different, pre-existing bug (#493) —
kernel.toInt64's string case calls parseTimestampString, not a plain
integer parse, so `c_port = '1724'` silently compares against 0". That was
true when it was written and is not any more: #536 gave the integer arms
PostgreSQL's integer input grammar and #646 gave every numeric column type
the same rule, so `c_port = '1724'` reads 1724 and answers the same row on
both engines. The record is corrected rather than deleted because the
omission it explained is gone with it.
