# Kernel statistics literal domain

Source: internal/engine/exec/kernel/stats_domain.go — func StatsDomainValue(typ batch.TypeID, scale int, v any) (any, bool) {, moved 2026-09-11 (#1026)

StatsDomainValue converts a SQL literal into the representation a column's
parquet STATISTICS and DICTIONARY entries are in, and reports whether the
conversion exists.

It is the producer half of the rule the prune layer cannot enforce for
itself: `scan.CanPruneRowGroup` compares two `any` values by their Go kind
and has no idea what either MEANS, so a raw file bound and an engine literal
that both land in the same kind get compared as if they agreed. Three
columns did exactly that (#442, and #438 which is the same defect seen
through a DECIMAL):

	DECIMAL(18,4)  stats hold the UNSCALED integer (1500.15 -> 15001500)
	               and the literal arrives as float64(1500.15), so every row
	               group whose unscaled bound exceeds the literal is pruned.
	IPV6, UUID     stats hold the RAW 16 bytes and the literal arrives as
	               text, and '2' (0x32) sorts above every byte of a
	               2001:db8:: address, so every row group is pruned.

The engine's own order for those columns is the stored one — the filter
kernel converts the LITERAL (IPv6LitKey, and decimalLiteralAt
against the vector's scale) rather than rendering the column — so this
function is that same conversion, hoisted to where the planner still knows
the column's type and scale. Rendering the bounds the other way would be
wrong for IPv6: text order is not address order ('2001:db8::10' sorts below
'2001:db8::5').

A false second result means "no conversion" and the caller must WITHHOLD the
predicate from the prune layer entirely. Every type is listed explicitly and
there is no pass-through default, because a new type that silently inherited
"compare it raw" is precisely how this class arrives.
