# Merge column folded name aliases

Source: internal/coordinator/coordinator.go — mergeColIdx, moved 2026-09-11 (#1026)

mergeColIdx maps a partial result's column names to their positions for the
coordinator's merge stages (re-aggregation, scalar-aggregate folding, sort
and top-N), and adds the FOLDED spelling of each name as an alias.

The two spellings are two different strings for one column. `columns` comes
off the WIRE, so it carries the catalog's own spelling (`RegionID` for a
parquet-registered table); every lookup into this map is a name off the
LOGICAL plan's MergeInfo — a GROUP BY key, an aggregate's output column, an
ORDER BY key — which arrives folded from the lexer (#731). A byte-exact map
alone therefore missed, and each of the three consumers failed its own
silent way: reAggregatePartials REFUSES the query on a GROUP BY miss
(loud), drops the aggregate from the merge on an agg miss (partials
returned unmerged, so a group appears once per worker), and compareBatchRows
SKIPS an unresolvable ORDER BY key (rows come back in arrival order under an
ORDER BY the client asked for).

The alias is added only when batch.ResolveSchemaIndex — the one resolver
that owns this rule, see internal/engine/batch/schema.go — agrees the
folded spelling names THIS column and no other, so an ambiguous fold and a
qualifier whose case differs both keep the byte-exact miss.

The camel-case invariance battery does not yet distinguish this site:
measured with every other fix in place and this one reverted, it owns 0 of
its cells, because its probe-split shapes reach the merge with names the
scan already spelled the schema's way. The alias is the hazard closed, not
a cell recovered.
