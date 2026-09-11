package parquet

// ReconcileRowGroupStats moves DECIMAL min/max from FILE to READ schema
// scale alongside value decode, so pruning cannot discard matching rows
// (#707, ADR-0018). Half-away-from-zero rounding is monotone, making
// rescaled extrema exact bounds on rescaled values.
// Withhold any bound that cannot move: carrier overflow or raw-byte wide
// DECIMAL bounds cost a prune, never justify guessing.
// Matching declarations take DecimalRescalePlan's no-op path with no
// allocation/copy and return stats unchanged.
// See docs/internals/parquet-decimal-stat-reconciliation.md for the design.
func ReconcileRowGroupStats(fr *FileReader, schema []Column, stats RowGroupStats) RowGroupStats {
	if fr == nil || len(stats.Columns) == 0 {
		return stats
	}
	var leafByName LeafIndex
	indexed := false
	out := stats
	copied := false
	for _, col := range schema {
		if col.Type != TypeDecimal {
			continue
		}
		cs, ok := stats.Columns[col.Name]
		if !ok || !cs.HasStats {
			continue
		}
		if !indexed {
			leafByName, indexed = TopLevelLeafIndex(fr.Leaves()), true
		}
		idx, ok := leafByName.Lookup(col.Name)
		if !ok {
			continue
		}
		leaves := fr.Leaves()
		if idx >= len(leaves) {
			continue
		}
		from, need := DecimalRescalePlan(leaves[idx], col)
		if !need {
			continue
		}
		if !copied {
			out.Columns = make(map[string]ColumnStats, len(stats.Columns))
			for k, v := range stats.Columns {
				out.Columns[k] = v
			}
			copied = true
		}
		cs.MinValue = rescaleStatsBound(cs.MinValue, from, col)
		cs.MaxValue = rescaleStatsBound(cs.MaxValue, from, col)
		out.Columns[col.Name] = cs
	}
	return out
}

// rescaleStatsBound moves one footer bound to the read schema's scale, or
// returns nil so CanPruneRowGroup withholds. An int64 is the only shape a
// DECIMAL bound reaches here in that is comparable against the stats-domain
// value the predicate carries: parquet.statsToNative decodes an INT32 or INT64
// leaf's bound to int64 and a FIXED_LEN_BYTE_ARRAY one to a raw byte string,
// which kernel.StatsDomainValue never produces for a DECIMAL, so a wide
// column's bound is already unusable and dropping it changes no prune.
func rescaleStatsBound(v any, fromScale int, col Column) any {
	n, ok := v.(int64)
	if !ok {
		return nil
	}
	out, err := DecimalRescale(Decimal128From(n), fromScale, col.Scale, col.Precision)
	if err != nil {
		return nil
	}
	i, ok := out.Int64()
	if !ok {
		return nil
	}
	return i
}
