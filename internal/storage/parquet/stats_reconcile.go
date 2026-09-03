package parquet

// ReconcileRowGroupStats moves a row group's DECIMAL min/max bounds from the
// scale the FILE declares to the scale the READ SCHEMA declares, and withholds
// a bound it cannot move.
//
// A DECIMAL statistic is the same kind of thing as a DECIMAL value — an
// unscaled integer whose meaning is half a declaration — so ADR-0018's rule
// covers it too: the file's number is input, not fact. Once the DECODE
// reconciles a file that declares another scale (rescaleDecimalChunk), the
// PRUNE has to reconcile it as well, or the two read the same predicate
// differently and the prune deletes rows the filter would have kept. That is
// exactly what happened: with the values fixed and the bounds left alone,
// `WHERE a = 12.75` over a (15,2) catalog column pruned the whole row group of
// a file that declared (15,4), because the predicate arrived as the unscaled
// 1275 and the footer said [127500, 127500] (#707).
//
// Rescaling the BOUNDS is exact, not an approximation: PostgreSQL's
// round-half-away-from-zero is monotone, so the rescaled minimum is the
// minimum of the rescaled values and the same for the maximum. A bound that
// cannot be moved — no carrier at the new scale, or a wide DECIMAL whose
// footer bound is raw bytes rather than an integer — is DROPPED rather than
// guessed at: withholding costs a prune, guessing costs rows
// (exec/kernel.decimalStatsValue's rule, applied on the other side of the same
// comparison).
//
// The ordinary file allocates nothing and copies nothing: every column whose
// declaration agrees with the catalog's takes DecimalRescalePlan's need=false
// exit, and stats is returned as it came in.
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
