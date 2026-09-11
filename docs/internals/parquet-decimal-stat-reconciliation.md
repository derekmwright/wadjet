# Parquet decimal stat reconciliation

Source: internal/storage/parquet/stats_reconcile.go — func ReconcileRowGroupStats(fr *FileReader, schema []Column, stats RowGroupStats) RowGroupStats {, moved 2026-09-11 (#1026)

ReconcileRowGroupStats moves a row group's DECIMAL min/max bounds from the
scale the FILE declares to the scale the READ SCHEMA declares, and withholds
a bound it cannot move.

A DECIMAL statistic is the same kind of thing as a DECIMAL value — an
unscaled integer whose meaning is half a declaration — so ADR-0018's rule
covers it too: the file's number is input, not fact. Once the DECODE
reconciles a file that declares another scale (rescaleDecimalChunk), the
PRUNE has to reconcile it as well, or the two read the same predicate
differently and the prune deletes rows the filter would have kept. That is
exactly what happened: with the values fixed and the bounds left alone,
`WHERE a = 12.75` over a (15,2) catalog column pruned the whole row group of
a file that declared (15,4), because the predicate arrived as the unscaled
1275 and the footer said [127500, 127500] (#707).

Rescaling the BOUNDS is exact, not an approximation: PostgreSQL's
round-half-away-from-zero is monotone, so the rescaled minimum is the
minimum of the rescaled values and the same for the maximum. A bound that
cannot be moved — no carrier at the new scale, or a wide DECIMAL whose
footer bound is raw bytes rather than an integer — is DROPPED rather than
guessed at: withholding costs a prune, guessing costs rows
(exec/kernel.decimalStatsValue's rule, applied on the other side of the same
comparison).

The ordinary file allocates nothing and copies nothing: every column whose
declaration agrees with the catalog's takes DecimalRescalePlan's need=false
exit, and stats is returned as it came in.
