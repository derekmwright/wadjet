# Catalog histogram wire format

Source: internal/storage/catalog/histogram.go — const (, moved 2026-09-11 (#1026)
Superseded: Histogram supports native float64 boundaries as well as int64 and bytes; numeric boundaries are not all int64-encoded.

Histogram is an equi-depth histogram over a column's values. Each
bucket holds boundary values and a count. For numeric columns, the
boundaries are int64-encoded; the planner converts to/from float64
for range comparisons.

Equi-depth means each bucket holds approximately the same number of
values (1/K of the total). Bucket boundaries adapt to the data
distribution: dense regions get narrow buckets, sparse regions wide.
This gives accurate selectivity estimates for both common and rare
values.

Used in stats.estimatePredSelectivity for range/equality filters
when the column has a histogram in the catalog. Replaces hardcoded
0.33 / 0.1 fractions with data-driven estimates.

Wire format (binary, version-1):

	[1]   version (1)
	[1]   bucket count K (≤ 255)
	[1]   value type code (0=int64, 1=float64, 2=bytes)
	[1]   reserved
	[8]   total values
	[K+1] boundary values (K buckets → K+1 boundaries)
	[K*8] per-bucket counts (uint64 LE)

Boundary encoding depends on type code:
  - int64: 8 bytes LE per value
  - float64: 8 bytes LE (math.Float64bits) per value
  - bytes: uint16 length prefix + raw bytes per value
