# Catalog file sketch bundle

Source: internal/storage/catalog/sketches.go — const (, moved 2026-09-11 (#1026)

FileSketches bundles every supported column's HLL + reservoir sample
for one parquet file into a single object-store blob. Storing these
sketches inline in the catalog manifest blew the NATS payload limit
at SF100 (63 lineitem files × 16 cols × 18 KB ≈ 18 MB > 1 MB max),
so the catalog now writes sketches to the object store and only
keeps a small reference in the manifest.

One blob per parquet file (≈ 16 cols × 18 KB ≈ 300 KB at SF100).
The blob is read once per file during AggregateColumnStats and the
individual sketches feed cross-file merges. Lazy: not loaded unless
the planner asks for the table's aggregated stats.

Wire format (binary, version-1):

	[4]   magic "WSKB" (Wadjet SKetches Bundle)
	[1]   version (1)
	[1]   reserved
	[2]   column count K (uint16 LE)
	then K entries, each:
	  [2]  column name length (uint16 LE)
	  [N]  column name bytes
	  [4]  HLL bytes length (uint32 LE; 0 = no HLL)
	  [M]  HLL bytes (HLL wire format, see hll.go)
	  [4]  Sample bytes length (uint32 LE; 0 = no Sample)
	  [P]  Sample bytes (Sample wire format, see sample.go)
