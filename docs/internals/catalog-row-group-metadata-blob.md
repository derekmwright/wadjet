# Catalog row group metadata blob

Source: internal/storage/catalog/rgmeta.go — const (, moved 2026-09-11 (#1026)
Superseded: The native bound set now includes parquet.CidrInetBound in addition to int64/float64/string/bool. Path-keyed correctness requires object immutability; registered objects can be overwritten externally.

FileRGMeta carries one parquet file's row-group-level metadata: per-RG
row counts and per-column min/max/null statistics — exactly what
buildRGUnits needs from the file footer to enumerate and prune row
groups at plan time.

The catalog persists ALL files' RGMeta for a table in a single
object-store blob (see rgMetaObjectKey). Before this existed, every
scan read every file's footer over the network as a barrier before
any data download — 600 S3 range-read round-trips per lineitem scan
at SF10, ~2-5s of pure metadata latency on EVERY query. The footer
contents are static per file, so one blob GET per (table, manifest
version) replaces all of them; scans of files not covered by the
blob (fresh ingest, never-analyzed tables) fall back to footer reads.

A blob entry can never be WRONG, only missing or superfluous: entries
are keyed by object paths and data files are immutable, so a stale
blob still describes exactly the files it covers. That makes a fixed
blob key with atomic overwrite safe — no versioning needed.

The blob is written by AnalyzeTable (which decodes every file anyway)
and encoded in a binary format so stat values keep their exact native
types (int64/float64/string/bool, the closed set statsToNative
produces). Routing them through the JSON manifest instead would
degrade int64→float64 and non-UTF-8 strings, which is tolerable for
CBO selectivity but not for pruning decisions.

Wire format (binary, version-1):

	[4]   magic "WRGM" (Wadjet Row-Group Metadata)
	[1]   version (1)
	[3]   reserved
	[4]   file count (uint32 LE)
	then per file:
	  [2]  path length (uint16 LE) + path bytes
	  [4]  row group count (uint32 LE)
	  then per row group:
	    [8]  num rows (int64 LE)
	    [2]  column count (uint16 LE)
	    then per column:
	      [2]  name length (uint16 LE) + name bytes
	      [8]  null count (int64 LE)
	      [1]  has stats (0/1)
	      min value: [1] type tag + payload
	      max value: [1] type tag + payload

Value tags: 0 = nil, 1 = bool (1 byte), 2 = int64 (8 bytes LE),
3 = float64 (8 bytes LE), 4 = string ([4] length uint32 LE + bytes),
5 = CIDR inet bound (two length-prefixed strings: the inet-order sort key,
then the address text — see parquet.CidrInetBound).

A tag this decoder does not know is an ERROR, and TableRGMeta turns a
decode error into "no blob" (scans fall back to per-file footer reads), so
an older binary reading a blob a newer one wrote degrades in performance
only, never in answers. That is what makes adding a tag safe without a
version bump — a version bump would degrade the SAME way, for every table
rather than only the ones holding the new type.
