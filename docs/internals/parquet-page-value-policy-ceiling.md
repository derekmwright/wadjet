# Parquet page value policy ceiling

Source: internal/storage/parquet/thrift.go — const MaxPageValues = 1 << 24, moved 2026-09-11 (#1026)

MaxPageValues bounds a page header's declared value count.

num_values is a thrift i32 that several decoders size an allocation from
before anything has looked at the page body — DecodeRLEInt32 allocates one
int32 per value, and the byte-array decoders one offset. Nothing bounded
it, so a single flipped byte in a varint could ask for gigabytes out of a
file a few hundred bytes long, and the process died in the allocator
rather than at the refusal that would otherwise have followed.

It was 2^28, which is a gibibyte of int32 per column and, since the scan
fans out per column, a gibibyte per column CONCURRENTLY — bought with a
twenty-byte page header. That was described as four orders of magnitude
above what writers produce; the real figure is further still, and it was
measured. parquet-cpp caps a data page at 20,000 values
(DEFAULT_MAX_ROWS_PER_PAGE) and parquet-mr at 20,000 rows, whatever the
byte target: pyarrow asked for 2 GiB pages over 40 million BOOLEAN rows
still wrote pages of exactly 20,000 values, and the largest page in this
repo's whole corpus is the same 20,000.

2^24 keeps ~838x headroom over that and holds the worst allocation a
header alone can provoke to 64 MiB. It is also the ceiling wadjet's own
writer is held to (writeDataPage), so the reader cannot refuse a file this
package wrote. The TIGHT bound is elsewhere and stays there: for a flat
leaf the page reader holds a chunk's pages to the row group's own row
count (ColumnPageReader.chargeRows), which is exact.
