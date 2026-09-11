# Parquet column chunk byte range

Source: internal/storage/parquet/page_reader.go — func chunkRange(cm *ColumnMetaData, fileSize int64) (start, end int64, err error) {, moved 2026-09-11 (#1026)

chunkRange turns a column chunk's footer offsets into a byte range inside
the file, refusing every claim the file cannot back.

Nothing validated these before. DataPageOffset and DictionaryPageOffset are
signed 64-bit thrift fields read straight out of the footer, and a negative
one became a negative slice index at the very first read: `r.data[r.off:]`
with r.off = -9025. Five of the six crashers the whole-file mutation fuzz
found were exactly that, and the file only has to be off by one flipped
byte in a varint to get there.

The END used to be CLAMPED to the file rather than refused, on the belief
that writers round TotalCompressedSize up. They do not. Every column chunk
in 44 files written by pyarrow (four codecs, both format versions, with and
without the page index), by parquet-go and by wadjet's own writer ends
EXACTLY where the next one begins — worst inter-chunk gap zero, worst
overlap zero — because the field is the sum of the chunk's page sizes,
headers included. Clamping therefore bought nothing and cost a silent wrong
answer: an overstated size reaches into the NEXT column's bytes, the page
loop decodes them as this column's, and a 128-row chunk comes back with 64
of its own values and 64 belonging to a neighbour, err == nil. An
overstatement is now refused, here and (against its neighbours, which one
chunk's metadata cannot see) in ValidateChunkLayout at open.
