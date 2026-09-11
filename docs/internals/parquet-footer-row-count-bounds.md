# Parquet footer row count bounds

Source: internal/storage/parquet/footer.go — func ValidateFileMetaData(md *FileMetaData, fileSize int64) error {, moved 2026-09-11 (#1026)

ValidateFileMetaData holds a decoded footer to the claims it makes about
itself, before any of its numbers is used to size something.

A row group's num_rows is the size EVERY destination vector in a scan is
allocated for (batch.NewRecordBatch(schema, numRows)), and it is a signed
64-bit thrift field nothing had checked. The whole-file mutation fuzz
reached a negative one in seconds: "makeslice: len out of range", raised
while building the batch, before a single page was read.

Refusing negatives was not enough, and neither was holding each row group
to the FILE's total: the file's total is a varint out of the same footer.
num_rows = 2^40 on a two-row file reached makeslice with 128 GiB and died
as "fatal error: runtime: out of memory" — unrecoverable, so in a worker
process it is the worker; 2^30 was accepted outright and decoded after
allocating gibibytes. Three bounds close that, in the order they cost:

 1. Nothing is negative.
 2. Every row group is within MaxRowsPerRowGroup, and within what the
    file's own BYTES can carry (rowCeiling). Both are policy ceilings,
    documented at their constants.
 3. The file's total is exactly the sum of its row groups'. The format
    requires that, every writer in the corpus honours it (244 files,
    wadjet's own and pyarrow's, checked), and it is the only check here
    that is exact rather than generous — which makes it the one that
    catches a single flipped varint wherever it landed.
