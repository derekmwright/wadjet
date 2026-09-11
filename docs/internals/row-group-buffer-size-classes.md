# Row group buffer size classes

Source: internal/planner/physical/scan_rowgroup_load.go — getSlab / putSlab, moved 2026-09-11 (#1026)

```go
// getSlab and putSlab reuse row-group buffers within one scan source, in
// buckets that are power-of-two size classes OF THE ROW GROUP'S OWN byte
// range.
//
// What a row group's buffer may be is not a matter of taste: it is charged to
// the query's memory budget, so a buffer bigger than the row group is a charge
// for memory the row group does not need. Three shapes were tried and each
// failed at one end or the other, which is why the rule that ships states an
// invariant instead of a preference:
//
//   - The process-wide readBufPool, whose only rule is "big enough". It also
//     holds whole-FILE buffers, so a row-group request draws one and the
//     charge becomes the largest file the process ever read.
//   - The parquet chunk pool's size classes. They have a 64 KiB FLOOR, so a
//     5 KiB row group is held in 64 KiB — a fixed floor is a tuning constant
//     with a pool's manners, and the gates caught it.
//   - One "big enough" pool per scan SOURCE. A source reads one TABLE, and a
//     table's files do not share a row-group size: a compacted file beside a
//     freshly ingested one is ordinary. Measured, a 332-byte row group of a
//     1,988-byte file drew a 105,900-byte buffer another file left behind and
//     was charged for it — 319x the row group, 53x the file.
//   - Bucketing by the FILE's exact largest row group fixed that but keyed on
//     a byte count that compression makes different for every file, so no two
//     files shared a bucket and every row group allocated: +29.2% heap over
//     the TPC-H SF1 suite, separated across five pairs.
//
// THE INVARIANT: a row group is CHARGED its own byte range, and HELD in a
// buffer of at most twice that. The class is derived from the row group, so
// there is no floor and no chosen number, and it decides only WHICH BUCKET a
// buffer is reused from — a buffer is always allocated at exactly the row
// group's size. Both halves are load-bearing: without the bucket, another
// file's shape serves this row group (319x); allocating AT the class instead
// of at the row group rounds every fresh buffer up to a power of two, which
// measured +6.6% suite heap (see getSlab). Two row groups in one class differ by less than
// 2x, which is where the bound comes from. The slack between the charge and
// the buffer is pool capacity, bounded by the row group itself and reused
// across the scan; ADR-0006's producer row 2 records that the row-group path
// does not reconcile the charge up to it, and why.
//
// Gated by TestARowGroupIsHeldInABufferAtMostTwiceItsSize (the invariant, over
// sizes from one byte to a megabyte) and
// TestOneSourceWithTwoRowGroupSizesChargesEachRowGroupItsOwnBytes (the charge,
// end to end over a table whose files have different row-group sizes).
```
