# Parquet page crc check boundary

Source: internal/storage/parquet/page_reader.go — func (r *ColumnPageReader) verifyPageCRC(off int, ph *PageHeader, body []byte) error {, moved 2026-09-11 (#1026)

verifyPageCRC holds a page body to the checksum its own header carries.

parquet.thrift makes PageHeader.crc a CRC-32 over the page's serialized
body EXACTLY as stored — after compression, the header excluded, and for
a v2 page the uncompressed level sections included — using the standard
(IEEE, GZip) polynomial. A file that carries one has told the reader how
to know its own bytes are intact; decoding the body anyway answers the
query out of data the file itself says is wrong. That is what happened:
a single flipped payload bit in a parquet-go-written INT64 page turned
[42, 43] into [43, 43] with a nil error, while parquet-go refused the
identical bytes (#891).

PRESENCE, not value: ph.CRCSet. See PageHeader.CRCSet for why zero is not
the absence test.

The check runs on every body this reader DECODES — data pages v1 and v2,
the dictionary page, in both the row reader's and the native scan's
walks, and on the dictionary page DictionaryIfPure prunes a row group
from. It deliberately does not run on a page NextPageMaybeSkip skips: no
value comes out of those bytes, so nothing the reader returns can depend
on them, and paying a full-body checksum for a payload the skip exists to
avoid touching would spend the optimization. A skipped page whose bytes
are corrupt reads clean; the same file read whole refuses. Both halves
are gated.
