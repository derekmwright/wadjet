#!/usr/bin/env python3
"""Generate page_crc.parquet / page_crc_v2.parquet: PyArrow files with PAGE CHECKSUMS.

The reader's CRC gate needs a fixture written by an implementation that is not
ours and not the Go library the gate's other cells use, so that "the checksum
the file carries is the checksum we compute" is a cross-implementation fact
rather than a round trip. PyArrow (parquet-cpp) writes checksums only when
asked — write_page_checksum defaults to False — which is why no other fixture
in this directory carries one.

Small pages (data_page_size) so the file holds SEVERAL checksummed pages per
column, and dictionary encoding on the string column so a checksummed
DICTIONARY page is covered too.

Run: python3 gen_page_crc.py   (writes page_crc.parquet beside this script)
"""
import os
import pyarrow as pa
import pyarrow.parquet as pq

N = 4000
tbl = pa.table({
    "i64": pa.array([i * 7 - 100 for i in range(N)], pa.int64()),
    "f64": pa.array([i / 3.0 for i in range(N)], pa.float64()),
    "s": pa.array([f"row-{i % 37}" for i in range(N)], pa.string()),
    "opt": pa.array([None if i % 5 == 0 else i for i in range(N)], pa.int32()),
})

here = os.path.dirname(os.path.abspath(__file__))
for name, page_version in (("page_crc.parquet", "1.0"), ("page_crc_v2.parquet", "2.0")):
    out = os.path.join(here, name)
    pq.write_table(
        tbl,
        out,
        compression="snappy",
        use_dictionary=["s"],
        data_page_size=1024,
        write_page_checksum=True,
        write_statistics=True,
        version="2.6",
        data_page_version=page_version,
    )
    print("wrote", out, os.path.getsize(out), "bytes")
