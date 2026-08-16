#!/usr/bin/env python3
"""Generate dict_fallback.parquet: chunks that mix dictionary-encoded and
PLAIN data pages (writer dict-page-overflow fallback), the layout real
ClickBench hits parts use for high-cardinality columns.

A tiny dictionary_pagesize_limit makes the dictionary overflow after the
first pages; pyarrow then falls back to PLAIN for the rest of the chunk.
A small data_page_size yields several pages on each side of the switch.
Values are pure functions of the row index so the Go regression test can
verify every value without a sidecar file:

  s (binary):  b"s%07d" % i          (unique -> guaranteed overflow)
  i64 (int64): i * 1000003           (unique)
  i32 (int32): i * 7                 (unique)
  f64 (double): i * 0.5
  sn (binary, nullable): NULL when i%5==0 else b"n%07d" % i

Run from this directory:  python3 gen_dict_fallback.py
Verifies both encoding classes appear in every chunk before writing.
"""
import pyarrow as pa
import pyarrow.parquet as pq

N = 20000
s = pa.array([b"s%07d" % i for i in range(N)], type=pa.binary())
i64 = pa.array([i * 1000003 for i in range(N)], type=pa.int64())
i32 = pa.array([i * 7 for i in range(N)], type=pa.int32())
f64 = pa.array([i * 0.5 for i in range(N)], type=pa.float64())
sn = pa.array([None if i % 5 == 0 else b"n%07d" % i for i in range(N)],
              type=pa.binary())
table = pa.table({"s": s, "i64": i64, "i32": i32, "f64": f64, "sn": sn})

out = "dict_fallback.parquet"
pq.write_table(
    table, out,
    compression="snappy",
    use_dictionary=True,
    dictionary_pagesize_limit=4096,   # overflow after ~first pages
    data_page_size=4096,              # several pages per side
    write_statistics=True,
)

pf = pq.ParquetFile(out)
for rg in range(pf.metadata.num_row_groups):
    for c in range(pf.metadata.num_columns):
        col = pf.metadata.row_group(rg).column(c)
        encs = set(col.encodings)
        has_dict = bool(encs & {"PLAIN_DICTIONARY", "RLE_DICTIONARY"})
        assert has_dict and "PLAIN" in encs, \
            f"rg{rg} col {col.path_in_schema}: {sorted(encs)} — not mixed"
        print(f"rg{rg} {col.path_in_schema}: {sorted(encs)}")
print(f"wrote {out}: {pf.metadata.num_rows} rows, "
      f"{pf.metadata.num_row_groups} row groups")
