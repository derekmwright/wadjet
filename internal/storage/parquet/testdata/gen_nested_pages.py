#!/usr/bin/env python3
"""Generate nested_pages.parquet: a NESTED leaf spread over MANY data pages.

The column-completeness gate cuts a column chunk at each page boundary and
asserts the reader refuses a chunk that ends before its declared rows. For a
nested leaf that needs a chunk with several pages, and wadjet's own writer
emits one page per nested leaf per row group — so the multi-page nested shape
has to come from another writer. PyArrow splits on data_page_size.

A repeated leaf also has more VALUES than rows (7999 elements over 4000 rows
here), which is the point: the reconciliation for a nested column counts
repetition-level-0 entries, not values.

Run: python3 gen_nested_pages.py   (writes nested_pages.parquet beside this script)
"""
import os

import pyarrow as pa
import pyarrow.parquet as pq

N = 4000
tbl = pa.table({
    "x": pa.array(list(range(N)), pa.int64()),
    "tags": pa.array([[i * 10 + j for j in range(i % 3 + 1)] for i in range(N)],
                     pa.list_(pa.int64())),
})

out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "nested_pages.parquet")
pq.write_table(
    tbl,
    out,
    compression="none",
    data_page_size=512,
    use_dictionary=False,
    write_statistics=True,
    version="2.6",
    data_page_version="1.0",
)
print("wrote", out, os.path.getsize(out), "bytes")
