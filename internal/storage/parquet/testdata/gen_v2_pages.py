#!/usr/bin/env python3
"""Generate DATA PAGE V2 fixtures, nested and null-heavy, plus a split-row file.

Everything else in this directory is data page version 1.0 (pyarrow's default)
or is written by parquet-go from a struct whose every leaf is REQUIRED and
FLAT. That left three properties the reader now enforces with no legitimate
file to measure them against:

  * a v2 header's num_rows against its repetition levels (needs a NESTED v2
    page: a flat leaf has no repetition levels to disagree with);
  * a v2 header's num_nulls against its definition levels (needs an OPTIONAL
    v2 page: a required leaf has no definition levels at all);
  * the v2 level SECTION lengths against what the level decoder consumes
    (needs either, since both are zero on a required flat leaf).

`v2_nested.parquet` carries null lists, empty lists, null maps, null structs,
an all-null column and a some-null column, so the definition levels are not
degenerate. `v2_flat.parquet` pairs a REQUIRED leaf with an OPTIONAL one in
the same file.

Run: python3 gen_v2_pages.py   (writes beside this script)
"""
import os

import pyarrow as pa
import pyarrow.parquet as pq

here = os.path.dirname(os.path.abspath(__file__))
N = 4000

tags = []
for i in range(N):
    if i % 7 == 0:
        tags.append(None)        # null list
    elif i % 7 == 1:
        tags.append([])          # empty list
    else:
        tags.append([i * 10 + j for j in range(i % 3 + 1)])

nested = pa.table({
    "x": pa.array(list(range(N)), pa.int64()),
    "tags": pa.array(tags, pa.list_(pa.int64())),
    "props": pa.array([None if i % 11 == 0 else [(f"k{j}", i * 100 + j) for j in range(i % 2 + 1)]
                       for i in range(N)], pa.map_(pa.string(), pa.int64())),
    "rec": pa.array([{"a": i, "b": f"s{i}"} if i % 9 else None for i in range(N)],
                    pa.struct([("a", pa.int64()), ("b", pa.string())])),
    "allnull": pa.array([None] * N, pa.int64()),
    "somenull": pa.array([None if i % 3 == 0 else i for i in range(N)], pa.int64()),
})
p = os.path.join(here, "v2_nested.parquet")
pq.write_table(nested, p, compression="none", data_page_size=512,
               use_dictionary=False, write_statistics=True,
               version="2.6", data_page_version="2.0")
print("wrote", p, os.path.getsize(p))

flat = pa.table({
    "req": pa.array(list(range(N)), pa.int64()),
    "opt": pa.array([None if i % 4 == 0 else i for i in range(N)], pa.int64()),
    "s": pa.array([f"s{i % 53}" for i in range(N)], pa.string()),
}).cast(pa.schema([pa.field("req", pa.int64(), nullable=False),
                   pa.field("opt", pa.int64(), nullable=True),
                   pa.field("s", pa.string(), nullable=True)]))
p = os.path.join(here, "v2_flat.parquet")
pq.write_table(flat, p, compression="none", data_page_size=1024,
               use_dictionary=False, write_statistics=True,
               version="2.6", data_page_version="2.0")
print("wrote", p, os.path.getsize(p))

# Small twins of both, for the exhaustive header bit-flip sweep: that sweep
# re-reads the WHOLE file once per mutated bit, so its fixtures have to be
# small enough that pages x header bytes x 3 bits stays a second or two. The
# shapes are identical; only the row count differs.
M = 60
small_nested = nested.slice(0, M)
p = os.path.join(here, "v2_nested_small.parquet")
pq.write_table(small_nested, p, compression="none", data_page_size=256,
               use_dictionary=False, write_statistics=True,
               version="2.6", data_page_version="2.0")
print("wrote", p, os.path.getsize(p))

small_flat = flat.slice(0, M)
p = os.path.join(here, "v2_flat_small.parquet")
pq.write_table(small_flat, p, compression="none", data_page_size=256,
               use_dictionary=False, write_statistics=True,
               version="2.6", data_page_version="2.0")
print("wrote", p, os.path.getsize(p))
