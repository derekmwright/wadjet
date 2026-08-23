#!/usr/bin/env python3
"""Generate dict_empty_rowgroup.parquet: dictionary-encoded BYTE_ARRAY
columns, flat and nested, over three row groups, where the MIDDLE row group
holds no present value at all for any of them.

That is the shape that produces an EMPTY dictionary page. pyarrow writes a
dictionary page for every chunk of a dictionary-encoded column, and a chunk
whose every value in that row group is NULL gets one with zero entries. The
reader read the entry count off the BYTE_ARRAY offset table as len-1, and an
empty page has no offset table at all — len(nil)-1 is -1, which the
declared-vs-decoded check read as a short decode and refused:

    dictionary page declares 0 entries but decoded -1 as STRING (physical BYTE_ARRAY)

so the file could not be read at all. Nothing about it is nested or
positional: `s` below (a plain STRING column) fails in exactly the same way
as `m`'s value leaf.

Row groups are two rows each:
  rows 0-1  values present
  rows 2-3  every BYTE_ARRAY column NULL / empty  <- the empty dictionaries
  rows 4-5  values present

Regenerate with:
  python3 internal/storage/parquet/testdata/gen_dict_empty_rowgroup.py
"""
import pyarrow as pa
import pyarrow.parquet as pq

table = pa.table({
    "id": pa.array([0, 1, 2, 3, 4, 5], pa.int64()),

    # Flat dictionary-encoded STRING: all-NULL in row group 1.
    "s": pa.array(["alpha", "beta", None, None, "alpha", "gamma"], pa.string()),

    # MAP<STRING,STRING>: row group 1 has one entry with a NULL value and one
    # NULL map, so the VALUE leaf has no present value and the KEY leaf has
    # one — an empty dictionary for the value, a non-empty one for the key.
    "m": pa.array([
        [("k", "v")],
        [("a", "1"), ("b", "2")],
        [("k", None)],
        None,
        [("z", "9")],
        [],
    ], type=pa.map_(pa.string(), pa.string())),

    # LIST<STRING>: row group 1 is one empty list and one NULL list, so the
    # element leaf's dictionary is empty there too.
    "tags": pa.array([
        ["a", "b"],
        ["a"],
        [],
        None,
        ["c"],
        ["a", "c"],
    ], pa.list_(pa.string())),
})

OUT = "internal/storage/parquet/testdata/dict_empty_rowgroup.parquet"
pq.write_table(table, OUT, version="2.6", compression="none",
               use_dictionary=True, row_group_size=2)

pf = pq.ParquetFile(OUT)
print("row groups:", pf.num_row_groups)
for i in range(pf.num_row_groups):
    rg = pf.metadata.row_group(i)
    for c in range(rg.num_columns):
        cc = rg.column(c)
        print(f"  rg{i} {cc.path_in_schema}: values={cc.num_values} "
              f"nulls={cc.statistics.null_count if cc.statistics else '?'} "
              f"dict_offset={cc.dictionary_page_offset}")
print(pq.read_table(OUT).to_pydict())
