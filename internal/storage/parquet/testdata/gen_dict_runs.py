#!/usr/bin/env python3
"""Generate dict_runs.parquet: PURE dictionary-encoded chunks whose index
streams span the whole run spectrum, from one RLE run per row group to
fully bit-packed. This is the fixture for run-granularity predicate
evaluation (scan/row_filter.go) — the run path and the expanding path must
agree cell-for-cell on every one of these shapes.

A large dictionary_pagesize_limit and low column cardinality keep every
data page dictionary-encoded (no PLAIN fallback); the Go test asserts that
before it tests anything, since a fallback chunk would silently move the
coverage to the wrong path.

Values are pure functions of the row index so the Go test can compute the
expected selection independently of the reader:

  flat   (int32):  rg_index                    -> ONE run per row group
  runs   (int32):  i // 500                    -> long runs, sorted
  short  (int32):  (i // 3) % 97               -> short runs
  packed (int32):  (i * 2654435761) % 4001     -> scattered, bit-packed
  s_runs (binary): b"cat%03d" % (i // 800)     -> long runs, strings
  f_runs (double): float(i // 400)             -> long runs, doubles
  opt    (int32):  i // 500, column OPTIONAL   -> long runs, definition
                                                  levels present, zero nulls
  nulls  (int32):  NULL when i%7==0 else i//500 -> run structure + nulls

`opt` is the shape real ClickBench hits parts have: every column is
declared OPTIONAL, so definition levels are written even for columns that
contain no null at all. Gating the run path on "no definition levels"
instead of "no nulls" would silently miss every real hits column.

Run from this directory:  python3 gen_dict_runs.py
"""
import pyarrow as pa
import pyarrow.parquet as pq

RG = 20000
NRG = 3
N = RG * NRG

flat = pa.array([i // RG for i in range(N)], type=pa.int32())
runs = pa.array([i // 500 for i in range(N)], type=pa.int32())
short = pa.array([(i // 3) % 97 for i in range(N)], type=pa.int32())
packed = pa.array([(i * 2654435761) % 4001 for i in range(N)], type=pa.int32())
s_runs = pa.array([b"cat%03d" % (i // 800) for i in range(N)], type=pa.binary())
f_runs = pa.array([float(i // 400) for i in range(N)], type=pa.float64())
opt = pa.array([i // 500 for i in range(N)], type=pa.int32())
nulls = pa.array([None if i % 7 == 0 else i // 500 for i in range(N)],
                 type=pa.int32())

# Everything but `nulls` is REQUIRED. That matters: with no definition
# levels the dictionary indices are one-per-row, which is what lets the
# scan filter evaluate a run as a row span. It is also the shape the real
# ClickBench hits columns have. `nulls` stays OPTIONAL so the null path is
# covered from the same file.
schema = pa.schema([
    pa.field("flat", pa.int32(), nullable=False),
    pa.field("runs", pa.int32(), nullable=False),
    pa.field("short", pa.int32(), nullable=False),
    pa.field("packed", pa.int32(), nullable=False),
    pa.field("s_runs", pa.binary(), nullable=False),
    pa.field("f_runs", pa.float64(), nullable=False),
    pa.field("opt", pa.int32(), nullable=True),
    pa.field("nulls", pa.int32(), nullable=True),
])
table = pa.table(
    [flat, runs, short, packed, s_runs, f_runs, opt, nulls], schema=schema)

out = "dict_runs.parquet"
pq.write_table(
    table, out,
    compression="snappy",
    use_dictionary=True,
    dictionary_pagesize_limit=1 << 20,  # never overflow: chunks stay pure
    data_page_size=1 << 20,             # few, large pages
    row_group_size=RG,
    write_statistics=True,
)

pf = pq.ParquetFile(out)
assert pf.metadata.num_row_groups == NRG, pf.metadata.num_row_groups
for rg in range(pf.metadata.num_row_groups):
    for c in range(pf.metadata.num_columns):
        col = pf.metadata.row_group(rg).column(c)
        encs = set(col.encodings)
        assert encs & {"PLAIN_DICTIONARY", "RLE_DICTIONARY"}, \
            f"rg{rg} {col.path_in_schema}: {sorted(encs)} — not dictionary"
        print(f"rg{rg} {col.path_in_schema}: {sorted(encs)}")
print(f"wrote {out}: {pf.metadata.num_rows} rows, "
      f"{pf.metadata.num_row_groups} row groups")
