#!/usr/bin/env python3
"""Generate decimal_physicals.parquet: DECIMAL columns in all three physical
encodings parquet allows besides the INT64 our own writer emits, written by
the Apache reference implementation (PyArrow), so the Go decoder is checked
against something other than itself.

Parquet stores a DECIMAL logical type over INT32, INT64, BYTE_ARRAY or
FIXED_LEN_BYTE_ARRAY. TypeIDFromSchemaNode answers TypeDecimal for all four,
but the scan's decode arm assumed INT64 — the native path refused everything
else outright ("unsupported physical encoding"), and the row path's
dictionary resolver had no DECIMAL case at all, so a dictionary-encoded
DECIMAL fell through to the BYTE_ARRAY default and could not be read.

Columns (PyArrow writes DECIMAL(p,s) as the narrowest physical that fits when
store_decimal_as_integer=True):
  label   string          — row name, for failure messages
  d9_2    decimal128(9,2)  -> INT32                 (4 bytes)
  d18_4   decimal128(18,4) -> INT64                 (8 bytes)
  d38_10  decimal128(38,10)-> FIXED_LEN_BYTE_ARRAY  (16 bytes)
  unscaled_9_2 / unscaled_18_4 / unscaled_38_10 int64/string
          — the exact UNSCALED integer each decimal encodes, so the Go test
            compares against the reference writer's own encoding rather than
            recomputing it from a float. The 38,10 column is a string
            because its unscaled value does not fit an int64.

Regenerate with:
  python3 internal/storage/parquet/testdata/gen_decimal_physicals.py
"""
import decimal
import pyarrow as pa
import pyarrow.parquet as pq

decimal.getcontext().prec = 60

ROWS = [
    ("zero", "0.00", "0.0000", "0.0000000000"),
    ("one", "1.00", "1.0000", "1.0000000000"),
    ("neg_one", "-1.00", "-1.0000", "-1.0000000000"),
    ("small", "3.25", "3.2500", "3.2500000000"),
    ("neg_small", "-12.34", "-12.3400", "-12.3400000000"),
    # Widest value each precision can hold, and its negation: these are the
    # sign-extension edges the big-endian two's complement decode gets wrong
    # first.
    ("max", "9999999.99", "99999999999999.9999", "9999999999999999999999999999.9999999999"),
    ("min", "-9999999.99", "-99999999999999.9999", "-9999999999999999999999999999.9999999999"),
    ("null", None, None, None),
]

d92 = pa.decimal128(9, 2)
d184 = pa.decimal128(18, 4)
d3810 = pa.decimal128(38, 10)


def unscaled(text, scale):
    if text is None:
        return None
    return int((decimal.Decimal(text) * (10 ** scale)).to_integral_value())


table = pa.table({
    "label": pa.array([r[0] for r in ROWS], pa.string()),
    "d9_2": pa.array([None if r[1] is None else decimal.Decimal(r[1]) for r in ROWS], d92),
    "d18_4": pa.array([None if r[2] is None else decimal.Decimal(r[2]) for r in ROWS], d184),
    "d38_10": pa.array([None if r[3] is None else decimal.Decimal(r[3]) for r in ROWS], d3810),
    "unscaled_9_2": pa.array([unscaled(r[1], 2) for r in ROWS], pa.int64()),
    "unscaled_18_4": pa.array([unscaled(r[2], 4) for r in ROWS], pa.int64()),
    "unscaled_38_10": pa.array(
        [None if r[3] is None else str(unscaled(r[3], 10)) for r in ROWS], pa.string()),
})

pq.write_table(
    table,
    "internal/storage/parquet/testdata/decimal_physicals.parquet",
    version="2.6",
    compression="snappy",
    store_decimal_as_integer=True,
    # PLAIN only: the dictionary arm is covered by a separate file so a
    # failure names which decode path broke.
    use_dictionary=False,
)

pq.write_table(
    table,
    "internal/storage/parquet/testdata/decimal_physicals_dict.parquet",
    version="2.6",
    compression="snappy",
    store_decimal_as_integer=True,
    use_dictionary=True,
)

for path in ("internal/storage/parquet/testdata/decimal_physicals.parquet",
             "internal/storage/parquet/testdata/decimal_physicals_dict.parquet"):
    md = pq.ParquetFile(path).metadata
    print(path)
    for i in range(md.row_group(0).num_columns):
        c = md.row_group(0).column(i)
        print("   ", c.path_in_schema, c.physical_type, c.encodings)
