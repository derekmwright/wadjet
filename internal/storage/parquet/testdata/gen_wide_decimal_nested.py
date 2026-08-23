#!/usr/bin/env python3
"""Generate wide_decimal_nested.parquet: a DECIMAL(38,10) column sitting
beside a nested one, so a read that takes the whole row (SELECT *) goes down
the row reader (#393) — the path issue #419's truncation was reachable from.

PyArrow writes any precision above 18 as a 16-byte FIXED_LEN_BYTE_ARRAY. The
row reader accumulated all sixteen bytes into a single int64, shifting the
top eight straight out of the register, and returned the low 64 bits of the
unscaled value reinterpreted as signed — a different number, with no error,
for every value whose magnitude exceeds 2^63.

  d          decimal128(38,10)  -- widest, narrowest, small, negative, null
  unscaled   string             -- the exact unscaled integer, as text, so
                                   the Go test compares against PyArrow's own
                                   encoding rather than recomputing it
  d_nested   list<decimal128(38,10)>
                                -- the same values one container deep, which
                                   reaches the nested assembler's leaf decode
  tags       list<string>       -- a second nested column, so an UNPROJECTED
                                   read (SELECT *) has one whatever else the
                                   projection drops

A projection of `d` alone does NOT take the row path: the scan chooses by the
PROJECTED schema (scan.HasUnsupportedColumnarTypes over projectSchema's
output), and the reader by the columns the read asks for, so dropping the
nested columns from the projection sends it to the native columnar path.
What this file gives is the same DECIMAL(38,10) values reachable from BOTH
paths — natively when only `d` is projected, and through the row assembler
when a nested column comes along — which is the shape #419 was silently
wrong on and what ADR-0018 §3 requires be checked.

Regenerate with:
  python3 internal/storage/parquet/testdata/gen_wide_decimal_nested.py
"""
import decimal
import pyarrow as pa
import pyarrow.parquet as pq

decimal.getcontext().prec = 60

VALUES = [
    "9999999999999999999999999999.9999999999",
    "-9999999999999999999999999999.9999999999",
    "0.0000000001",
    "-0.0000000001",
    "1.0000000000",
    None,
]

d3810 = pa.decimal128(38, 10)


def unscaled(text):
    if text is None:
        return None
    return str(int((decimal.Decimal(text) * (10 ** 10)).to_integral_value()))


table = pa.table({
    "d": pa.array([None if v is None else decimal.Decimal(v) for v in VALUES], d3810),
    "unscaled": pa.array([unscaled(v) for v in VALUES], pa.string()),
    "d_nested": pa.array(
        [None if v is None else [decimal.Decimal(v)] for v in VALUES],
        pa.list_(d3810)),
    "tags": pa.array([["a"], ["b"], [], None, ["c", "d"], None], pa.list_(pa.string())),
})

OUT = "internal/storage/parquet/testdata/wide_decimal_nested.parquet"
pq.write_table(table, OUT, version="2.6", compression="none", use_dictionary=False)

md = pq.ParquetFile(OUT).metadata
for i in range(md.row_group(0).num_columns):
    c = md.row_group(0).column(i)
    print("   ", c.path_in_schema, c.physical_type, c.encodings)
print(pq.read_table(OUT).to_pydict())
