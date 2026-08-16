#!/usr/bin/env python3
"""Emit per-column checksums of a ClickBench hits part via pyarrow, for
value-exact comparison against wadjet's parquet reader (hits_sig_test.go).
Depends only on pyarrow.

Signature per column (all arithmetic mod 2^64, row index i is file order):
  ints:   weighted = sum over non-null i of (i+1) * uint64(int64(value))
  binary: weighted = sum over non-null i of (i+1) * crc32(value)
  plus:   nonnull count, nullpos = sum over null i of (i+1)

The position weighting makes the sum sensitive to value placement, so a
shifted/permuted/mis-nulled decode cannot match.

Usage: python3 gen_hits_sig.py <hits_part.parquet> <out.json>
"""
import json
import sys
import zlib

import pyarrow as pa
import pyarrow.parquet as pq

MASK = (1 << 64) - 1


def sig_column(col: pa.ChunkedArray) -> dict:
    weighted = 0
    nullpos = 0
    nonnull = 0
    is_int = pa.types.is_integer(col.type)
    if not is_int and not (pa.types.is_binary(col.type)
                           or pa.types.is_string(col.type)):
        raise SystemExit(f"unhandled type {col.type}")
    i = 0
    for chunk in col.chunks:
        for v in chunk.to_pylist():
            i += 1
            if v is None:
                nullpos += i
                continue
            nonnull += 1
            if is_int:
                weighted += i * (v & MASK)
            else:
                if isinstance(v, str):
                    v = v.encode()
                weighted += i * zlib.crc32(v)
    return {"weighted": weighted & MASK, "nonnull": nonnull,
            "nullpos": nullpos & MASK}


def main():
    path, out = sys.argv[1], sys.argv[2]
    table = pq.read_table(path)
    sigs = {}
    for name in table.column_names:
        sigs[name] = sig_column(table.column(name))
        print(f"{name}: {sigs[name]}", flush=True)
    with open(out, "w") as f:
        json.dump({"rows": table.num_rows, "columns": sigs}, f, indent=1)
    print(f"wrote {out}: {len(sigs)} columns, {table.num_rows} rows")


if __name__ == "__main__":
    main()
