#!/usr/bin/env python3
"""Generate nested_containers.parquet: every nesting of ARRAY / ROW / MAP the
catalog's type grammar can express, written by the Apache reference
implementation (PyArrow), so the Go assembler is checked against something
other than itself.

Issue #409: the reader resolved a nested column's leaves by a FIXED-DEPTH
path, so every MAP that was not a top-level column of leaf-typed values read
back absent or wrong, silently:

  ROW(a INT64, m MAP(STRING,INT64))  ->  {a:5}          -- m dropped
  MAP(STRING, ARRAY(INT64))          ->  column absent
  MAP(STRING, ROW(x INT64))          ->  column absent
  ARRAY(MAP(STRING,INT64))           ->  ["k"]          -- the array of KEYS

Columns, all nullable, with NULL and empty containers in every position:

  id       int64                                  -- row label
  m_int    map<string,int64>                      -- baseline (already worked)
  m_list   map<string,list<int64>>
  m_struct map<string,struct<x:int64,y:string>>
  m_map    map<string,map<string,int64>>
  r_map    struct<a:int64, m:map<string,int64>>
  r_arr    struct<a:int64, l:list<int64>>
  r_row    struct<a:int64, s:struct<b:int64>>
  a_map    list<map<string,int64>>
  a_row    list<struct<x:int64>>
  a_arr    list<list<int64>>

Regenerate with:
  python3 internal/storage/parquet/testdata/gen_nested_containers.py
"""
import pyarrow as pa
import pyarrow.parquet as pq

ty_m_int = pa.map_(pa.string(), pa.int64())
ty_m_list = pa.map_(pa.string(), pa.list_(pa.int64()))
ty_m_struct = pa.map_(pa.string(), pa.struct([("x", pa.int64()), ("y", pa.string())]))
ty_m_map = pa.map_(pa.string(), pa.map_(pa.string(), pa.int64()))
ty_r_map = pa.struct([("a", pa.int64()), ("m", pa.map_(pa.string(), pa.int64()))])
ty_r_arr = pa.struct([("a", pa.int64()), ("l", pa.list_(pa.int64()))])
ty_r_row = pa.struct([("a", pa.int64()), ("s", pa.struct([("b", pa.int64())]))])
ty_a_map = pa.list_(pa.map_(pa.string(), pa.int64()))
ty_a_row = pa.list_(pa.struct([("x", pa.int64())]))
ty_a_arr = pa.list_(pa.list_(pa.int64()))

# Five rows: 0 = ordinary values, 1 = empty containers, 2 = NULLs one level
# in, 3 = the whole column NULL, 4 = multiple entries / elements.
table = pa.table({
    "id": pa.array([0, 1, 2, 3, 4], pa.int64()),

    "m_int": pa.array([
        [("k", 9)],
        [],
        [("k", None)],
        None,
        [("a", 1), ("b", 2)],
    ], type=ty_m_int),

    "m_list": pa.array([
        [("k", [1, 2])],
        [],
        [("k", None)],
        None,
        [("a", []), ("b", [3, None, 5])],
    ], type=ty_m_list),

    "m_struct": pa.array([
        [("k", {"x": 3, "y": "three"})],
        [],
        [("k", None)],
        None,
        [("a", {"x": None, "y": "a"}), ("b", {"x": 7, "y": None})],
    ], type=ty_m_struct),

    "m_map": pa.array([
        [("k", [("inner", 11)])],
        [],
        [("k", None)],
        None,
        [("a", []), ("b", [("p", 1), ("q", None)])],
    ], type=ty_m_map),

    "r_map": pa.array([
        {"a": 5, "m": [("k", 9)]},
        {"a": 6, "m": []},
        {"a": None, "m": None},
        None,
        {"a": 8, "m": [("p", 1), ("q", None)]},
    ], type=ty_r_map),

    "r_arr": pa.array([
        {"a": 5, "l": [1, 2]},
        {"a": 6, "l": []},
        {"a": None, "l": None},
        None,
        {"a": 8, "l": [3, None]},
    ], type=ty_r_arr),

    "r_row": pa.array([
        {"a": 5, "s": {"b": 9}},
        {"a": 6, "s": {"b": None}},
        {"a": None, "s": None},
        None,
        {"a": 8, "s": {"b": 4}},
    ], type=ty_r_row),

    "a_map": pa.array([
        [[("k", 1)]],
        [],
        [None],
        None,
        [[("a", 1)], [], [("b", None), ("c", 3)]],
    ], type=ty_a_map),

    "a_row": pa.array([
        [{"x": 1}],
        [],
        [None],
        None,
        [{"x": 2}, {"x": None}],
    ], type=ty_a_row),

    "a_arr": pa.array([
        [[1, 2]],
        [],
        [None],
        None,
        [[3], [], [None, 5]],
    ], type=ty_a_arr),
})

OUT = "internal/storage/parquet/testdata/nested_containers.parquet"
pq.write_table(table, OUT, version="2.6", compression="none", use_dictionary=False)

pf = pq.ParquetFile(OUT)
print(pf.schema)
print(pq.read_table(OUT).to_pydict())
