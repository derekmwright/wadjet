#!/usr/bin/env python3
"""Generate timestamp_precision.parquet: the SAME instants written at all
three parquet TIMESTAMP precisions, by the Apache reference implementation
(PyArrow), so the Go reader can be checked against something other than our
own writer.

Wadjet's engine unit for TypeTimestamp is epoch MILLISECONDS (that is what
file_writer.go emits and what the expression layer's parseTemporalInt64
assumes). Parquet files written elsewhere routinely carry MICROS (PyArrow's
and Spark's default) or NANOS. Reading those without scaling puts every
instant off by 1000x or 1000000x, silently.

Columns
  label      string   — row name, for failure messages
  expect_ms  int64    — the epoch-millisecond value the engine must decode
  ts_millis  timestamp[ms]         -> TIMESTAMP(isAdjustedToUTC, MILLIS)
  ts_micros  timestamp[us]         -> TIMESTAMP(isAdjustedToUTC, MICROS)
  ts_nanos   timestamp[ns]         -> TIMESTAMP(isAdjustedToUTC, NANOS)
  ts_us_utc  timestamp[us, tz=UTC] -> TIMESTAMP(isAdjustedToUTC=true, MICROS)
  ts_ms_null timestamp[ms]         -> nullable, exercises def levels
  ts_us_null timestamp[us]         -> nullable, scaling must skip null slots

expect_ms is written by PyArrow from the same Python integers that produce
the timestamp columns, so the Go test compares reader output against the
reference writer's own encoding rather than recomputing it.

Every instant here is a WHOLE number of milliseconds, so all four timestamp
columns must decode to exactly expect_ms. Sub-millisecond truncation is a
separate fixture (timestamp_subms.parquet) because a millis column cannot
represent those inputs at all.

Nanosecond range note: int64 nanoseconds only span 1677-09-21 .. 2262-04-11,
so every instant stays inside that window — a wider one would not round-trip
through ts_nanos in ANY implementation.

Regenerate:  python3 gen_timestamp_precision.py
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq

# (label, epoch milliseconds). Whole milliseconds only — see docstring.
ROWS = [
    ("epoch", 0),
    # The value from issue #321's report.
    ("issue_321", 826727136000),
    ("modern_with_millis", 1755000000123),
    ("one_ms_after_epoch", 1),
    ("one_ms_before_epoch", -1),
    # Pre-1970: the sign path through every scale factor.
    ("apollo_11", -14182940000),
    ("pre_epoch_with_millis", -14182939877),
    # Near the edges of what int64 nanoseconds can hold.
    ("near_ns_min", -9214000000000),
    ("near_ns_max", 9214000000000),
]

LABELS = [r[0] for r in ROWS]
MS = [r[1] for r in ROWS]

# Nullable columns: every third row is NULL. Scaling must leave null slots
# alone and must not shift the values around them.
MS_NULL = [None if i % 3 == 0 else v for i, v in enumerate(MS)]


def ts(values, unit, tz=None):
    """Build a timestamp array from epoch-millisecond integers."""
    per_ms = {"ms": 1, "us": 1_000, "ns": 1_000_000}[unit]
    raw = [None if v is None else v * per_ms for v in values]
    return pa.array(raw, type=pa.timestamp(unit, tz=tz))


def main():
    table = pa.table(
        {
            "label": pa.array(LABELS, type=pa.string()),
            "expect_ms": pa.array(MS, type=pa.int64()),
            "ts_millis": ts(MS, "ms"),
            "ts_micros": ts(MS, "us"),
            "ts_nanos": ts(MS, "ns"),
            "ts_us_utc": ts(MS, "us", tz="UTC"),
            "ts_ms_null": ts(MS_NULL, "ms"),
            "ts_us_null": ts(MS_NULL, "us"),
        }
    )

    # version='2.6' is required for NANOS: the 1.0 writer coerces nanosecond
    # columns down to micros, which would quietly delete the case this
    # fixture exists to cover.
    pq.write_table(
        table,
        "timestamp_precision.parquet",
        version="2.6",
        compression="none",
        use_dictionary=False,
        store_schema=False,
    )

    # ---- sub-millisecond truncation fixture ----
    # Instants that are NOT a whole millisecond, so micros/nanos carry
    # precision the engine unit cannot hold. Wadjet truncates toward the
    # PAST (floor), i.e. an instant is reported as the millisecond that
    # CONTAINS it; that is the only rule that behaves the same on both
    # sides of the epoch.
    sub_us = [
        ("half_ms_after_epoch", 500, 0),
        ("half_ms_before_epoch", -500, -1),
        ("one_us_before_epoch", -1, -1),
        ("one_us_after_epoch", 1, 0),
        ("neg_1500us", -1500, -2),
        ("pos_1500us", 1500, 1),
        ("modern_odd_us", 1755000000123456, 1755000000123),
        ("pre_epoch_odd_us", -14182939876543, -14182939877),
    ]
    sub_table = pa.table(
        {
            "label": pa.array([r[0] for r in sub_us], type=pa.string()),
            "raw_us": pa.array([r[1] for r in sub_us], type=pa.int64()),
            "expect_ms": pa.array([r[2] for r in sub_us], type=pa.int64()),
            "ts_micros": pa.array(
                [r[1] for r in sub_us], type=pa.timestamp("us")
            ),
            "ts_nanos": pa.array(
                [r[1] * 1000 for r in sub_us], type=pa.timestamp("ns")
            ),
        }
    )
    pq.write_table(
        sub_table,
        "timestamp_subms.parquet",
        version="2.6",
        compression="none",
        use_dictionary=False,
        store_schema=False,
    )

    # ---- row-group pruning fixture ----
    # A MICROS column whose RAW values (~1.75e15) live nowhere near the
    # engine-millisecond domain (~1.75e12) the query predicate is written
    # in. Unscaled footer statistics therefore exclude every millis-domain
    # literal, and zone-map pruning drops row groups that really do contain
    # matching rows — silent row loss with no error anywhere.
    #
    # Three row groups of 100 rows, one millisecond apart, so a predicate
    # can select a known slice and the test can name exactly which rows
    # must come back.
    base_ms = 1755000000000  # 2025-08-12T12:40:00Z
    n = 300
    prune_ms = [base_ms + i for i in range(n)]
    # ts_dict is deliberately LOW cardinality (10 distinct instants per row
    # group) so the writer keeps the chunk purely dictionary-encoded. That is
    # what the dictionary-probe pruning path requires: it reads the raw
    # dictionary page and tests an engine-unit literal for exact membership.
    dict_ms = [base_ms + (i % 10) for i in range(n)]
    prune_table = pa.table(
        {
            "row_id": pa.array(list(range(n)), type=pa.int64()),
            "expect_ms": pa.array(prune_ms, type=pa.int64()),
            "ts_micros": ts(prune_ms, "us"),
            "ts_nanos": ts(prune_ms, "ns"),
            "ts_millis": ts(prune_ms, "ms"),
            "ts_dict": ts(dict_ms, "us"),
            # Same shape at MILLIS: the control that proves declining the
            # foreign-unit probe did not disable dictionary pruning itself.
            "ts_dict_ms": ts(dict_ms, "ms"),
            "expect_dict_ms": pa.array(dict_ms, type=pa.int64()),
        }
    )
    pq.write_table(
        prune_table,
        "timestamp_prune.parquet",
        version="2.6",
        compression="none",
        use_dictionary=["ts_dict", "ts_dict_ms"],
        row_group_size=100,
        write_statistics=True,
    )

    print(
        json.dumps(
            {"rows": len(ROWS), "subms_rows": len(sub_us), "prune_rows": n}
        )
    )


if __name__ == "__main__":
    main()
