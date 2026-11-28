# Join scan-filter eligibility

Source: internal/planner/physical/scan_filter_eligibility.go — scanFilterTarget, storedTypeCarries (#1677 lines q and r)

When a single-process hash join finishes its build, the planner can hand the
probe side's catalog scan two row-group filters derived from the build keys:

- the **dynamic min/max range** (`HashJoin.BuildKeyRange`, INNER / SEMI /
  RIGHT joins, every key type the build records a range for), and
- the **bloom scan filter** (`BloomFilterOp.BloomScanFilter`, a single
  integer-family key).

Both prune a row group by comparing the build's key values with the STORED
column's statistics. So they are right only when the probe key the join
compares IS that stored value, and the filter's values are in that stored
column's type.

## The rule

A build-key scan filter is installed only when `scanFilterTarget` proves BOTH:

1. **the key** (line q): hop by hop from the join's probe down to the scan,
   the key is the unmodified stored column; and
2. **the type** (line r): the filter's values are in exactly the stored
   column's declared type and representation, with no promotion on either side
   of the join key.

Anything it cannot prove gets no filter.

### The key

The hops it proves:

- a row filter (the Filter family, a bloom pre-check). A row-filter policy is
  one of these, so it stays eligible.
- a lower hash join's probe (`HashJoinProbe.ForwardsProbeColumn`): an INNER,
  LEFT, SEMI or ANTI join, where the key comes from the probe side and the
  build side has no column of the same bare name.
- a projection that passes the column through under its own name, as a plain
  copy, and is the only output of that name.
- a `pipelineSource`, whose own operators are walked by the same rules.

The scan must read the key as a stored column. It also must not be the scan
that fills the multi-consumer scan cache: a filter there would prune the rows
every other consumer of the cache replays. When the cache is already filled,
the filter is a no-op anyway.

Everything else is unproved, and the guard fails closed on it: a policy mask,
a computed, cast or renamed projection, a set operation, an aggregate, a
window, a LIMIT, a lateral, or a source that is not a base-table scan. A lost
pruning opportunity is acceptable. A wrong prune is a silently wrong answer.

The in-pipeline bloom pre-check (`BloomFilterOp`, appended just below the
probe) is not affected. It reads the key as the join reads it, above any mask
or projection, so it compares like with like.

### The type

The build records its min/max range off the build vector, in the build
column's own type and units (`HashJoin.updateKeyMinMax`), and fills its bloom
with the keys at the pair's resolved common type (`HashJoin.KeyTypes`). A join
whose key types differ compares them at that common type, so a range or bloom
read against the probe's stored statistics is in the wrong units.
`HashJoin.ScanFilterValueTypes` answers, per key, the build column's type, or
"unresolved" when the pair is keyed at a different common type or the build
column was never recorded; it is read before `PruneBuildColumns` can drop the
build schema. A column two keys name keeps a type only when both carry it.

`storedTypeCarries` then requires, on the scan:

- the column is declared once in the table's schema (`catalogScanSource.storedCols`,
  the read schema the scan's values and its reconciled statistics share);
- its declared type IS the filter's value type; and
- that type is one whose representation the type ID alone fixes and has been
  measured to agree between the build's values and the statistics: INT32,
  DATE, PORT, PROTOCOL (int32); INT64, TIMESTAMP (milliseconds; the reader
  normalizes MICROS and NANOS files), IPv4, MAC (int64); FLOAT64; STRING.

Not listed, so no filter: DECIMAL (the statistics are unscaled integers at the
column's own scale), CIDR (its confirmed bounds are their own domain),
DURATION (its statistics were not measured), the containers, and the types no
build filter is recorded for (BOOL, FLOAT32, BYTES, IPv6, UUID).

Every promotion a join key accepts therefore gets no filter: DATE against
TIMESTAMP (both ways), INTEGER against BIGINT, an integer or DOUBLE against
NUMERIC, an integer against DOUBLE, REAL against DOUBLE, NUMERIC at two
scales. The filter is not converted to the stored type: that would keep the
pruning, and is a possible later optimization.

## Why

Before the guard, the attachment walked through any `pipelineSource` to the
scan and ignored every operator in between. Two kinds of statement went wrong:

- `a.ssn IN (SELECT ssn …)` under a mask: the masked build keys were compared
  with the stored ssn's statistics.
- `(SELECT y AS x FROM t) q WHERE q.x IN (…)`: t was pruned by its stored x.

In both, every row group was pruned and the answer was 0. A min/max range is
recorded only by the serial key-only build, so the string cases went wrong on
one CPU alone. The integer and date cases went wrong on every host, through
the bloom scan filter.

With the key proved but not the type (line q alone), a stored DATE probed by
TIMESTAMP build keys (`a.d IN (SELECT ts …)`, compared at the join as the
date's midnight) was ranged by the build's milliseconds against the stored day
counts, and the reverse; a stored NUMERIC(10,2) probed by integer or DOUBLE
build keys was ranged by 3 against its stored unscaled 300. Every row group
was pruned and the answer was 0, on one CPU, on the single-process, spilled,
fast-path and asynchronous doors.

`exec.ForceSerialJoinBuildForTest` puts the build on its one-CPU routes, so
many-core CI exercises the range filter
(`wadjet/arc_1677q_scan_filter_eligibility_test.go`,
`wadjet/arc_1677r_scan_filter_key_types_test.go`, and on every door
`internal/coordinator/arc_1677r_scan_filter_key_types_arms_test.go`).

## Not covered here

The DAG's own dynamic filters (`dagplan.applyDynamicFilters`, opt-in through
`WADJET_DYNAMIC_FILTERS`) and the probe-sourced semi/anti build filters
(`dagplan.markSemiAntiBuildFilters`) are planned by a different pass, with its
own leaf-scan walk (`findLeafScanStage`). This guard does not cover them. Each
has its own type check: `applyDynamicFilters` requires both leaf tables'
catalog types to be equal and one of INT32, INT64, DATE
(`StagePlanner.columnIntType`); `markSemiAntiBuildFilters` requires the same of
the probe stage when it names a table, and otherwise relies on the emit op's
runtime integer-carrier guard (`exec.dynamicFilterIntKey`).
