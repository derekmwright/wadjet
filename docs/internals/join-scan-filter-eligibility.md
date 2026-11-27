# Join scan-filter eligibility

Source: internal/planner/physical/scan_filter_eligibility.go — scanFilterTarget (#1677 line q)

When a single-process hash join finishes its build, the planner can hand the
probe side's catalog scan two row-group filters derived from the build keys:

- the **dynamic min/max range** (`HashJoin.BuildKeyRange`, INNER / SEMI /
  RIGHT joins, every key type the build records a range for), and
- the **bloom scan filter** (`BloomFilterOp.BloomScanFilter`, a single
  integer-family key).

Both prune a row group by comparing the build's key values with the STORED
column's statistics. So they are right only when the probe key the join
compares IS that stored value.

## The rule

A build-key scan filter is installed only when `scanFilterTarget` proves,
hop by hop from the join's probe down to the scan, that the key is the
unmodified stored column. Anything it cannot prove gets no filter. The hops it
proves:

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

`exec.ForceSerialJoinBuildForTest` puts the build on its one-CPU routes, so
many-core CI exercises the range filter
(`wadjet/arc_1677q_scan_filter_eligibility_test.go`).

## Not covered here

The DAG's own dynamic filters (`dagplan.applyDynamicFilters`, opt-in through
`WADJET_DYNAMIC_FILTERS`) and the probe-sourced semi/anti build filters
(`dagplan.markSemiAntiBuildFilters`) are planned by a different pass, with its
own leaf-scan walk (`findLeafScanStage`). This guard does not cover them.
