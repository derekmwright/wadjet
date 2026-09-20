# ADR-0040: A resolved setting belongs to the instance that uses it, and rides the task to a process that re-plans

Status: Accepted (2026-09-19, #1223, arc BJ)

Related: ADR-0029 (which tier wins when the value is RESOLVED — this ADR is
what happens to it afterwards), ADR-0024 (the planner rules the option
selects), ADR-0037 (the option crosses the MIT/AGPL seam as a member of
`physical.Planner`, not as a package variable either side can write).

## Context

`logical.BushyJoinReorder` was a package `atomic.Bool`. `wadjet.Open` stored
`true` when the caller asked for the bushy regime and nothing ever stored
`false`, so the FIRST database in a process that asked for it turned it on for
every other `wadjet.DB` in that process, and for the rest of the process's
life. Closing the database that asked did not take the setting with it.

Two `wadjet.DB`s in one process is not an exotic shape — it is the embedded
API's own contract, and it is what a host application with a tenant per
database has. The wrong instance planned a different join ORDER for the same
statement, which is a plan nobody configured, silently.

The same value had a second scope problem one layer out. A worker that
re-plans a whole query from `Task.SQLText` (`worker.Executor.executePipeline`
is the one path that does) ran the optimizer under the WORKER process's
setting, while the coordinator had already chosen that task's probe split and
build sides from a plan made under ITS setting. Two processes planning one
statement under two settings divide a relation the other one does not probe.

ADR-0029 settled which TIER a value comes from and that it is resolved once.
It says nothing about where the resolved value then lives, and a package
variable is where it lived.

## Decision

**A resolved setting belongs to the instance that uses it. It travels as a
value on that instance, never as package state.**

1. **The carrier is a struct the instance owns.** `logical.Options` holds the
   optimizer's settings and `logical.OptimizeWith` takes it; `physical.Planner`
   holds the planner's, beside `LateMaterialization`, and
   `Planner.LogicalOptions()` is the single accessor that converts one to the
   other. Every production call site that optimizes a plan passes the options
   of the instance it is planning for — `wadjet.DB`, `server.Config`,
   `coordinator.Config` — and `logical.BushyJoinReorder` is deleted. A future
   second setting is a field on `Options`, which is why it is a struct and not
   a bool.

2. **Every database a process opens carries the process's resolved
   configuration.** A `wadjet.Config` literal behind a command is not a
   different kind of database from the one behind `serve`: the CLI's shared
   catalog database, its catalog-free `query` path and the standalone server's
   pgwire fallback database each take the resolved planner and engine flags
   (#1226). The field nobody sets is the ZERO value, so an omitted flag is not
   "the default" — it is `false` or `0`, which for `--late-materialization`
   meant a documented default of `true` running as `false` on five commands.

3. **Where two PROCESSES plan one statement, the value rides the task.** The
   coordinator's setting is stamped onto every task carrying `SQLText`
   (`distributed.Task.BushyJoinReorder`, stamped in
   `Scheduler.PublishTasks` — the one marshal-and-publish choke point all
   seventeen dispatchers go through, which already holds the SQL-text policy
   guard for the same reason). The worker assigns it unconditionally and has
   no setting of its own to disagree with. Absent on the wire means off, which
   is both the shipped default and what a worker predating the field reads, so
   a mixed-version cluster plans the shipped default rather than something
   neither side chose.

4. **The exception, named:** an env-only kill switch is process-wide BY
   DESIGN. `internal/optswitch`'s toggles, `logical.ScalarAggSemijoin` and
   `physical.semiAntiNE` are set from the environment at `init` and have no
   `Config` path; they are a deployment-wide statement about which
   optimization is allowed to exist, not a per-database setting. A COUNTER is
   not configuration either: `logical.BushyJoinsPlanned` and the physical
   planner's other mechanism markers stay package-scope, because what they
   count is the process's work.

## Alternatives rejected

- **Keep the package variable and store `false` on every `Open`.** Makes the
  LAST database to open win instead of the first — still one setting for the
  process, still not released on `Close`, and now racy between two `Open`s.
- **Give the worker its own flag.** The worker would then be configured to
  disagree with the coordinator that planned the task, which is the defect
  with a knob on it.
- **Let each AGPL caller build a `logical.Options` by hand from the field it
  already holds.** That is a COPY of the planner's configuration on the far
  side of the license seam: when `Options` grows a second field every
  hand-built copy silently drops it, and the second plan of a statement runs
  under settings the first did not have. One exported accessor is the smaller
  cost (the licensecheck member budget, 214 → 216).

## Gates

- `wadjet.TestPlannerConfigIsInstanceScoped` — two databases with opposite
  settings, both `Open` orders, interleaved queries, asserting the bushy arm's
  `BushyJoinsPlanned` delta BEFORE the default arm's zero so the gate cannot
  pass by the enumeration never firing; and
  `wadjet.TestCloseThenOpenRestoresTheDefault`. Both fail at `6960cd27`, where
  the default instance planned a bushy join in all three cells.
- `coordinator.TestTwoCoordinatorsPlanByTheirOwnBushyOption` — the same
  property for two coordinators over one catalog and one worker pool.
- `coordinator.TestTheWireCarriesTheCoordinatorsPlannerOption` — two
  coordinators built by `New` publish a pipeline task each while a subscriber
  captures the bytes that leave; it asserts the option on the published
  payload both ways, and that a default coordinator's payload does not name
  the key. It fails when `Coordinator.New` stops wiring the scheduler, which
  the gate that calls the stamp directly does not.
- `internal/cli.TestTheSharedCLIDatabaseHonoursEveryResolvedEngineFlag` and
  the source half of `internal/cli/planner_config_doors_test.go`, which parses
  the package and requires every `wadjet.Config` literal to set each of the
  five flags, with a floor so a door added later inherits the requirement;
  `internal/clid.TestEveryDatabaseThisServerPlansWithCarriesItsResolvedOptions`
  for the fallback database.
- `benchmarks/tpch.TestTPCHQueriesBushyForced` and
  `coordinator.TestTPCHNativeDAG_BushyForced` — the 22-query parity suites,
  run as TWO INSTANCES over one fixture, so the default arm's dormancy is
  asserted while the bushy instance is open beside it.

## Consequences

- Adding a planner setting means a field on `logical.Options` or
  `physical.Planner` and a line in each instance's construction, not a package
  variable and a `Store` at startup.
- **The same shape survives outside the planner** and is filed rather than
  implied: `embedding.globalProvider` (the `embed()` provider, set at `Open`
  from `Config`) and `expr.DefaultUDFs` (the UDF registry a `CREATE FUNCTION`
  writes) are process-wide values an instance still feeds, last `Open` wins,
  and `Close` restores neither (#1224).
- A worker is configured by the tasks it is given, for anything that changes a
  PLAN. That is a deliberate asymmetry with the rest of its configuration —
  its budgets and its scratch are its own — and it holds only because
  `executePipeline` is the single worker path that runs the optimizer;
  fragment tasks carry `OpSpec`s and reorder nothing.
