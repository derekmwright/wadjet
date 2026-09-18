# Outer join residual evaluation

Source: internal/planner/physical/join_residual.go — buildJoinResidualFilter,
moved 2026-09-11 (#1026), rewritten 2026-09-18 (arc JR, #1153)

buildJoinResidualFilter compiles an outer join's ON-clause residual — every
conjunct that is not an equi-join key pair — into a FACTORY of predicates over
the COMBINED row: the probe row plus one candidate build row (#358).

An outer join's ON runs BEFORE the NULL-padding, so this residual cannot be a
filter above the join (that deletes the preserved rows) and cannot be pushed
into a preserved side's scan (that deletes the rows the join owes unmatched).
The executor evaluates it per key-matched candidate; see
`exec.HashJoin.NewResidual` for the unmatched semantics it feeds, and
ADR-0006's 2026-09-18 amendment for why the evaluation point is settled.

## The expression is the engine's own (#1153)

What this used to be was a SECOND expression evaluator — a small AST
interpreter that accepted columns, literals, arithmetic and comparisons and
refused everything else, so `a LEFT JOIN b ON CAST(a.x AS VARCHAR) = b.y`,
`ON b.y LIKE '1%'` and every other ON PostgreSQL evaluates came back as a plan
refusal on all three outer kinds while the same predicate on an INNER join
answered.

Two evaluators for one seam is also two semantics: whichever of them a query
reached decided what its ON meant, and the interpreter's own operator switch
carried no case for `IS DISTINCT FROM` — so that comparison returned SQL
UNKNOWN for every candidate pair, every candidate was rejected, and a LEFT JOIN
answered its whole probe side NULL-padded where PostgreSQL matches.

There is one evaluator now. Each DISTINCT reference is bound to a SIDE and a
column at plan time, the AST is rewritten to read that binding by position
(`_wj_on_<i>`), and `expr.Compile` compiles the rest — the same compiler the
inner join's lifted filter runs above the join, so a predicate means the same
thing on both sides of that lift.

The rewrite is why the compiled expression never resolves a user name: two arms
of a self-join publish the same bare names, and a compiler asked to choose
between them would be a THIRD resolution rule beside this file's and
`exec.ColRef`'s.

## Column resolution

By name, decided lazily on the first evaluated pair and cached:

- a `row.field` path is asked for BEFORE the qualifier is stripped (ADR-0022
  rule 1, #769) and binds to the CONTAINER'S CHILD, so the combined row carries
  the field's own value under the field's own declared type;
- a qualified name is looked up in the probe then the build schema (self-join
  chains carry qualified columns);
- a qualifier equal to `buildAlias` forces the build side;
- otherwise the bare name resolves probe-first.

Every one of those lookups is `ResolveColumnIndex`, not `ColumnIndex`: the
names here are REFERENCES off the ON clause, so they arrive folded from the
lexer (#731), while the batch carries the catalog's own spelling —
`RegionName`, not `regionname`, for a parquet-registered table. A byte-exact
probe misses every CamelCase column, and because an unresolvable column makes
the residual UNKNOWN the failure mode is not a loud one: the join rejects every
candidate and NULL-pads each preserved row, so a LEFT JOIN whose ON carries a
residual answers all-NULL on the null-supplying side. The rule the resolver
applies (fold only a reference that is itself folded; a delimited name stays
byte-exact) is in `internal/engine/batch/schema.go`.

An unresolvable column still makes every evaluation UNKNOWN (candidate
rejected) and logs once — the planner ships JoinFilter columns through
NeededColumns, so a miss here is a plan bug, not user error.

## Why a factory

An evaluator OWNS the combined-row scratch batch it rewrites per candidate, so
it cannot be shared: `exec.HashJoin.Probe` mints one per clone, and parallel
pipeline workers each get their own. The COMPILED tree stays shared, which is
what this engine already requires of every predicate closure
(`exec.Filter.Clone` shares `Pred` for the same reason).

Both halves of the combined row are rewritten on every candidate. Caching the
probe half on the batch pointer would be wrong rather than merely stale: probe
batches are POOLED, so the same pointer carrying different rows is the ordinary
case and pointer identity is not freshness.

A slot whose type is append-built (ARRAY, MAP, ROW) is minted fresh per
refresh, because `batch.Vector.ResetForWrite` refuses to reset those in place;
every other type reuses its slot.

## What still refuses

The error is the plan's refusal and NAMES the construct that cannot be
evaluated at a join: a subquery in ON (`plansql.ColumnRefs` refuses it, because
a subquery's columns are not resolvable from here), a window function, an AST
node nothing there knows, or a function or type the expression compiler itself
refuses. The caller must raise it rather than drop the conjunct — the pre-#351
silent drop is the defect class this path exists to bury.
