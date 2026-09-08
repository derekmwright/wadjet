# ADR-0033: A column policy is a plan-time projection at the scan

Status: Accepted
Date: 2026-09-04
Issue: #859

## Context

Wadjet's ABAC obligations include `mask_column` (replace a column's value) and
`deny_column` (remove it). Until v0.18.31 they were enforced in two different
places, and neither of them was where a value can actually be caught.

`logical.InjectColumnPolicies` wrapped a policed table's Scan in a security
projection — the right design — but it built that projection from the scan's
own column annotation and **returned the plan unchanged when that annotation
was empty**. It is empty for exactly the queries a mask matters most for. And
the relations it was told to police came from `plansql.SelectInfo.Tables`,
which is not a list of tables: a derived table appears there under its own
subquery TEXT, a CTE reference under the CTE's name, and the arms of a UNION
appear not at all.

Where the projection was skipped, a second mechanism was supposed to catch it:
`Server.applyColumnPolicies` rewrote RESULT ROWS after execution. It existed on
one door — HTTP — and could only ever rewrite an output column that still
carried the policed column's name. The census at v0.18.30 (three doors ×
four arms × eighteen shapes) measured what that produced:

| shape | what the analyst got at base |
|---|---|
| `SELECT ssn`, `SELECT *`, `MIN/MAX(ssn)`, `GROUP BY ssn`, `ORDER BY ssn` on the standalone HTTP door | the TRUE values |
| `SUM(acct)`, `COUNT(DISTINCT ssn)`, a self-join on `ssn`, `WHERE ssn = '<true>'`, a window over `ssn` on the same door | computed on the TRUE values |
| `UNION ALL` over a masked column, every door and every arm | the TRUE values |
| `(SELECT MAX(ssn) FROM t)`, every door and every arm | the TRUE value |
| a derived table or a CTE over the table, every door | `access denied to table "(SELECT ssn FROM e7emp)"` |
| `SELECT salary` (denied) | a phantom all-NULL column on the single-process path; the whole `SELECT *` row set on the DAG |
| `WHERE salary > 0` (denied) | answered from the raw column — a working oracle for the denied value |
| `SUM(salary)` (denied) on the standalone HTTP door | the TRUE sum |

## Decision

**1. Masking and denial are plan-time, at the scan, unconditionally.**

The security projection is built from the TABLE's declared catalog schema,
which is always known at enforcement time. It replaces the column for every
consumer above the scan: WHERE, GROUP BY, aggregates, DISTINCT, join keys,
windows, derived tables, CTEs, set-operation arms, `SELECT *`.

**2. The relations to police come from the PLAN.**

`logical.PolicedScanTables` walks the plan for base-table Scans, which is the
one place a derived table, a CTE reference and a UNION arm all resolve to the
relation actually read. The statement's FROM list is still consulted, filtered
to the names the catalog recognises as tables, so a relation the plan carries
no Scan for is still access-checked and a subquery's TEXT is never handed to a
default-deny evaluator.

**3. A denied column does not exist.**

A reference to one is 42703 — the same refusal, byte for byte, that a column
the table really does not have produces, hint list included. This is
implemented by binding names against the schema the identity can see
(`physical.ValidateColumnsUnderPolicy`), not by a second rule about denial, so
every position a name can appear in is covered by the binder that already
knows them all. An identity that may not read a column may not learn that the
column exists.

**4. A policy that cannot be applied refuses.**

`logical.ErrColumnPolicyUnenforceable`. A scan whose columns nobody can name,
or a table with every column denied, is not "no policy": it is a control that
could not be applied, and the query is refused rather than answered unmasked.
This is #802's rule ("a security control never degrades to a grant") applied to
enforcement rather than to configuration.

**5. One enforcement path; the door-specific one is deleted.**

`Server.applyColumnPolicies` and `auth.AccessPolicy.ApplyToRow(s)` are gone. A
legacy YAML `policies:` block keeps working exactly as documented, through
`MigrateRBACToABAC` into the same obligations the plan-time path enforces —
and now reaches the aggregate, the group key and the join key, which a
result-row pass never could.

**6. A row filter is BELOW the security projection; a user predicate is above
it — on every arm, including the DAG's fragment.**

This is PostgreSQL's RLS ordering. The POLICY's predicate reads the row as
stored, so a row filter written against a masked column compares the TRUE
value. A predicate the USER writes sits above the projection, so
`WHERE ssn = '<true value>'` matches nothing and `WHERE ssn = '<mask>'` matches
every visible row. `auth.EnforcePlanPolicies` injects the projection first and
the row filter second for exactly this reason; `InjectRowFilter` lands directly
above the Scan and marks its Filter `PolicyFilter`.

The DAG's scan fragment carries the same two-slot order —
`OpScan → OpFilter(policy) → SecurityProject → OpFilter(user)` — because ONE
slot for both is a disclosure, not a detail. With one slot, a user predicate
the plan left above the barrier (one substitution could not push down, which in
practice means one carrying a subquery) was lowered into it and read the
STORED column: `WHERE bal > (SELECT MIN(bal) FROM t)` over a `bal` masked to 0
returned exactly the rows whose hidden value was positive, while the
in-process pipeline returned none. The other operand is the policy's own mask,
a constant the client knows, so each row's membership in that answer IS the
hidden value.

`physical.CheckSecurityFilterOrder` is the invariant, checked after every
rewriting pass: **no predicate below a security projection may name a column
that projection hides**, the policy's own filter excepted. It refuses `0A000`
rather than trusting the routing, because a pass that copies `FilterExprs`
without its `PostSecurityFilterExprs` companion fails as a disclosure, not as
a wrong count. A pin is never the disposition for a leak.

A row filter naming a column the table does not have refuses (42703): it would
restrict no rows, which is the mask spellings' failure class said for the other
half of a cell policy. It binds against the UNFILTERED schema, because a policy
predicate is allowed to read a column the same policy denies.

**7. An expression subquery is planned under the same policy.**

`(SELECT MAX(ssn) FROM t)`, an IN set and an EXISTS are whole second queries
that `physical.buildSubqueryPipeline` parses, builds and optimizes on its own;
they never passed through the enforcement path. The resolved policies travel on
the CONTEXT (`logical.ContextWithColumnPolicies`), which is the only carrier
both planning paths already share, and that path applies the same projection
and the same 42703 binding.

**8. A DML statement is a write, and its reads see what a SELECT sees.**

An identity whose policies grant no `ActionWrite` on the table is refused with
42501 before anything is read or written. When the write IS allowed, a DENIED
column does not exist inside the statement — in a predicate, as a SET or
INSERT target, or inside a SET expression — and a MASKED column reads as its
mask, so `WHERE ssn = '<stored>'` matches nothing and `SET dept = ssn` writes
the mask. It is a SUBSTITUTION into the statement's own expressions rather
than a projection because a DML predicate is compiled and never planned
(ADR-0031); where the rewrite cannot be done soundly the statement is refused.

**9. An unpoliced identity is unchanged.**

An admin identity, and an in-process caller with no identity at all, see the
raw table. `EnforcePlanPolicies` still no-ops with no provider, no identity or
no evaluator. A network door with a provider wired refuses an unauthenticated
caller before planning, so "no identity" is a state only the embedded API and
the coordinator's `ExecuteSQL` can present.

## Consequences

- The mask's default value is chosen from the column's DECLARED TYPE (`0` for
  numerics, `false` for booleans, `'***'` otherwise) rather than from the Go
  type of a result value. A plan-time projection has to type-check, so a bare
  `'***'` over a BIGINT column would make `SUM(col)` an error. This is the
  same table the deleted row-side `defaultMaskValue` used.
- **A masked column declares the MASK EXPRESSION's type on the wire**, which is
  what PostgreSQL declares for any expression in a SELECT list. Where the mask
  is written in the column's own type — `'***'` over `text`, `0` over
  `bigint` — the declaration does not move, and those are the two the
  type-derived default produces. Where it is not, it moves and the client sees
  the mask's type: a `TIMESTAMP` masked with the default `'***'` arrives as
  `text`, `MAX(ts)` is `'***'`, and `WHERE ts > '2000-01-01'` returns no rows.
  That is the honest consequence of a mask being an expression, and the
  remedy is to write a mask of the column's own type
  (`value: "TIMESTAMP '1970-01-01'"`); a typed placeholder for every one of the
  22 types is NOT settled here. `SELECT *` under a deny policy omits the
  column from the RowDescription on every door.
- The projection is one plan node per POLICED scan. A query by an identity with
  no column obligations gets no node and no cost.
- A policy binds to a RELATION — the catalog table a scan reads — and never to
  an alias. `FROM other AS policed_name` is a scan of `other` and is untouched.
- The optimizer MINTS scans: the decorrelation passes re-parse a subquery from
  its text and build a fresh Scan after enforcement ran. The projection is
  re-applied to any uncovered policed scan immediately after every
  `logical.Optimize`. That pass takes its column list from the SCAN, because
  after pruning the authority is what the scan produces; before the optimizer
  the catalog is the authority, which is decision 1. It adds every MASKED
  column to that list whether the scan's pruned list names it or not — a mask
  is computed from a literal, so publishing it costs nothing, and leaving it
  out turns a predicate above the projection into a read of a column the
  projection does not carry.
- **The relation may not exist yet when enforcement runs.** A table named only
  inside an `IN (SELECT … )` is SQL TEXT at that moment — no Scan, no FROM
  entry — so it was never policed at all, and the semi-join the optimizer
  later built from it had no projection and a predicate over the STORED
  column. `logical.PolicyLookup` rides the context so any pass that meets a
  scan can ask what the policy does to ITS table, taking the same decision
  path (table access, column obligations, row filter) as the first pass.
- **The invariant is asserted, not assumed, over EVERY plan the query builds.**
  `logical.CheckPolicyPlanOrder` runs on the statement's final logical plan AND
  on each plan the physical planner builds for itself — every subquery pipeline
  (which every subquery runner and IN-set materializer reaches) and the DAG's
  scalar producer. A scan of a policed relation with NO projection is `0A000`,
  and so is a non-policy filter between a projection and its scan that reads a
  policed column. Asking only about the STATEMENT's plan is unfalsifiable for a
  relation named only inside subquery TEXT, and what that text contains — a
  derived table, a set operation, a correlation — is the client's choice, so no
  per-shape enumeration can cover it. The line is drawn by the INNER PLAN: a
  subquery the planner folds into the outer plan is ordered with it and
  ANSWERS; a subquery that keeps a plan of its own — a set operation inside an
  `IN`/`EXISTS` list, a correlated scalar, `LATERAL` — REFUSES, whether or not
  the outer statement reads the same relation. Which shapes fold is the
  PLANNER's business and moves with it: a derived table inside an `IN` list
  refused until v0.18.36's set-operation work and answers now, correctly, on
  every arm.
- **Scan-level filter pushdown stops at a security projection.** It evaluates
  against the FILE, so pushing a predicate that sits above the projection makes
  it read the stored column — the in-process twin of a single filter slot on
  the DAG.
- **A policed scan carries no predicate but the policy's own.** Node order
  above a scan says nothing about what is ATTACHED to it: `attachScanPredicates`
  copies a filter's `col <op> literal` conjuncts onto the scan directly beneath
  it for row-group pruning, and the scans the post-optimize pass covers are
  minted by that same optimizer — the inner of a decorrelated `IN`/`EXISTS` is a
  Filter over a bare Scan when the copy happens, and the projection arrives
  afterwards. The scanner then prunes by the STORED column's statistics: `… IN
  (SELECT id FROM emp WHERE ssn = '<the mask>')` skipped every row group whose
  stored range excluded the mask and answered NO ROWS in process, where the DAG,
  which attaches nothing there, answered every row. That was this arc's one arm
  split, and it was a DISCLOSURE — the row set is arithmetic on statistics of
  the hidden column, so a client moving the constant reads its range off the
  answer — not the neutral path difference an earlier revision of this ADR
  recorded on a mis-localized diagnosis. The pass that injects the projection
  now strips the policed attachments (scan predicates, node predicates,
  partition filters) from the scan it covers; `Predicate.FromPolicy` exempts
  the row filter, which reads the row as stored by design (decision 6); and
  `CheckPolicyPlanOrder` asserts it structurally, so an attachment made by a
  future pass refuses rather than prunes.
- A mask expression is evaluated BELOW the barrier, against the row as stored.
  One that reads a column the same rule masks or denies is refused at load and
  at enforcement, because it would publish exactly what the rule takes away. An
  expression over an unrestricted column is allowed and sees the stored row.

### A `query_limit` obligation is a cost ceiling, enforced where the config's is

The obligation was read by the loader and dropped by the evaluator, so
docs/security.md's obligation table said "Not enforced" beside a security
control. It now narrows the SAME cost guard `query_limits:` uses:
`target` names the ceiling (`max_scan_rows` — the default when empty and the
only reading the docs ever gave it — `max_scan_bytes` or `max_scan_files`),
`value` is a positive integer, and anything else refuses at config load and at
hot reload, keeping the previous policy set (decision 4's doctrine).

The two ceilings arrive by different routes because they are decided at
different times: the deployment's is on `Planner.QueryLimits`, set at each
door's one planner-construction site; the identity's is decided while the
policies are evaluated, which is after every planner has been built, so
`EnforcePlanPolicies` puts it on the context and
`Planner.enforceQueryLimits` — the one place the guard runs — takes the
tighter of the two. A policy can only NARROW: an obligation naming a larger
number than the deployment allows does not widen the deployment's guard, and a
statement reading two policed relations is held to the tighter of theirs.

A DML statement is not planned (ADR-0031), so it has no scan-cost estimate and
no ceiling to compare it against; `query_limit` is a read control.

### Not settled

- **A task that carries a statement's TEXT is re-planned where no policy is.**
  `TaskTypePipeline` carries `SQLText`, and `worker/executor.go`'s
  `executePipeline` parses, builds and optimizes it again. Every dispatch site
  that puts such a task on the wire is guarded at the one choke point they all
  go through — `Scheduler.PublishTasks` refuses `0A000` when a policy shaped
  the query and the task carries text with no operator fragment and no inputs —
  so the consequence today is that the async door refuses rather than answers
  unmasked. The fix is a pipeline task that carries the enforced PLAN rather
  than its text; a worker that reconstructs the projection from
  `PolicyDecisionJSON` is a SECOND enforcement path and decision 5 forbids it.
  Its own arc.
- **MERGE under a column policy is refused.** Its WHEN clauses carry raw
  SET/VALUES text that the DML rewriter does not decompose, so it cannot be
  shown to honour the policy. Refusing beats running those reads against the
  stored row.

  The structural fix, measured: `MergeWhenClause.SQL` is the raw text of a
  `SET a = …, b = …` or `(cols) VALUES (…)` clause, and the ONE thing that
  decomposes it is `wadjet/dml.go`'s `applySetClauses` / `buildInsertRow`,
  which split it with `strings.Index("=")` and `splitSetClauses` at EXECUTION
  time. Rewriting the clause in `internal/auth` means a SECOND string splitter
  over the same text, and the two would have to agree about every spelling
  forever or the policy rewrite would cover a different set of expressions
  than the executor evaluates — decision 5's second enforcement path, in the
  place it does the most damage. The fix is for the PARSER to decompose a WHEN
  clause into typed assignments the way it already decomposes `UPDATE ... SET`
  into `UpdateInfo.SetClauses`, so one decomposition exists and both the
  rewriter and the executor read it. That is a parser change plus an executor
  rewrite: its own arc. The refusal is pinned by
  `internal/server.TestMergeUnderAColumnPolicyIsRefusedOnEveryDoor`, which
  fails the day MERGE answers.
- **A typed mask placeholder per type** (see the wire-type consequence above).
- **`LATERAL` over a policed relation refuses.** Its decorrelated inner keeps
  its predicate in a shape the planner cannot reorder above the projection, so
  the invariant refuses `0A000` uniformly on every arm rather than answer.
- Per-row / per-cell labels (a visibility column, a `has_access` function
  family, dictionary-level evaluation) are a 0.19 arc, not this one.

## Gate

`internal/server/policy_masking_matrix_test.go`:

- `TestPolicyMaskingIsPlanTimeOnEveryDoor` — three doors (embedded, pgwire via
  pgx, HTTP `POST /v1/queries`) × the arms each can present (single, spilled,
  DAG, DAG-shuffled; pgwire and HTTP in both their single-process and
  coordinator wiring) × twenty-one shapes. Every cell asserts the masked value
  or the refusal, and a blunt textual check that no true value appears anywhere
  in the answer. `WADJET_E7_CENSUS=1` prints the per-cell census instead of
  asserting — that is how the base state above was recorded.
- `TestPolicyMaskingLeavesUnpolicedIdentitiesAlone` — decision 8.
- `TestLegacyYAMLCellPoliciesAreEnforcedAtThePlanOnEveryDoor` — decision 5.
- `TestARowFilterSeesTheTrueValueAndTheUserPredicateSeesTheMask` — decision 6.
- `internal/planner/logical/plan_coverage_test.go` —
  `TestInjectColumnPolicies_SchemaColumnsBeatAnEmptyScan`,
  `_NoColumnsAvailable`, `_AllColumnsDenied` for decisions 1 and 4.

---

## Amendment 2026-09-05 — a policy binds to the RELATION, not to a spelling of it (#882)

**Status:** accepted, supersedes nothing; extends the invariant to the NAMES a
policy is written with.

An unquoted SQL identifier is folded to lower case by the lexer (#731), while a
catalog keeps the spelling the parquet file or the Iceberg import gave it —
where CamelCase is ordinary (`Hits`, `WatchID`, `EventDate`). So one relation
has two legitimate spellings, and the engine already reconciles them with one
rule: `catalog.ResolveTableName` for a relation, `batch.ResolveSchemaIndex` for
a column — byte-exact first, then a unique ASCII-case-insensitive match for a
name that is itself folded, with a delimited name staying byte-exact
(ADR-0012 item 4).

The policy layer did not use that rule, and it failed OPEN in both directions
at once:

- ABAC compared `resource.name` through the generic attribute comparator
  (`compareEq`, `fmt.Sprintf("%v")` — byte-exact). With catalog `Hits` and a
  rule scoped `resource.name eq "hits"`, the scoped rule did not match. **An
  unmatched scoped rule is not a refusal**: the broad `allow` that every
  `roles:`-to-ABAC migration emits still matched, so the decision came back
  Allowed with NO obligations — the masked column in plaintext, the denied
  column present, and a DML predicate on the masked column a working oracle for
  its own stored value, which is exactly the disclosure rule 2 exists to close.
- the legacy `PolicySet` keyed its map by the YAML's bytes and looked it up
  with the statement's folded name, so `table: Hits` bound to nothing and the
  row filter was silently absent.

The two directions are opposite, so on a CamelCase relation **there was no
single spelling an operator could write that bound on both paths**. Whichever
they chose, one path was open. That is what makes this a defect in the policy
model rather than a configuration error.

### The position

1. **A policy's relation and column names resolve through the SAME rule the
   query planner uses for identifiers.** `catalog.ResolveTableName` and
   `batch.ResolveSchemaIndex`; a delimited name stays byte-exact, so a policy is
   never more permissive about names than the queries it polices.
2. **The binding happens ONCE, when the policy set is ATTACHED to a catalog.**
   Every relation and every policed column is rewritten to the catalog's own
   spelling, so evaluation compares two names that came from the same place
   rather than two spellings of an idea. Binding is a property of the ATTACH,
   not of any particular caller: every entry that installs a provider against
   a catalog binds — `wadjet.Open(Config{AuthProvider})`,
   `wadjet.DB.SetAuthProvider`, `Coordinator.SetAuthProvider`, `server.New`,
   `pgwire.NewServer`, `NewGRPCServer`, the provider's own `Update` /
   `UpdateWithEvaluator` setters, and the hot-reload path. Wiring it into two
   `serve` functions instead left the embedded API and any caller standing up
   its own doors on the floor alone, which is how #882 survived its first fix;
   wiring it into five left `wadjet.Open` — the entry `wadjet mcp` uses — which
   is how it survived its second.

   The list is not the mechanism, because a list is what was wrong twice.
   Installing a set goes through ONE function (`Provider.installState`, or
   `BindToCatalog` / `AttachProvider` for an attach), and
   `TestEveryProviderFieldIsAttachedThroughTheBindingFunction` reads the source:
   it enumerates every field whose declared type MENTIONS `*auth.Provider` at
   any depth — plain, embedded, in a slice, map, channel, anonymous struct or
   generic type argument, or behind an interface a provider satisfies — and
   fails when a function assigns one without binding THAT provider in the same
   function (a bind on another value, or one parked in a dead branch, does not
   count). **A new door that holds a provider in a field cannot be added and
   forget.** That is the claim, and it is the claim the gate proves: the census
   is syntactic, so a provider reached through a named type declared elsewhere,
   through an interface from another module, or through reflection is outside
   it. Ten shapes that defeated the first version are its negative controls
   (`TestTheCensusCatchesTheShapesThatDefeatedIt`).
3. **A policy that names a relation or a column that does not resolve is
   REFUSED AT ATTACH.** This is ADR-0033's existing rule — a policy that cannot
   be enforced does not load (#802) — applied to names. Without it a typo is
   indistinguishable from a relation that does not exist yet, and the rule
   carrying the obligations silently never matches, which beside a broad allow
   is a grant. A hot reload that cannot be bound **keeps the previous set**, and
   an attach that cannot bind is REMEMBERED: `Provider.BindError` makes every
   enforcement entry point refuse the statement, so a caller that ignores the
   error does not get the unbound set enforced quietly.
4. **A policy set that has been bound cannot yield "no policy applies" through
   a spelling mismatch, and one that could not be bound does not enforce at
   all.** Be precise about the floor, because it is narrower than it sounds:
   the fold-aware comparison (`relationEq` for `resource.name`, `policyKey` for
   the legacy set) reconciles exactly ONE pair of spellings — the catalog's and
   the folded one — because a name carrying an upper-case letter can only have
   been delimited and a delimited name is byte-exact. `HITS` and `hItS` against
   a catalog `Hits` are neither of that pair, and the floor does not reach them;
   the BIND does, by refusing them the way it refuses a typo. Where no catalog
   is attached the floor is all there is, and it covers the one pair. Rule 4 as
   first written claimed more than `relationEq` can give, and the arc's own
   gate could not see the gap because its harness did not attach the way
   production attaches.
5. **The bind rewrites a COPY and swaps it in with a CAS, and is idempotent.**
   The evaluator it would otherwise rewrite is being read by every decision in
   flight, so an in-place rewrite is a data race on a live security decision;
   and a bind that fails partway would leave the RUNNING set half-rewritten,
   which is the opposite of what (3) promises. The swap is a compare-and-swap
   because a bind reads the running set and installs a bound copy of it: a
   store would overwrite a set installed inside that window, so a policy set
   the operator RETIRED would come back. A lost CAS re-binds the set that won.
   And a set already bound to the same catalog re-attaches for free — the HTTP
   DML door re-attaches on every statement, which is what made a microsecond
   window a live one.
6. **Only the attributes that carry an IDENTIFIER get the identifier rule.**
   `eq` stays byte-exact for every other attribute: folding it generally would
   make `classification eq "SECRET"` match `"secret"` and quietly widen every
   clearance in the file. `resource.name` and `resource.table` are the list.

### Consequence an operator must know

A policy may not name a relation the catalog does not hold. Startup refuses
with the policy, rule and name; a hot reload refuses and keeps the running set.
This is deliberate and it is the fail-closed direction: the alternative is a
policy file that loads clean and enforces nothing. It reaches every door, not
only `wadjet serve`: `wadjet mcp` refuses to start, `wadjet.Open` returns the
error, and a caller that discards it still gets every statement refused
(42501). In an embedded program this decides an ORDER — create the tables a
policy names, then attach the provider.

### Gates

- `internal/server/policy_relation_spelling_test.go` —
  `TestPolicyBindsToTheRelationNotToASpellingOfIt`: both policy spellings ×
  both statement spellings × three doors (embedded, pgwire, HTTP) × read
  (mask + deny) and write (UPDATE / DELETE / INSERT / MERGE on a denied
  column), plus the mask-as-oracle cell and the counter-cell that a policy
  scoped to a DIFFERENT relation still does not bind.
  `TestLegacyPolicySetBindsToTheRelationNotToASpellingOfIt` covers the
  non-ABAC path, whose failure pointed the other way.
- `internal/auth/policy_bind_test.go` —
  `TestPolicyNamesBindToTheCatalogAtLoad` (binding, refusal, the delimited
  wrong-case column, the `tables: ["*"]` wildcard, the legacy set),
  `TestPolicyBindFailureKeepsThePreviousSet` (the hot-reload contract),
  `TestBindToCatalogDoesNotMutateTheRunningSet` (run under `-race`),
  `TestAFailedBindLeavesTheRunningSetIntact`,
  `TestAReattachOfABoundSetWritesNothing` and
  `TestARebindNeverResurrectsARetiredPolicySet` (the CAS, measured on a
  deliberately widened window) for (5), and
  `TestASetInstalledOnABoundProviderIsBoundToo` for the setters — including
  the case that must NOT refuse: with no catalog attached the set installs and
  the floor of (4) applies.
- `internal/auth/attach_sites_test.go` —
  `TestEveryProviderFieldIsAttachedThroughTheBindingFunction`, the source
  census behind (2): every field whose type mentions `*auth.Provider` at any
  depth is enumerated and every function that assigns one must bind that
  provider. Its exception list carries a reason per entry, its config-type
  exemption is an exact type set, and both are asserted in both directions.
  `TestTheCensusCatchesTheShapesThatDefeatedIt` runs it over ten shapes that
  slipped past its first version — embedded, slice, map, interface, generic,
  a func literal in a package-level var, a bind on an unrelated receiver, a
  bind in a dead branch — each of which must now be caught, beside a control
  that a constructor which does bind is not flagged.
- `internal/server/policy_relation_spelling_test.go` —
  `TestAnAttachedPolicySetThatCannotBindRefusesEveryQuery`: the spellings the
  floor cannot reach (`HITS`, `hItS`, `Hitz`) refuse on all three doors, read
  and write, with a control that a bindable set still answers and masks. Its
  harness attaches through `SetAuthProvider` exactly as a door does, which is
  what lets it see (2). One provider reaches all three doors, so that matrix
  cannot attribute a bind to a call site: `TestTheEmbeddedAttachBindsOnItsOwn`
  builds only the embedded DB, `TestTheHTTPServerAttachBindsOnItsOwn` only the
  HTTP server, and `TestTheOpenAttachBindsOnItsOwn` passes the provider in
  `wadjet.Config` and builds nothing else, so in each exactly one attach could
  have bound — and with that attach's bind removed the door DISCLOSES while
  the matrix still passes.
- `internal/server/mcp/policy_attach_test.go` —
  `TestTheMCPDoorEnforcesThePolicyItsOpenAttached`: the `wadjet mcp` shape,
  built the way `runMCP` builds it, driving the query tool an agent calls.

Every fixture in this arc registers its relation through the CATALOG, because
the DDL door folds a name it MINTS: a CamelCase relation is one a dataset
brought, which is the only way the two spellings can differ at all — and the
reason every previous policy fixture, all lower case, was blind to this.

## Amendment 2026-09-08 — a star expands from the POLICED list, not the catalog's

Decision 1 says the security projection replaces the column "for every consumer
above the scan… `SELECT *`". A star is a consumer above the scan, but it is a
consumer of a particular kind: it does not name a column, it asks for the
relation's column LIST, and until this amendment it asked the wrong thing for
it.

`logical.ExpandStarProjections` read that list off the scan's catalog-annotated
`ScanColumns` — the table as the CATALOG declares it, underneath the barrier.
`SELECT *` was right anyway, but only by accident of shape: a bare star alone
builds no projection at all, so the plan's output IS the security projection's
output. Every other spelling read past the barrier. In v0.18.61 `SELECT a.*`
sent an analyst a `salary` column its policy DENIES, on the embedded, pgwire
and HTTP single-process doors.

The regression is TWELVE spellings, not one. Measured with the gate's own star
cells (`WADJET_E7_CENSUS=1`, one tree per base):

| tree | leaking cells / shapes |
|---|---|
| v0.18.60 `bb8635a4` | 8 / **2** on eight doors; **10 / 2** with the ninth (`SELECT *, id AS z`, `SELECT a.*, a.id AS z`, both of which leak on the fast path too) |
| v0.18.61 `a0539069` | 52 / **13** on eight doors; **65 / 13** with the coordinator's default local fast path as a ninth door |
| the fix | **0** |

Only the two shapes that had a projection to expand at BOTH bases predate the
reported one; the other eleven — the qualified star alone, its derived, CTE and
two-deep spellings, the positional `ORDER BY`, both join orders, the filter, the
`ORDER BY … LIMIT`, the union arm and `SELECT * FROM (SELECT a.* …) d` — are
right on all eight doors at v0.18.60.

The fast-path figure is the one a deployment sees: `--local-fastpath-bytes`
defaults to 64 MiB, so a small query on a coordinator runs in process, and the
gate had that configuration pinned OFF.

**A star's source is what the relation PUBLISHES for this identity.**

`logical.StarSourceColumns` is the one list every star expansion asks for, and
`publishedScanColumns` inside it answers with the security projection's list
wherever one stands over the scan. Consequences:

1. **One source, every spelling.** `*`, `t.*`, a star beside another item, a
   derived table's or a CTE's star, a star nested inside either, a star under a
   positional `ORDER BY`, a star over a join, a star under a filter or a LIMIT,
   and a star inside a subquery all take the same list. The rule cannot differ
   by spelling, which is the failure this amendment exists to make impossible —
   `SELECT *` had a gate cell and `SELECT a.*`, one keystroke away, did not.

2. **A column-alias list renames what the identity can SEE.** The width a
   `d(k1, …, kn)` list must match is the POLICED width. Reading the catalog's
   list renamed the denied column to `k5` and left the real fifth column behind
   under its own name — a denied column laundered through a rename, invisible
   to any check that looks for the column's name.

3. **A list that cannot be enumerated REFUSES.** A barrier whose own column
   list cannot be read answers nil, the star stays unexpanded, and the planner
   refuses it (0A000, one sentence). It never falls back to the catalog list: a
   security control never degrades to a grant, which is the same rule an
   uncoverable scan already follows.

4. **A DECLARATION is a read too.** The zero-row `SELECT *` declaration (#846)
   and its join arm (#978) describe a relation without reading a row, and a
   denied column's NAME in a `RowDescription` is the same disclosure as one in
   a row. The scan arm was already safe — its walk stops at any Project, and a
   security projection is one — but v0.18.62's join arm read each side past the
   barrier and declared `salary` for `SELECT * FROM policed JOIN other`, on
   either join side. `declaredJoinSchema` now reads a security projection the
   way it already reads a materialized block: the side IS its projection.

5. **Second answers are gated structurally, not only by cells.**
   `logical.TestOnlyOnePathReadsAScanColumnListForAStar` parses the three
   planner packages and asserts that `publishedScanColumns` is the only
   function inside `star_expansion.go` that reads a scan's own column list,
   that `ExpandStarProjections`'s call sites are pinned, and that no other
   planner function reads a scan's column list while talking about stars —
   except four named "what does this subtree publish" helpers, each of which
   returns at the first Project (which a security projection is) and each of
   which has a test showing it. An allow-list entry that stops matching FAILS.

Not settled here: a star whose source is a decorrelated LATERAL is still
refused rather than expanded (#979), and a star inside a decorrelated `IN`
subquery's derived table is refused on the single-process arms while the DAG
answers it — an arm divergence in the expansion's REACH, not in the policed
list, and it predates this amendment.

Gate: `server.TestPolicyMaskingIsPlanTimeOnEveryDoor` (21 new star cells, NINE
doors — the ninth is a coordinator at the shipped `--local-fastpath-bytes`
default — no `-short`), `logical.TestEveryStarSpellingExpandsFromThePolicedList`,
and CI's "Door Security Gates (no -short)" step — the matrix skips under
`-short`, which is why v0.18.61 shipped through a green CI.
