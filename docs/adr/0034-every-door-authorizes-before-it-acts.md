# ADR-0034: Every door authorizes before it acts

Status: Accepted
Date: 2026-09-06
Issues: #930 #931 #932 #933 (the authentication and policy core); #934 #935 (gRPC); #936 #937 #938 (HTTP and pgwire doors); #939 #940 #941 #942 #943 (the embedded API and SQL statement door)

## Context

Wadjet has four client doors — the HTTP API, the PostgreSQL wire protocol,
gRPC, and the embedded `wadjet.DB` — plus MCP over stdio. ADR-0033 settled what
a COLUMN policy is and where it is applied. It did not settle who asks, and
what happens when the answer cannot be computed.

A batch-wide census in September 2026 found that each door had answered those
questions for itself, and that the seams between them all failed OPEN:

| what was found | shape |
|---|---|
| An authentication configuration that could not be built reported "authentication disabled" (#931) | HTTP served a request with no credential (200), pgwire never asked for a password and returned rows, gRPC and MCP the same. A hot reload of a broken config REPLACED a working authenticator with the disabled one. |
| The config conversion stripped the namespace from every ABAC condition (#930) | Every documented `subject.` / `resource.` / `env.` condition matched nothing. A conditional ALLOW failed closed; a conditional DENY beside a broad allow failed OPEN — the deny vanished and the grant answered. |
| The security vocabulary was open (#932) | `effect: dney` resolved to ALLOW and granted the action the operator denied. `type: deny_colum` was dropped and the column came back in plaintext. An unknown action, operator or attribute made its rule match nothing. |
| The environment never reached the evaluator (#933) | `env.time`, `env.hour` and `env.source_ip` were never published: every shared enforcement path built `Environment{Protocol}` by hand. The one site that passed an address passed `host:port`, which a documented IP rule cannot match. |
| A provider with no ABAC evaluator enforced nothing | `roles:`-only deployments — the embedded `SetAuthProvider` shape — skipped BOTH shared paths. A role scoped to one table could read another; a role allowed only `read` could DELETE. |
| A denied SELECT and a denied DELETE refused differently | The write was a `sqlerr` 42501; the read was a bare `fmt.Errorf`, so the wire carried the generic 42000 and gRPC `codes.Internal`. |
| Metadata doors and data doors disagreed | `SHOW TABLES` filtered on one door and on no other; `DESCRIBE` refused on one and answered on the rest; DDL asked `HasPermission` and never asked whether the identity may touch THAT relation. |

The pattern is one pattern. Wherever a security decision could not be
COMPUTED — a config that would not build, a word that was not understood, an
attribute that was not published, a component that was not installed — the code
proceeded as though there were nothing to enforce. That is the failure mode a
security control must not have.

## Decision

**1. Fail closed.** Default deny. A configuration that cannot be read, built or
understood REFUSES to start and REFUSES to reload, keeping the previous state.
It never degrades to "disabled", to "allow", or to "obligation dropped". An
unknown word in a security field is an error at load, and the runtime that
consumes it closes anyway for a caller who skipped the loader.

**2. No admin by inference.** Administrator status comes from the Authorizer —
`HasPermission(id, "admin")` — never from a role NAME and never from a
hard-coded `true`. A role called `admin` whose `allow` omits `admin` is not an
administrator.

**3. Every door authorizes before it acts.** Authentication proves WHO; each
operation still asks MAY. Every mutation (DDL, DML, COPY, UDF, DLQ, result
cleanup, cancel) and every metadata read (list, describe) authorizes BEFORE it
touches the catalog, the registry, the ingester or the store. A refusal happens
before any side effect and before any row is consumed — and a fixture proves
the side effect did not happen, not merely that an error was returned.

**4. Ownership is enforced.** A resource created under an identity (a query, a
result, a UDF) records that identity immutably; access is owner-or-admin; a
listing is filtered by owner.

**5. One decision, called — never forked.** The effective table-access decision
lives in `internal/auth` and every door asks IT:

```go
// The one decision, for a catalog table a door is about to read metadata of
// or act on.
func TableAccess(ctx context.Context, provider *Provider, table string, action Action) error
// The same decision over a listing, in order.
func VisibleTables(ctx context.Context, provider *Provider, tables []string) []string
```

with, in order: provider nil or auth disabled → nil, nothing to enforce; a
policy set that could not be BOUND to the catalog → refused (ADR-0033 rule 3);
auth enabled and no identity → refused; **the role's `allow` list**
(`HasPermission(perm(action))`) in BOTH provider shapes; then, with an ABAC
evaluator installed → the EVALUATOR decides (deny-overrides, default deny), and
with no evaluator → `CanAccessTable(table)`. `table` is the CATALOG-RESOLVED
spelling (`catalog.ResolveTableName`), so a policy bound to `Users` polices a
statement that spelled it `users` (#731, #882).

The `allow` list is a COARSE GATE, and it is applied first in both shapes: **a
policy narrows what a role may do and never widens it.** `admin` still grants
everything, because `HasPermission` says so. Both halves remain required: a
permission alone is not access to a relation, and a relation alone is not
permission to do a thing to it.

**Every door reaches this one body.** `EnforcePlanPolicies` asks it per relation
the plan reads and `EnforceDMLPolicies` asks it for the target; the evaluator
decides the OBLIGATIONS — masks, row filters, ceilings — and no longer decides
access on its own. That is not a detail: while the plan and DML paths called the
evaluator directly, the coarse gate existed only on the metadata decision that
no data door asks, so a role written `allow: [read]` DELETED rows under a
permissive policy while `TableAccess(write)` refused, and an identity whose role
the configuration does not define was served by a rule matching everyone.

**A statement needs the permission for what it DOES.** A read needs `read` on
every relation the plan reads. A write needs `write` on its target — and `read`
as well only when the statement OBSERVES that target (a `WHERE` naming a column,
a `SET` value naming one, a `MERGE`). That is PostgreSQL's rule, measured on the
oracle: `INSERT` and an unqualified `DELETE` succeed on the write privilege
alone, a predicated `DELETE` does not. A blanket read requirement would refuse
what PostgreSQL allows and would make this decision disagree with itself
between the metadata door and the DML door.

A door that re-implements any of this — `TableAccess`, `VisibleTables`,
`RequirePermission`, `EnforcePlanPolicies`, `EnforceDMLPolicies`,
`Identity.ToSubject` + `PolicyEvaluator.EvaluateTableAccess` — has forked the
decision, and a fork is a divergence waiting to be found by an attacker rather
than by a census.

**6. A refusal has a class, and it is the same class on every door.**

| door | authorization | authentication |
|---|---|---|
| pgwire | SQLSTATE `42501`, `permission denied for table "x"` / `permission denied: "write" permission required` | SQLSTATE `28000` / `28P01` |
| HTTP | 403 with the same message text | 401 |
| gRPC | `codes.PermissionDenied` | `codes.Unauthenticated` |
| embedded `wadjet.DB` | the `sqlerr` 42501 error, verbatim | — |

The SAME operation refuses with the SAME class on every door, and a door census
is the gate that says so.

The text carries the relation and NOTHING ELSE — not the rule that denied, not
its description, not the reason. Which control fired is operator information
and goes to the audit log; telling the refused caller names the control that
stopped them, and a policy rule id is a rule an attacker can then probe around.

**7. Fail closed on a missing identity — on the DATA paths too.** With auth
ENABLED, a context carrying no identity is refused at every boundary: metadata,
DDL, the shared table-access decision, AND the shared plan and DML paths. The
last two used to return early and enforce nothing, so an embedded caller that
attached a provider and then queried without stamping an identity could SELECT
and INSERT while DESCRIBE and DDL refused it — one boundary, two answers. A
caller that runs under a provider stamps an identity (a scheduled alert does,
through `auth.StampDefiner`).

A stamped definer's GRANTS are re-resolved from the CURRENT role definitions,
never replayed from the stored snapshot: a snapshot records who the definer was
and not what they could do, so an alert whose creator's role has since lost
`write`, or been deleted, is refused on its next tick — and a persisted grant
can never go stale, because none is persisted.

With auth DISABLED, or with no provider (dev, embedded without
`SetAuthProvider`), nothing is enforced and NOTHING changes — every existing
no-auth caller stays working. That second half is not a courtesy: a security
change that breaks working deployments is reverted, and a reverted control
protects nobody.

**8. The environment is the boundary's observation, never the caller's claim.**
Each protocol door attaches a trusted `Environment` where the connection is —
the HTTP middleware, the pgwire connection handler, the gRPC authentication
interceptor — and every enforcement path reads it from the context through one
builder, `auth.DecisionEnvironment(ctx, protocol)`. `SourceIP` is the peer
address the server observed, with the port stripped at the attach so no door
has to remember; `X-Forwarded-For` is NOT trusted, because a header the client
sets would let the caller choose which source-address rule applies to them;
`Time` is stamped at DECISION time, not at attach time, because a pgwire
connection lives for hours and `env.hour` means the hour the STATEMENT ran; and
`env.protocol` names the door the client used, not the execution path
underneath it.

**9. The security vocabulary is closed and enumerated at load.** Effect,
action, condition operator, condition attribute namespace and obligation type
are exactly what the evaluator implements. `ValidateABACPolicies` refuses a
word outside them and names the policy, the rule and the field. The runtime
closes anyway: an effect that does not resolve to `allow` is DENY, and an
obligation that cannot be applied DENIES the relation rather than allowing it
with the restriction missing.

**10. A capability is granted only by a rule that NAMES it.** A resource whose
`resource.type` is not `table` is a CAPABILITY, not a relation — a table
function (`read_csv`, `read_parquet`, `postgres_scan`) reads the server's own
filesystem and opens outbound connections on the process's behalf. An ALLOW
rule matches such a resource only when it names the type with
`resource.type eq <type>` or `resource.type in [...]`; an unscoped allow, or
one scoped to relations, does not.

Without that, ordinary deny-overrides matching granted the capability to every
rule written about tables — including the broad allow every `roles:`-to-ABAC
migration emits — so a role holding `read` on two tables silently also held
"read any file this process can open" and "connect anywhere this process can
reach". `neq` and `not_in` EXCLUDE a type; excluding one is not naming it.

The gate is on ALLOW rules only. An unscoped DENY still matches a capability,
because a rule that takes access away must reach further than one that grants
it, never less far: a deny that stopped matching would be a widening dressed
as a restriction. The capability MODEL — which functions present which
resource, what attributes their destination carries (`path`, `url`, `host`,
`arg_<name>`), and what refuses them at the scan — is the embedded-authz arc's;
this item is only the matching rule underneath it.

**11. No new permission vocabulary.** The permissions are `read`, `write` and
`admin`, and `admin` implies the other two. A mutation needs `write` — ingest
and COPY, DML, CREATE and DROP TABLE, UDF mutation. `admin` covers the
operational endpoints, purges, and overriding a resource another identity owns.

## Where the decision is asked

The rows below cover every door the batch settled: the authentication and
policy core first, then each protocol door.


### The gRPC door

The gRPC service authorizes **per method**, explicitly, in the method body. The interceptor stays authentication-only: it proves who the caller is and stamps the identity, and decides nothing else. A method-name allowlist inside the interceptor was rejected — it would be a second place the rule lives, and it cannot be enumerated by the door census, which is what makes the per-method rule checkable.

| operation | decision asked | refusal |
|---|---|---|
| `CreateTable` | `auth.RequirePermission(provider, ctx, "write")` | `codes.PermissionDenied`, before request validation and before the catalog |
| `DropTable` | `RequirePermission(…, "write")` **and** `auth.TableAccess(ctx, provider, ResolveTableName(name), ActionWrite)` | `codes.PermissionDenied`, before the catalog and before `if_exists` |
| `DescribeTable` | `auth.TableAccess(…, ActionRead)` | `codes.PermissionDenied`, before the catalog read |
| `ListTables` | `auth.VisibleTables(ctx, provider, names)` | not a refusal: the name is absent from the listing |
| `Query`, `QueryStream` | the plan/DML enforcement inside the engine | `codes.PermissionDenied` whenever the refusal carries SQLSTATE 42501 or wraps `auth.ErrUnauthorized`, on the reject paths **and** the result-drain paths |
| health `Check` | none, by design | — |

**DDL asks two questions, and which two depends on whether the relation exists.** A mutation needs the `write` PERMISSION; a mutation of an EXISTING relation additionally needs write access to THAT RELATION. `DropTable` asks both — otherwise an identity the evaluator refuses to show one column of can destroy every row of it, the narrowest operation refused while the widest succeeds. `CreateTable` asks only the permission: the name does not resolve to a relation, so a table-scoped rule has nothing to match. PostgreSQL draws the same line.

**Code mapping.** Authentication failures are `codes.Unauthenticated` (the interceptor, before any method body). Authorization refusals are `codes.PermissionDenied`. `codes.Internal` is reserved for execution and storage failures; the door maps on the CLASS the error carries (SQLSTATE 42501, or a wrapped `auth.ErrUnauthorized`), never on message text — matching sentences would silently reopen when a message is reworded, and would be a second copy of the decision. The mapping covers the drain paths as well as the reject paths, so *where* a refusal surfaces does not change what it is called.

**Message text.** The refusal carries the shared decision's own message: `RequirePermission`'s (naming permission, identity and role) for the DDL permission check, and `TableAccess`'s `permission denied for table "x"` for the table decision, metadata and data. Naming the table is not a disclosure: the data door already names it in 42501.

**Fail-closed on a missing identity.** With auth enabled the interceptor refuses a credential-less call before any method runs, so the nil-identity arm of `RequirePermission`/`TableAccess` is a second floor rather than the first. With no provider, or auth disabled, nothing is enforced and the door behaves exactly as it did.

**PostgreSQL divergence (ADR-0012 list).** `ListTables` and `DescribeTable` follow the effective table-access decision. PostgreSQL does not: `\d` and the `pg_catalog` views show every relation to every user, and only the DATA is protected. Wadjet's position is that metadata follows the data decision — object names, column names and partition design are an exact target map for a lower-privilege caller — and the HTTP door already behaved this way. Deliberate; applies to `SHOW TABLES` / `DESCRIBE` on every door.

**Not settled by this section.**
- Query submission, status and cancellation (`SubmitQuery`, `GetQueryStatus`, `CancelQuery`) — query ownership, in "The HTTP and PostgreSQL wire doors" below.
- **Metadata follows `ActionRead`, so a WRITE-ONLY role sees no metadata.** `allow: [write]` with no `read` gets an empty `ListTables` and a `PermissionDenied` `DescribeTable` on gRPC, while the HTTP door still answers for the same identity until it moves to the shared helper. Whether a role that may write a table should see its schema is open.
- DDL sent as SQL text through `Query` takes the engine's statement path, not this table (#939).

---

### The HTTP and PostgreSQL wire doors

#### The HTTP door

| operation | route | who may | refusal |
|---|---|---|---|
| run a query | POST /v1/queries, POST /v1/queries/async | any authenticated identity, subject to the table decision per relation | 403 |
| query status / results | GET /v1/queries/{id}, .../results | the query's OWNER, or admin | 403 permission denied: query "<id>" belongs to another principal |
| cancel a query | DELETE /v1/queries/{id} | owner or admin | 403, and the query keeps running |
| list queries | GET /v1/queries | the caller's own; every USER query for admin; internal stage entries never | — |
| delete a query's result files | DELETE /v1/results/{id} | owner or admin (an ID the tracker no longer holds: admin) | 403 |
| purge stale results | POST /v1/results/cleanup | admin | 403 unauthorized: "admin" permission required ... |
| read / purge the DLQ | GET/DELETE /v1/dlq, GET /v1/dlq/{id} | admin | 403, same text, and the check precedes the "no DLQ configured" shortcut |
| list workers | GET /v1/workers | admin | 403 |
| runtime config, keys, roles, policies, tuning | /v1/admin/... | admin | 403 |
| list tables | GET /v1/tables | auth.VisibleTables — the effective table decision, per name | the name is absent, not refused |
| describe a table | GET /v1/tables/{name} | auth.TableAccess(ActionRead) on the CATALOG-RESOLVED name | 403 permission denied for table "x" |
| SHOW TABLES / DESCRIBE | POST /v1/queries | the same two decisions (#941) | as above |
| run a SELECT on a denied relation | POST /v1/queries | the table decision, per base relation | 403 permission denied for table "x" — the decision's own text, not a door-local wording |
| CREATE TABLE | POST /v1/tables, CREATE TABLE | write permission | 403 unauthorized: "write" permission required ... |
| DROP / ANALYZE an existing table | DELETE /v1/tables/{name}, DROP TABLE, ANALYZE TABLE | write permission AND auth.TableAccess(ActionWrite) on the resolved name | 403 |
| liveness | GET /v1/health | anyone, no credential (a liveness probe has none) | — |
| profiling | /debug/pprof/* | admin | 403 (401 without a credential) |
| Prometheus scrape | /metrics | NOT SETTLED: any authenticated identity, as before this batch | 401 without a credential |

#### The pgwire door

| operation | who may | refusal |
|---|---|---|
| COPY ... FROM STDIN | auth.TableAccess(ActionWrite) on the resolved table AND the identity's column policy over the COPY column list (INSERT's rule, auth.EnforceDMLPolicies) | SQLSTATE 42501 permission denied for table "x" (or 42703 for a denied column), sent INSTEAD of CopyInResponse; no row is consumed, the ingester is never constructed, the connection stays in the message loop |
| is_superuser ParameterStatus | reports HasPermission(identity, "admin") | — (with no provider the session is unrestricted and it stays on) |

#### The positions these rows encode

1. Authorization is per OPERATION, never per door. The query endpoints on the
   HTTP mux accept ordinary identities, so authentication middleware can never
   be the place an operational permission is checked. The comment claiming
   otherwise (handleDeleteResults) was the whole of #937's defense.
2. A resource created under an identity is OWNED by it. A query's status, SQL,
   results, cancellation and result files are the submitter's and an
   administrator's. An entry with no recorded owner is the administrator's:
   nobody can claim what nobody owns. Listings are filtered by the same rule
   that governs reading one entry, because a listing publishes what reading one
   publishes.
3. A handle is not a capability. Query IDs are full UUIDs; the eight-hex prefix
   was 32 bits, guessable by an identity that may run queries.
4. The refusal has one class and one text per operation. HTTP 403 body == gRPC
   PermissionDenied message == pgwire 42501 message. A refusal is never a 404:
   the data door already names the table it refuses, and hiding one resource
   behind "not found" while naming another is two answers to one question.
5. Metadata follows the effective table decision — a deliberate divergence from
   PostgreSQL, which shows \d to anyone (ADR-0012's divergence list). The HTTP
   door already behaved this way under the legacy role rule; it now asks the
   shared decision, so an ABAC deny governs it too.
6. DDL on an EXISTING relation asks the table decision, not only the
   permission. `write` says the identity may write something;
   TableAccess(..., ActionWrite) says it may write THIS. CREATE is
   permission-only, because minting a name decides nothing about an existing
   relation — PostgreSQL treats CREATE as a schema privilege for the same
   reason.

#### PostgreSQL divergences recorded (for ADR-0012's list)

- Metadata visibility follows the table-access decision: SHOW TABLES /
  GET /v1/tables omit a relation the identity may not read, and DESCRIBE /
  GET /v1/tables/{name} refuse 42501 where PostgreSQL's \d answers anyone.
  Deliberate.
- COPY ... FROM STDIN refuses 42501 before CopyInResponse; PostgreSQL refuses
  with 42501 too — same class, same point in the protocol.
- is_superuser reports a permission, not a role name. PostgreSQL reports
  rolsuper; the analogue here is the admin permission.

### The embedded API and the SQL statement door

**The embedded API and the PostgreSQL wire protocol are one door.** `wadjet.DB.Query`'s statement switch is not an implementation detail of the embedded API: pgwire's non-SELECT path *is* that call with the connection's identity on the context, and the standalone gRPC `Query` RPC reaches it too. Every decision for a statement that switch dispatches belongs in its handlers, not in a frontend — before this batch the HTTP door alone checked DDL and metadata, so the product's own rule held on one door out of four.

| operation | permission | additional decision | refusal |
|---|---|---|---|
| `CREATE TABLE` | `write` | — (no relation yet; PG treats CREATE as a schema privilege) | 42501 / 403 |
| `DROP TABLE`, `ANALYZE` | `write` | `TableAccess(ActionWrite)` on the relation | 42501 / 403 |
| `CREATE`/`DROP FUNCTION` | `write` | `admin` to override another owner's `WITH LOCK` | 42501 / 403 |
| `SHOW FUNCTIONS` | — | an authenticated identity | 42501 / 403 |
| `DESCRIBE`, `SHOW COLUMNS FROM` | — | `TableAccess(ActionRead)` | 42501 / 403 |
| `SHOW TABLES` | — | `VisibleTables` filters the listing | never a refusal |
| a table function in `FROM` | — | the `table_function` capability | 42501 / 403 |
| `CREATE`/`DROP`/`ALTER ALERT` | `admin` | — | 42501 / 403 |

With auth enabled a context carrying no identity is refused at every row; with no provider nothing is enforced and nothing changes.

**UDF ownership.** `expr.DefaultUDFs` is process-global — replacing a function changes what other identities' queries mean. `WITH LOCK` records the creating identity; only that owner or an identity holding `admin` may replace or drop it. Administrator status comes from `Authorizer.HasPermission(id, "admin")`, never from the role's NAME: both directions were wrong before (a role named `admin` with only `read` overrode a lock; a role named `ops` holding `admin` was refused), and `DB.Query` was worse still, passing a literal `isAdmin = true` so the lock did not exist on its doors. `DROP FUNCTION IF EXISTS` forgives an ABSENT function (42883) and never one that is present and locked by somebody else — "does not exist (no-op)" there is a lie about the registry that also hides the refusal. `SHOW FUNCTIONS` stays readable by any authenticated identity; **PG-consistent**, verified on the oracle server where a role with no privileges reads `pg_proc.prosrc` and prints the definition with `\sf`.

**A table function is a capability, and an external read is not a relation.** `read_csv`, `read_json`, `read_parquet`, `postgres_scan`, `postgres_query`, `mysql_scan`, `mysql_query` read what the catalog does not hold. `PolicedScanTables` and `StatementBaseTables` both skip a function scan — correctly, since a column policy binds to a relation's schema — and the consequence was that such a scan was not a resource of ANY kind. It is one now: `Resource{Type:"table_function", Name:"<func>", Attributes:{path (~/-expanded, Clean'd), url, host, arg_<k>}}` evaluated with `ActionRead`; the connection string is never an attribute because it carries a password. Default DENY under auth (deny-overrides' closed world doing its job), `admin` under legacy roles, unchanged with auth disabled. Enforcement is in two places and both are load-bearing: the plan-time pass so a denial opens no file and sends no request, and the context guard `physical.buildScan` asks, which covers the scalar/`IN`/`EXISTS` subquery and CTE body that are SQL *text* when the statement's plan is enforced. **PG divergence and precedent**: PostgreSQL has no analogue, but its equivalent primitives are privileged — an ordinary role gets `42501 permission denied for function pg_read_file`, and `COPY … FROM PROGRAM` answers `42501 permission denied to COPY to or from an external program` (only `pg_execute_server_program`). Server-side file and program access being a privilege is PostgreSQL's own position.

**Metadata follows the effective table decision — a deliberate PG divergence.** `DESCRIBE`, `SHOW COLUMNS FROM` and `SHOW TABLES` ask what the data door asks: `TableAccess` for a named relation, `VisibleTables` for a listing, on the catalog-resolved spelling so a policy bound to `Ledger` polices `DESCRIBE ledger`. Explicit ABAC denies govern, which legacy `CanAccessTable`/`FilterTables` cannot express (`tables:["*"]` says yes to everything). **PostgreSQL says otherwise and we diverge on purpose** — measured: `\d` and `\dt` as a role with no privileges print the full column list and all 95 tables. Metadata visibility is a product decision, not a wire-compatibility one; an entry in ADR-0012's divergence list. **Not covered, recorded rather than implied**: a client introspecting through `pg_catalog` is answered by `internal/server/pgwire/catalog_rows.go`, which is not filtered.


| door | operation | decision asked | refusal |
|---|---|---|---|
| all | SELECT, per policed relation in the plan | `EnforcePlanPolicies` → the shared decision, `ActionRead`, in both provider shapes; the evaluator then supplies the obligations | 42501 `permission denied for table "x"` |
| all | column binding (`SELECT nosuchcol`), per relation | `ValidateStatementColumns` → the same resolver, same environment | 42703, over the schema the identity can see |
| all | INSERT / UPDATE / DELETE / MERGE | `EnforceDMLPolicies` → the shared decision, `ActionWrite` on the target — plus `ActionRead` when the statement observes it (a WHERE or SET naming a column, or MERGE) | 42501 `permission denied for table "x"` |
| all | a scan the resolved set never saw (decorrelation, late passes) | the resolver's `lookup` — the same decision | 42501 |
| all | CREATE / DROP ALERT | `RequirePermission(provider, ctx, "admin")` | authorization error, rendered per door |
| HTTP | authentication | `ProviderMiddleware` → `Authenticator.Authenticate`; the trusted environment is attached here | 401 |
| pgwire | authentication | `authenticate` → `AuthenticateToken`; the environment is attached in `queryContext`, which is the context enforcement reads | 28000 / 28P01 |
| gRPC | authentication | `grpcAuthenticateContext` → `AuthenticateToken`; the environment is attached from `peer.FromContext` | `codes.Unauthenticated` |
| MCP | session authentication | `resolveMCPAuth` → `AuthenticateToken`; refuses to start without a credential when auth is enabled | startup refusal |
| startup | the whole auth block | `auth.Build` → `buildAuth` → `buildProviderFromConfig` | the process does not start |
| hot reload | the whole auth block and policy set | `Provider.UpdateFromConfig` → `auth.Build` + `ValidateABACPolicies` + `installState` | refused; the running state keeps serving |

## PostgreSQL divergences (added to ADR-0012's list)

1. **Metadata visibility follows the effective table-access decision.**
   PostgreSQL shows `\d` and `information_schema` to anyone who can connect;
   Wadjet does not. A relation an identity may not read is not listed by
   `SHOW TABLES`, and `DESCRIBE` on it refuses. The HTTP door already behaved
   this way; the position is now the product's rather than one door's, and
   `auth.VisibleTables` is how every door implements it. The reasoning: the
   deployments this product is built for treat a table's NAME as sensitive —
   `classified_events` names an operation whether or not its rows can be read —
   and there is no PostgreSQL client behaviour that depends on seeing a
   relation it cannot select from.

2. **A condition attribute with no namespace is a load error.** PostgreSQL has
   no equivalent; this is Wadjet's own configuration surface. The alternative,
   which shipped, was that an unnamespaced attribute was silently reinterpreted
   as a subject attribute nothing populates — a rule that never matches, which
   beside a broad allow is a grant.

3. **`auth.enabled: true` with no credential mechanism refuses startup.**
   PostgreSQL would start with whatever `pg_hba.conf` says, including `trust`.
   Wadjet treats "the operator asked for authentication and it cannot be
   performed" as a configuration error rather than a mode.

## Not settled here

- **Per-column DDL privileges.** `write` is whole-relation. A privilege model
  that distinguishes ALTER from INSERT, or grants a column at a time, is a
  vocabulary extension and needs its own decision.
- **GRANT / REVOKE statements.** Authorization is configuration today —
  `roles:` and `abac_policies:` in the config file, hot-reloadable. Making it
  SQL-mutable means a privilege catalog, its own DDL, and a durability and
  distribution story for it.
- **X-Forwarded-For / trusted-proxy configuration.** `env.source_ip` is the
  peer address the server observed. Behind a reverse proxy that is the proxy's
  address, and policing the real client needs a trusted-proxy setting with a
  hop count — a configuration surface that is itself a security control, and
  one that is worse than useless if it is added carelessly.

## Gates

- `internal/auth/table_access_test.go` — `TestTableAccessDecidesEveryCell`:
  {evaluator, legacy} × {allow, explicit deny, no rule, nil identity, provider
  disabled} × {read, write}, each refusal asserted as SQLSTATE 42501 with
  PostgreSQL's message; plus the missing-identity, unbound-policy-set,
  decision-time-clock and attached-environment cells, and the `VisibleTables`
  filter on both arms including its no-auth pass-through.
- `internal/auth/coarse_gate_test.go` —
  `TestTheRolesAllowListIsACoarseGateUnderABAC` (a policy cannot widen a role,
  and still narrows one that holds the permission),
  `TestTheCoarseGateLetsAdminThrough`,
  `TestAnIdentityWithNoPermissionsIsRefusedBeforeThePolicy` — item 5's coarse
  gate — and `TestAMissingIdentityIsRefusedOnTheDataPathsToo` for item 7, with
  its control that a nil provider still enforces nothing.
- `internal/auth/capability_scope_test.go` —
  `TestAnUnscopedAllowDoesNotGrantACapability` (the table it was written for is
  still allowed; the capability is refused),
  `TestARuleThatNamesTheCapabilityGrantsIt` (`eq` and `in`),
  `TestACapabilityRuleStillHonoursItsOtherConditions` (a destination scope
  still decides once the type is named), `TestAnUnscopedDenyStillReachesACapability`,
  `TestExcludingATypeIsNotNamingIt`, `TestAnUntypedResourceIsNotACapability`,
  and `TestTheValidatorAcceptsTableFunctionResourceAttributes` — item 10.
- `internal/auth/refusal_class_test.go` —
  `TestADeniedReadAndADeniedWriteRefuseInTheSameClass`: the same identity's
  read and write on the same relation carry the same SQLSTATE and the same
  message, and the shared decision agrees with both.
- `internal/auth/abac_vocabulary_test.go` —
  `TestPolicySchemaRejectsUnknownSecurityWords` over all five fields,
  `TestPolicySchemaAcceptsTheWholeImplementedVocabulary` (the other side: a
  refusal that swallowed valid policies would be the worse defect),
  `TestUnknownEffectResolvesToDenyNotAllow`,
  `TestUnknownObligationRefusesTheTable`, and the hot-reload cell.
- `internal/auth/build_error_test.go` and `cmd/wadjet/auth_config_error_test.go`
  — a configuration that cannot be built refuses at startup and on reload, and
  the Authenticator it produces is ENABLED and refuses every credential.
- `internal/auth/environment_enforcement_test.go` and
  `cmd/wadjet/abac_condition_namespace_test.go` — the environment reaches the
  shared paths, the attached address loses its port, and a condition keeps its
  namespace from a real YAML file through `config.Load` to a decision.
- `internal/server/policy_environment_door_test.go` — the door census for the
  environment: an `env.hour` deny refuses on embedded, pgwire, HTTP and gRPC;
  an `env.source_ip` deny refuses on the network doors; the DML door is its own
  cell; and `env.protocol` names the door. Every cell has a CONTROL arm whose
  deny cannot match, so a refusal proves the environment rather than the
  harness.
- `internal/server/auth_config_error_door_test.go` — pgwire, gRPC and HTTP all
  refuse under a broken auth configuration, and all still serve under a working
  one.
- `internal/server/legacy_role_enforcement_door_test.go` — the `roles:`-only
  shape authorizes reads and writes on embedded and pgwire, the refused DELETE
  leaves the row in place, and what `VisibleTables` lists is what the data path
  lets the identity read.
- `internal/server/denied_read_class_door_test.go` — a denied SELECT and a
  denied DELETE both leave pgwire as SQLSTATE 42501 with PostgreSQL's message.
- `internal/auth/attach_sites_test.go` (ADR-0033) stays the source census: a
  provider reaches a catalog through the binding function and through nothing
  else.


