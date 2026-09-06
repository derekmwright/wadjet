# ADR-0034: Every door authorizes before it acts

Status: Accepted
Date: 2026-09-06
Issues: #930 #931 #932 #933 (SEC1); the SEC2/SEC3/SEC4 door arcs of the same batch

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
policy narrows what a role may do and never widens it.** That did not hold —
under an explicit `abac_policies:` block the evaluator answered alone, so a
role written `allow: [read]` that a policy permitted to write could write,
while DDL on the same door demanded the permission. `admin` still grants
everything, because `HasPermission` says so. Both halves remain required in the
legacy shape: a permission alone is not access to a relation, and a relation
alone is not permission to write it.

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

**7. Fail closed on a missing identity — on the DATA paths too.** With auth
ENABLED, a context carrying no identity is refused at every boundary: metadata,
DDL, the shared table-access decision, AND the shared plan and DML paths. The
last two used to return early and enforce nothing, so an embedded caller that
attached a provider and then queried without stamping an identity could SELECT
and INSERT while DESCRIBE and DDL refused it — one boundary, two answers. A
caller that runs under a provider stamps an identity (a scheduled alert does,
through `auth.StampDefiner`).

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

Filled for SEC1. The other three arcs' rows arrive from the coordinator at
landing.

<!-- COORDINATOR: fold in the SEC2 / SEC3 / SEC4 rows here at landing. -->

| door | operation | decision asked | refusal |
|---|---|---|---|
| all | SELECT, per policed relation in the plan | `EnforcePlanPolicies` → evaluator, or `TableAccess(ActionRead)` when no evaluator is installed | 42501 `permission denied for table "x": <reason>` |
| all | column binding (`SELECT nosuchcol`), per relation | `ValidateStatementColumns` → the same resolver, same environment | 42703, over the schema the identity can see |
| all | INSERT / UPDATE / DELETE / MERGE | `EnforceDMLPolicies` → evaluator (ActionWrite, then the read decision), or `TableAccess(ActionWrite)` then `TableAccess(ActionRead)` | 42501 `permission denied for table "x"` |
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
