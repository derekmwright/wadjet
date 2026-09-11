# Auth shared table access

Source: internal/auth/table_access.go — func TableAccess(ctx context.Context, provider *Provider, table string, action Action) error {, moved 2026-09-11 (#1026)

TableAccess is the ONE effective table-access decision every door asks for a
catalog table it is about to read metadata of or act on.

It exists because the doors did not agree. `SHOW TABLES` filtered on the
HTTP door and on no other; `DESCRIBE` refused on one door and answered on
the rest; a DDL statement asked `HasPermission` and never asked whether the
identity may touch THAT relation. Each of those is a separate reading of the
same question, and a security decision with several readings has the weakest
one for its answer. The decision lives here now, and a door that
re-implements it has forked it.

The rule, in order:

  - Provider nil, or auth disabled: nil. There is nothing to enforce
    (dev, embedded without SetAuthProvider), and nothing changes.
  - A policy set that could not be BOUND to the catalog: refused. An
    unbindable set enforces nothing, and a rule that matches nothing is a
    grant beside a broad allow (#882, ADR-0033 rule 3).
  - Auth enabled with NO identity in the context: refused. Authentication
    proves who; a door that reaches here with nobody has no one to
    authorize.
  - The role's `allow` list, in BOTH provider shapes:
    `HasPermission(id, perm(action))`. It is a COARSE GATE — a policy
    narrows what a role may do and never widens it — and `admin` grants
    everything, as it always has.
  - Then, with an ABAC evaluator installed: the EVALUATOR decides
    (`EvaluateTableAccess`), which is deny-overrides with a default deny —
    explicit denies win, an unmatched request is refused.
  - With no evaluator: `CanAccessTable(id, table)`, the other half of the
    legacy rule. The permission alone is not access to a relation, and the
    relation alone is not permission to write it.

`table` must be the CATALOG-RESOLVED spelling (`catalog.ResolveTableName`):
an unquoted identifier folds at the lexer (#731), and a policy bound to
`Users` must police a statement that spelled it `users`.

The environment comes from the context — attached at the protocol boundary,
never from a caller below it — through the one builder every enforcement
path uses (`DecisionEnvironment`), so `Time` is stamped at DECISION time. A
pgwire connection lives for hours; an `env.hour` condition means the hour
the statement ran.

The refusal is a `sqlerr` 42501, which every door renders in its own class:
pgwire SQLSTATE 42501, HTTP 403, gRPC codes.PermissionDenied, and the same
error verbatim on the embedded API. Its TEXT is PostgreSQL's and carries
nothing else — not the rule that denied, not the reason. Which rule fired is
operator information and goes to the audit log; telling the refused caller
is a disclosure, and a policy rule id names the control that stopped them.
