# Auth table function capabilities

Source: internal/auth/table_func_enforce.go — func AuthorizeTableFunction(ctx context.Context, provider *Provider, protocol string,, moved 2026-09-11 (#1026)

AuthorizeTableFunction is the decision for ONE table-function scan: may this
identity read this destination?

A table function is a CAPABILITY, not a relation. `read_csv('/etc/shadow')`
reads a file off the server's own disk; `read_json('http://10.0.0.5/x')`
makes the server issue an HTTP request from inside the network perimeter;
`postgres_query(dsn, sql)` opens an outbound database connection to wherever
the caller points it. None of these is in the catalog, so before #943 none
of them became an ABAC Resource: `logical.PolicedScanTables` and
`StatementBaseTables` both skip a function scan, and a role restricted to
one catalog table could still read any file the process could read.

The rule, with auth ENABLED, is DEFAULT DENY:

  - No provider / auth disabled: allowed, unchanged. The CLI, the embedded
    engine without SetAuthProvider and every existing no-auth test read
    files exactly as they did — that is the documented single-user use.
  - A pure function (generate_series, unnest): allowed. It opens nothing.
  - No identity under auth enabled: refused.
  - An ABAC evaluator installed: the evaluator decides, over
    `Resource{Type: "table_function", Name: <func>, Attributes: {path, url,
    host}}` with ActionRead. Deny-overrides with a default deny, so a
    deployment that has never written a rule about table functions refuses
    them.
  - Legacy roles only (no evaluator): the `admin` permission. There is no
    way to spell "may read server files" in the `read`/`write`/`admin`
    vocabulary, and reading arbitrary server-local files is an
    administrator's capability. No new permission words (ADR-0034).

PostgreSQL's precedent for the default: `pg_read_file` is superuser-only and
answers `42501 permission denied for function pg_read_file` to an ordinary
role, and `COPY … FROM PROGRAM` needs the `pg_execute_server_program` role
(both verified on the oracle server). Server-side file and program access is
a privilege there too.
