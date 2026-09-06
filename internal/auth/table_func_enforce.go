package auth

import (
	"context"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The ABAC resource TYPEs. `ResourceTable` is what `EvaluateTableAccess`
// stamps for a catalog relation; `ResourceTableFunction` is what a
// table-function scan presents itself as, and it is not a relation — the
// policy binder must not resolve its `resource.name` against the catalog, and
// a rule written for tables must not reach it by breadth.
const (
	ResourceTable         = "table"
	ResourceTableFunction = "table_function"
)

// pureTableFunctions are the table functions that open NOTHING — no file, no
// URL, no database connection. They compute over their own arguments, so they
// are not a capability and are not policed.
//
// The list is a DENY-list inverted on purpose: anything not named here is
// governed, so a table function added later is closed by default rather than
// open until somebody remembers this file. `buildTableFunctionSource` today
// dispatches read_csv / read_csv_auto / read_json / read_json_auto /
// read_parquet / postgres_scan / postgres_query / mysql_scan / mysql_query
// (all external) plus generate_series, and `buildScan` handles unnest.
var pureTableFunctions = map[string]bool{
	"generate_series": true,
	"unnest":          true,
}

// AuthorizeTableFunction is the decision for ONE table-function scan: may this
// identity read this destination?
//
// A table function is a CAPABILITY, not a relation. `read_csv('/etc/shadow')`
// reads a file off the server's own disk; `read_json('http://10.0.0.5/x')`
// makes the server issue an HTTP request from inside the network perimeter;
// `postgres_query(dsn, sql)` opens an outbound database connection to wherever
// the caller points it. None of these is in the catalog, so before #943 none
// of them became an ABAC Resource: `logical.PolicedScanTables` and
// `StatementBaseTables` both skip a function scan, and a role restricted to
// one catalog table could still read any file the process could read.
//
// The rule, with auth ENABLED, is DEFAULT DENY:
//
//   - No provider / auth disabled: allowed, unchanged. The CLI, the embedded
//     engine without SetAuthProvider and every existing no-auth test read
//     files exactly as they did — that is the documented single-user use.
//   - A pure function (generate_series, unnest): allowed. It opens nothing.
//   - No identity under auth enabled: refused.
//   - An ABAC evaluator installed: the evaluator decides, over
//     `Resource{Type: "table_function", Name: <func>, Attributes: {path, url,
//     host}}` with ActionRead. Deny-overrides with a default deny, so a
//     deployment that has never written a rule about table functions refuses
//     them.
//   - Legacy roles only (no evaluator): the `admin` permission. There is no
//     way to spell "may read server files" in the `read`/`write`/`admin`
//     vocabulary, and reading arbitrary server-local files is an
//     administrator's capability. No new permission words (ADR-0034).
//
// PostgreSQL's precedent for the default: `pg_read_file` is superuser-only and
// answers `42501 permission denied for function pg_read_file` to an ordinary
// role, and `COPY … FROM PROGRAM` needs the `pg_execute_server_program` role
// (both verified on the oracle server). Server-side file and program access is
// a privilege there too.
func AuthorizeTableFunction(ctx context.Context, provider *Provider, protocol string,
	funcName string, args []string, namedArgs map[string]string) error {
	if provider == nil || !provider.Enabled() {
		return nil
	}
	if pureTableFunctions[strings.ToLower(funcName)] {
		return nil
	}
	if err := provider.BindError(); err != nil {
		return sqlerr.Wrap("42501", err)
	}
	id := IdentityFromContext(ctx)
	if id == nil {
		return refuseTableFunction(funcName, "authentication required")
	}
	if ev := provider.Evaluator(); ev != nil {
		// The ONE environment builder every shared enforcement path uses, so
		// an `env.*` condition on a `table_function` resource means exactly
		// what it means on a table: the environment the PROTOCOL BOUNDARY
		// attached, with `Time` stamped at DECISION time rather than at
		// attach time (#933).
		env := DecisionEnvironment(ctx, protocol)
		res := TableFunctionResource(funcName, args, namedArgs)
		if d := ev.Evaluate(id.ToSubject(), res, ActionRead, env); d.Allowed {
			return nil
		}
		return refuseTableFunction(funcName, "no policy grants this identity the "+
			ResourceTableFunction+" capability")
	}
	if authz := provider.Authorizer(); authz != nil && authz.HasPermission(id, "admin") {
		return nil
	}
	return refuseTableFunction(funcName, `the "admin" permission is required`)
}

// refuseTableFunction is the one refusal shape. It names the FUNCTION and not
// its arguments: a connection string carries a password, and echoing the
// destination back adds nothing the caller does not already know.
func refuseTableFunction(funcName, why string) error {
	return sqlerr.New("42501", "permission denied for table function %q: %s", funcName, why)
}

// TableFunctionResource is the ABAC resource one table-function scan presents.
//
// The attributes are derived from the ARGUMENTS, before anything is opened, so
// a policy can scope the capability to a destination:
//
//	path  a local filesystem path or glob — with a leading "~/" expanded and
//	      the result cleaned, so `~/x`, `/data/../etc/passwd` and
//	      `/etc/passwd` are not three different strings a prefix rule has to
//	      know about. Cleaning is safe because the cleaned and uncleaned forms
//	      open the same file.
//	url   an http(s) source, verbatim.
//	host  the host of `url`, or the host of a database connector's connection
//	      string. Empty when the connection string cannot be parsed — which,
//	      under a default-deny evaluator, refuses.
//
// The connection STRING itself is never an attribute: it carries a password.
func TableFunctionResource(funcName string, args []string, namedArgs map[string]string) Resource {
	name := strings.ToLower(funcName)
	attrs := Attributes{}
	switch name {
	case "postgres_scan", "postgres_query", "mysql_scan", "mysql_query":
		if len(args) > 0 {
			if h := connStringHost(args[0]); h != "" {
				attrs["host"] = h
			}
		}
	default:
		if len(args) > 0 {
			target := args[0]
			if isHTTPSource(target) {
				attrs["url"] = target
				if u, err := url.Parse(target); err == nil {
					attrs["host"] = u.Hostname()
				}
			} else {
				attrs["path"] = cleanTableFuncPath(target)
			}
		}
	}
	for k, v := range namedArgs {
		// Named arguments are the reader's options (delimiter, header). They
		// are exposed under their own names so a policy CAN read them, and
		// they cannot shadow the three above.
		switch k {
		case "path", "url", "host":
		default:
			attrs["arg_"+k] = v
		}
	}
	return Resource{Type: ResourceTableFunction, Name: name, Attributes: attrs}
}

func isHTTPSource(s string) bool {
	return strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://")
}

// cleanTableFuncPath resolves a leading "~/" and cleans the result, matching
// what `physical.expandHome` does before the file is opened. A policy that
// allowed `/data/x.csv` must not be walked around by writing `~/../data/x.csv`.
func cleanTableFuncPath(path string) string {
	if strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil && home != "" {
			path = filepath.Join(home, path[2:])
		}
	}
	return filepath.Clean(path)
}

// connStringHost is the host:port a database connector's connection string
// points at, for both spellings the drivers accept: a URL
// (`postgres://u:p@host:5432/db`) and libpq/mysql key-value pairs
// (`host=db.internal port=5432 ...`). Unparseable → "".
func connStringHost(conn string) string {
	if i := strings.Index(conn, "://"); i > 0 {
		u, err := url.Parse(conn)
		if err != nil || u.Host == "" {
			return ""
		}
		return u.Host
	}
	var host, port string
	for _, field := range strings.Fields(conn) {
		k, v, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		switch strings.ToLower(k) {
		case "host":
			host = v
		case "port":
			port = v
		}
	}
	if host == "" {
		return ""
	}
	if port == "" {
		return host
	}
	return net.JoinHostPort(host, port)
}

// enforceTableFunctionScans decides every table-function scan the plan carries
// and installs the guard the LATER plans ask.
//
// Two halves, and both are needed:
//
//   - The pass over this plan refuses at PLAN time, before a pipeline is
//     built, so a denied `read_csv` never opens the file and a denied
//     `read_json('http://…')` never sends the request. That is the property
//     the gate asserts with a path that would error if opened and with an
//     httptest server that fails the test if it is hit.
//   - The guard on the context covers the plans this one does not contain. A
//     scalar, IN or EXISTS subquery is SQL TEXT here and becomes a second plan
//     inside the physical planner (`buildSubqueryPipelineFor`), as does a CTE
//     body; the optimizer also MINTS scans while decorrelating. Every one of
//     those reaches `buildScan`, which asks the guard.
func enforceTableFunctionScans(ctx context.Context, provider *Provider, plan *logical.Node,
	protocol string) (context.Context, error) {
	for _, s := range logical.TableFuncScans(plan) {
		if err := AuthorizeTableFunction(ctx, provider, protocol, s.FuncName, s.Args, s.NamedArgs); err != nil {
			return ctx, err
		}
	}
	guard := func(funcName string, args []string, namedArgs map[string]string) error {
		return AuthorizeTableFunction(ctx, provider, protocol, funcName, args, namedArgs)
	}
	return logical.ContextWithTableFuncGuard(ctx, guard), nil
}
