package logical

import "context"

// TableFuncGuard decides whether the calling identity may run one table
// function against the destination its arguments name. It returns nil to allow
// and a refusal (a `sqlerr` 42501) to deny.
//
// It travels on the CONTEXT rather than being called directly because the
// decision lives in `internal/auth`, which imports this package and the
// physical planner — an authorization call from a planner would be an import
// cycle. This is the same shape `ContextWithPolicyLookup` uses for column
// policies, and for the same reason.
type TableFuncGuard func(funcName string, args []string, namedArgs map[string]string) error

type tableFuncGuardKey struct{}

// ContextWithTableFuncGuard attaches the guard. `auth.EnforcePlanPolicies`
// installs it once per statement, so every plan built under that context —
// the statement's own, and the SEPARATE plans the physical planner builds for
// a scalar / IN / EXISTS subquery and for a CTE body — asks the same question.
//
// A subquery is SQL TEXT when the statement's plan is enforced, so a pass over
// that plan alone cannot see the table function inside one: `SELECT (SELECT
// COUNT(*) FROM read_csv('/etc/passwd'))` carries no Scan for the file until
// the physical planner re-parses the subquery and builds a second plan. That
// is exactly how #859's column policies were bypassed, one construct over.
func ContextWithTableFuncGuard(ctx context.Context, g TableFuncGuard) context.Context {
	if g == nil {
		return ctx
	}
	return context.WithValue(ctx, tableFuncGuardKey{}, g)
}

// TableFuncGuardFromContext returns the guard, or nil when none was installed
// — which is the case with no auth provider, or with auth disabled, and means
// there is nothing to enforce.
func TableFuncGuardFromContext(ctx context.Context) TableFuncGuard {
	g, _ := ctx.Value(tableFuncGuardKey{}).(TableFuncGuard)
	return g
}

// TableFuncScan describes one table-function scan in a plan: the function and
// the arguments that name what it will open.
type TableFuncScan struct {
	FuncName  string
	Args      []string
	NamedArgs map[string]string
}

// TableFuncScans lists every table-function scan in the plan, in walk order,
// once per NODE (not deduplicated by name — two `read_csv` scans over
// different paths are two decisions).
//
// It is the counterpart of PolicedScanTables, which deliberately EXCLUDES
// these nodes: a column policy binds to a catalog relation and a table
// function has none, so it has no columns for a security projection to police.
// That exclusion stays. What it left behind is that a table-function scan was
// not a resource of ANY kind, so no policy could reach it at all (#943); this
// gives it its own.
func TableFuncScans(n *Node) []TableFuncScan {
	var out []TableFuncScan
	var walk func(*Node)
	walk = func(x *Node) {
		if x == nil {
			return
		}
		if x.Type == NodeScan && x.IsTableFunc && x.FuncName != "" {
			out = append(out, TableFuncScan{
				FuncName:  x.FuncName,
				Args:      x.FuncArgs,
				NamedArgs: x.FuncNamedArgs,
			})
		}
		for _, c := range x.Children {
			walk(c)
		}
	}
	walk(n)
	return out
}
