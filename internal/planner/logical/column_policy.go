package logical

import (
	"context"
	"errors"
	"strings"
)

// ErrColumnPolicyUnenforceable is returned when a column policy names a table
// the plan reads but the security projection could not be built for it — no
// catalog schema and no scan annotation to build the projection from, or every
// column denied.
//
// It is an ERROR and not a silently unmasked answer on purpose: a security
// control that cannot be applied refuses, the way an unreadable `columns:`
// action refuses to load (#802). Loud beats plausible.
var ErrColumnPolicyUnenforceable = errors.New("column policy could not be enforced on this plan: refusing to answer unmasked")

// TablePolicies is the set of column policies in force for one query, keyed by
// the FOLDED base-table name.
//
// It travels on the context because a query is planned in more than one place.
// The statement's own plan is enforced by auth.EnforcePlanPolicies before the
// optimizer runs, but an expression subquery — `(SELECT MAX(ssn) FROM t)`, an
// IN set, an EXISTS — is a WHOLE SECOND QUERY that the physical planner
// parses, builds and optimizes on its own (physical.buildSubqueryPipeline),
// and it never passed through the enforcement path at all. Before #859 that
// second path answered from the raw column while the first one masked.
//
// The context is the only carrier both paths already share.
type TablePolicies map[string][]ColumnPolicy

type columnPolicyKey struct{}

// ContextWithColumnPolicies returns ctx carrying the column policies in force.
// A nil or empty map returns ctx unchanged, so an unpoliced query costs
// nothing and every reader can test for absence with len().
func ContextWithColumnPolicies(ctx context.Context, tp TablePolicies) context.Context {
	if len(tp) == 0 {
		return ctx
	}
	return context.WithValue(ctx, columnPolicyKey{}, tp)
}

// ColumnPoliciesFromContext returns the column policies in force, or nil.
func ColumnPoliciesFromContext(ctx context.Context) TablePolicies {
	if ctx == nil {
		return nil
	}
	tp, _ := ctx.Value(columnPolicyKey{}).(TablePolicies)
	return tp
}

// PlanCarriesPolicyEnforcement reports whether this plan carries anything a
// policy put there: a security projection, or a row filter injected by
// row-level security.
//
// A caller that is about to hand a query to something that will RE-PLAN it
// from the SQL TEXT — the coordinator's async door dispatches a
// TaskTypePipeline task carrying `SQLText`, and the worker parses, builds and
// optimizes it again with no policy in reach — has to ask this first. The
// enforced plan does not survive that hop, and answering from the re-planned
// one hands the caller the stored values.
func PlanCarriesPolicyEnforcement(n *Node) bool {
	if n == nil {
		return false
	}
	if (n.Type == NodeProject && n.SecurityBarrier) || (n.Type == NodeFilter && n.PolicyFilter) {
		return true
	}
	for _, c := range n.Children {
		if PlanCarriesPolicyEnforcement(c) {
			return true
		}
	}
	return false
}

// For returns the policies for one table, matched case-insensitively.
func (tp TablePolicies) For(table string) []ColumnPolicy {
	if len(tp) == 0 || table == "" {
		return nil
	}
	return tp[strings.ToLower(table)]
}

// DeniedColumns maps each policed table (folded) to its denied columns
// (folded). These are the columns that, for this identity, DO NOT EXIST: a
// reference to one is 42703, not a NULL and not a mask.
func (tp TablePolicies) DeniedColumns() map[string]map[string]bool {
	if len(tp) == 0 {
		return nil
	}
	out := make(map[string]map[string]bool, len(tp))
	for table, policies := range tp {
		for _, p := range policies {
			if !p.Denied {
				continue
			}
			if out[table] == nil {
				out[table] = map[string]bool{}
			}
			out[table][strings.ToLower(p.Column)] = true
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// Apply injects the security projection for every policed table into plan.
// columnsOf resolves a table's declared column list (the catalog); it may
// return nil, in which case the scan falls back to its own annotations and,
// failing that, is reported unprotected. The second return value is the number
// of scans left UNPROTECTED — never zero-and-ignored: a caller that cannot
// protect a scan must refuse the query.
func (tp TablePolicies) Apply(plan *Node, columnsOf func(table string) []string) (*Node, int) {
	if len(tp) == 0 || plan == nil {
		return plan, 0
	}
	unprotected := 0
	for _, table := range PolicedScanTables(plan) {
		policies := tp.For(table)
		if len(policies) == 0 {
			continue
		}
		var cols []string
		if columnsOf != nil {
			cols = columnsOf(table)
		}
		var n int
		plan, n = InjectColumnPolicies(plan, table, policies, cols)
		unprotected += n
	}
	return plan, unprotected
}
