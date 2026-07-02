package auth

import (
	"context"
	"fmt"

	"github.com/citc-tech/wadjet/internal/planner/logical"
	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
)

// EnforcePlanPolicies applies ABAC to a query at plan level: table-access
// denial, row-filter injection, and column deny/mask injection for every
// table the SELECT references. It is THE shared enforcement path — the
// embedded engine (wadjet.DB.Query) and the coordinator's native-DAG
// executor (Coordinator.ExecuteSQL) both call it with the same inputs, so
// an identity sees identical policy behavior regardless of which execution
// path answers.
//
// No-ops (returns the plan unchanged) when the provider is nil/disabled,
// no identity is attached to ctx, or the provider has no evaluator —
// matching the embedded engine's historical behavior. protocol labels the
// evaluation environment for policy conditions and audit.
func EnforcePlanPolicies(ctx context.Context, provider *Provider, selectInfo *plansql.SelectInfo, plan *logical.Node, protocol string) (*logical.Node, error) {
	if provider == nil || !provider.Enabled() {
		return plan, nil
	}
	identity := IdentityFromContext(ctx)
	if identity == nil {
		return plan, nil
	}
	evaluator := provider.Evaluator()
	if evaluator == nil {
		return plan, nil
	}

	subject := identity.ToSubject()
	env := Environment{Protocol: protocol}

	allTables := make([]string, 0, len(selectInfo.Tables)+len(selectInfo.Joins))
	for _, t := range selectInfo.Tables {
		allTables = append(allTables, t.Name)
	}
	for _, j := range selectInfo.Joins {
		allTables = append(allTables, j.RightTable)
	}

	for _, tableName := range allTables {
		td := evaluator.EvaluateTableAccess(subject, tableName, ActionRead, env)
		if !td.Allowed {
			return nil, fmt.Errorf("access denied to table %q: %s", tableName, td.Reason)
		}
		if td.RowFilter != "" {
			plan = logical.InjectRowFilter(plan, tableName, td.RowFilter)
		}
		if len(td.Columns) > 0 {
			var colPolicies []logical.ColumnPolicy
			for _, col := range td.Columns {
				if !col.Allowed {
					colPolicies = append(colPolicies, logical.ColumnPolicy{
						Column: col.Column,
						Denied: true,
					})
				} else if col.MaskFunc != "" || col.MaskExpr != "" {
					mask := col.MaskExpr
					if mask == "" {
						mask = "'***'"
					}
					colPolicies = append(colPolicies, logical.ColumnPolicy{
						Column:   col.Column,
						MaskExpr: mask,
					})
				}
			}
			if len(colPolicies) > 0 {
				plan = logical.InjectColumnPolicies(plan, tableName, colPolicies)
			}
		}
	}
	return plan, nil
}
