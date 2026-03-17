package auth

import "fmt"

// MigrateRBACToABAC converts legacy RBAC role and policy configs into equivalent
// ABAC policies. This runs at startup when ABACPolicies is empty but Roles/Policies
// are populated, providing full backward compatibility.
func MigrateRBACToABAC(roles []RoleConfig, cellPolicies []PolicyConfig) []AccessControlPolicy {
	var policies []AccessControlPolicy

	// Convert each role into an ABAC policy
	for _, role := range roles {
		var rules []PolicyRule

		// Table access rule
		var resourceConds []Condition
		hasWildcard := false
		for _, t := range role.Tables {
			if t == "*" {
				hasWildcard = true
				break
			}
		}
		if !hasWildcard && len(role.Tables) > 0 {
			tables := make([]any, len(role.Tables))
			for i, t := range role.Tables {
				tables[i] = t
			}
			resourceConds = append(resourceConds, Condition{
				Attribute: "resource.name",
				Op:        "in",
				Value:     tables,
			})
		}

		// Map permissions to actions
		var actions []Action
		for _, perm := range role.Allow {
			switch perm {
			case "read":
				actions = append(actions, ActionRead, ActionDescribe)
			case "write":
				actions = append(actions, ActionWrite, ActionCreate, ActionDrop)
			case "admin":
				actions = append(actions, ActionRead, ActionWrite, ActionAdmin, ActionCreate, ActionDrop, ActionDescribe)
			}
		}

		rules = append(rules, PolicyRule{
			ID:        fmt.Sprintf("migrated-role-%s", role.Name),
			EffectStr: "allow",
			Priority:  100,
			Subjects: []Condition{{
				Attribute: "subject.role",
				Op:        "eq",
				Value:     role.Name,
			}},
			Resources: resourceConds,
			Actions:   actions,
		})

		policies = append(policies, AccessControlPolicy{
			Name:    fmt.Sprintf("migrated-role-%s", role.Name),
			Version: 1,
			Rules:   rules,
			Enabled: true,
		})
	}

	// Convert cell-level policies into ABAC obligation rules
	for _, cp := range cellPolicies {
		var obligations []Obligation

		for col, action := range cp.Columns {
			switch action {
			case "deny":
				obligations = append(obligations, Obligation{
					Type:   "deny_column",
					Target: col,
				})
			case "mask":
				obligations = append(obligations, Obligation{
					Type:     "mask_column",
					Target:   col,
					MaskFunc: "redact",
				})
			}
			// "allow" needs no obligation
		}

		if cp.RowFilter != "" {
			obligations = append(obligations, Obligation{
				Type:  "row_filter",
				Value: cp.RowFilter,
			})
		}

		if len(obligations) == 0 {
			continue
		}

		var resourceConds []Condition
		if cp.Table != "*" {
			resourceConds = append(resourceConds, Condition{
				Attribute: "resource.name",
				Op:        "eq",
				Value:     cp.Table,
			})
		}

		rule := PolicyRule{
			ID:        fmt.Sprintf("migrated-policy-%s-%s", cp.Table, cp.Role),
			EffectStr: "allow",
			Priority:  50, // Higher priority than role rules to ensure obligations apply
			Subjects: []Condition{{
				Attribute: "subject.role",
				Op:        "eq",
				Value:     cp.Role,
			}},
			Resources:   resourceConds,
			Actions:     []Action{ActionRead},
			Obligations: obligations,
		}

		policies = append(policies, AccessControlPolicy{
			Name:    fmt.Sprintf("migrated-policy-%s-%s", cp.Table, cp.Role),
			Version: 1,
			Rules:   []PolicyRule{rule},
			Enabled: true,
		})
	}

	return policies
}
