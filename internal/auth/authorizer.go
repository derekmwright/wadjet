package auth

// Authorizer checks whether an identity has permission to perform operations.
type Authorizer struct {
	roles map[string]*RoleDef
}

// HasPermission checks whether the identity has the given permission.
// Valid permissions: "read", "write", "admin".
func (a *Authorizer) HasPermission(id *Identity, perm string) bool {
	if id == nil {
		return false
	}
	for _, p := range id.Perms {
		if p == perm || p == "admin" {
			return true
		}
	}
	return false
}

// CanAccessTable checks whether the identity's role grants access to a table.
func (a *Authorizer) CanAccessTable(id *Identity, tableName string) bool {
	if id == nil {
		return false
	}
	for _, t := range id.Tables {
		if t == "*" || t == tableName {
			return true
		}
	}
	return false
}

// ResolveRole fills id's Tables and Perms from the role definitions this
// Authorizer holds NOW, and reports whether the role was found.
//
// It is how an identity reconstructed from a stored snapshot gets its grants.
// A snapshot records who the definer WAS — name, role, method, attributes —
// and deliberately not what they could do: persisting `Perms` and `Tables`
// would freeze a grant at creation time, so an alert created by a role that
// has since been narrowed, or removed, would keep running under the old one.
// Re-resolving at run time means the CURRENT configuration decides, every
// tick, and a definer whose role is gone holds nothing at all — which is the
// right answer and the fail-closed one.
//
// An identity that already carries grants is left alone: only a role the
// configuration defines can add any.
func (a *Authorizer) ResolveRole(id *Identity) bool {
	if a == nil || id == nil || id.Role == "" {
		return false
	}
	role := a.roles[id.Role]
	if role == nil {
		return false
	}
	id.Tables, id.Perms = role.Tables, role.Perms
	return true
}

// FilterTables returns only tables the identity is allowed to see.
func (a *Authorizer) FilterTables(id *Identity, tables []string) []string {
	if id == nil {
		return nil
	}
	// Wildcard access — return all
	for _, t := range id.Tables {
		if t == "*" {
			return tables
		}
	}
	allowed := make(map[string]bool, len(id.Tables))
	for _, t := range id.Tables {
		allowed[t] = true
	}
	var filtered []string
	for _, t := range tables {
		if allowed[t] {
			filtered = append(filtered, t)
		}
	}
	return filtered
}
