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
