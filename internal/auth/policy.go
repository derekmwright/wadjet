package auth

import (
	"strings"
)

// ColumnPolicy defines how a column is handled for a given role.
type ColumnPolicy int

const (
	// ColumnAllow permits full access to the column value.
	ColumnAllow ColumnPolicy = iota
	// ColumnMask replaces the value with a redacted placeholder.
	ColumnMask
	// ColumnDeny removes the column from results entirely.
	ColumnDeny
)

// CellMaskFunc replaces a cell value with a masked version.
// Receives the original value and returns the masked value.
type CellMaskFunc func(val any) any

// AccessPolicy defines cell-level access control for a table.
// Policies are evaluated per-role to filter rows and mask/deny columns.
type AccessPolicy struct {
	Table      string            // table name this policy applies to
	Role       string            // role name this policy applies to
	Columns    map[string]ColumnPolicy // column name -> policy
	MaskValues map[string]CellMaskFunc // column name -> custom mask function (optional)
	RowFilter  string            // SQL predicate appended to WHERE clause (e.g. "region = 'us'")
}

// PolicySet holds all access policies, indexed by table+role.
type PolicySet struct {
	policies map[string]*AccessPolicy // key: "table:role"
}

// NewPolicySet creates an empty policy set.
func NewPolicySet() *PolicySet {
	return &PolicySet{policies: make(map[string]*AccessPolicy)}
}

// Add registers an access policy.
func (ps *PolicySet) Add(p *AccessPolicy) {
	key := p.Table + ":" + p.Role
	ps.policies[key] = p
}

// Lookup returns the policy for a given table and role, or nil if none exists.
func (ps *PolicySet) Lookup(table, role string) *AccessPolicy {
	if ps == nil {
		return nil
	}
	// Try exact match
	if p, ok := ps.policies[table+":"+role]; ok {
		return p
	}
	// Try wildcard table
	if p, ok := ps.policies["*:"+role]; ok {
		return p
	}
	return nil
}

// ApplyToRow applies column policies to a single result row.
// Returns the filtered row with masked/denied columns handled.
func (p *AccessPolicy) ApplyToRow(row map[string]any) map[string]any {
	if p == nil || len(p.Columns) == 0 {
		return row
	}

	result := make(map[string]any, len(row))
	for col, val := range row {
		policy, hasPol := p.Columns[col]
		if !hasPol {
			// No explicit policy — allow by default
			result[col] = val
			continue
		}

		switch policy {
		case ColumnAllow:
			result[col] = val
		case ColumnMask:
			if fn, ok := p.MaskValues[col]; ok {
				result[col] = fn(val)
			} else {
				result[col] = defaultMask(val)
			}
		case ColumnDeny:
			// Omit from result
		}
	}
	return result
}

// ApplyToRows applies column policies to all result rows.
func (p *AccessPolicy) ApplyToRows(rows []map[string]any) []map[string]any {
	if p == nil || len(p.Columns) == 0 {
		return rows
	}
	result := make([]map[string]any, len(rows))
	for i, row := range rows {
		result[i] = p.ApplyToRow(row)
	}
	return result
}

// defaultMask returns a redacted placeholder based on the value type.
func defaultMask(val any) any {
	if val == nil {
		return nil
	}
	switch val.(type) {
	case string:
		return "***"
	case int, int32, int64:
		return int64(0)
	case float32, float64:
		return float64(0)
	case bool:
		return false
	default:
		return "***"
	}
}

// PolicyConfig is the YAML representation of cell-level access policies.
type PolicyConfig struct {
	Table   string              `yaml:"table"`
	Role    string              `yaml:"role"`
	Columns map[string]string   `yaml:"columns"` // column name -> "allow", "mask", "deny"
	RowFilter string            `yaml:"row_filter"` // SQL WHERE predicate
}

// ParsePolicies converts YAML policy configs into a PolicySet.
func ParsePolicies(configs []PolicyConfig) *PolicySet {
	ps := NewPolicySet()
	for _, cfg := range configs {
		p := &AccessPolicy{
			Table:   cfg.Table,
			Role:    cfg.Role,
			Columns: make(map[string]ColumnPolicy, len(cfg.Columns)),
		}
		for col, action := range cfg.Columns {
			switch strings.ToLower(action) {
			case "allow":
				p.Columns[col] = ColumnAllow
			case "mask":
				p.Columns[col] = ColumnMask
			case "deny":
				p.Columns[col] = ColumnDeny
			default:
				p.Columns[col] = ColumnAllow
			}
		}
		if cfg.RowFilter != "" {
			p.RowFilter = cfg.RowFilter
		}
		ps.Add(p)
	}
	return ps
}
