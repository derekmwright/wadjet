package auth

import (
	"fmt"
	"sort"
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
	Table      string                  // table name this policy applies to
	Role       string                  // role name this policy applies to
	Columns    map[string]ColumnPolicy // column name -> policy
	MaskValues map[string]CellMaskFunc // column name -> custom mask function (optional)
	RowFilter  string                  // SQL predicate appended to WHERE clause (e.g. "region = 'us'")
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

// There is deliberately no ApplyToRow / ApplyToRows here any more.
//
// A cell policy is enforced at the SCAN, by the security projection
// auth.EnforcePlanPolicies injects (#859). A second, result-row pass could
// only ever rewrite an output column that still carried the policed column's
// NAME: it cannot reach `SUM(acct)`, `COUNT(DISTINCT ssn)`, a GROUP BY key, a
// join key or a WHERE predicate, because the pipeline consumed those values
// long before any row reached it — and it existed on ONE door (HTTP) while
// pgwire and the embedded API had no such pass at all. A door-specific
// enforcement is a second, weaker enforcement path; the plan-time one is the
// only one.
//
// A YAML `policies:` block still works exactly as before: MigrateRBACToABAC
// turns it into the deny_column / mask_column obligations the plan-time path
// enforces.

// PolicyConfig is the YAML representation of cell-level access policies.
type PolicyConfig struct {
	Table     string            `yaml:"table"`
	Role      string            `yaml:"role"`
	Columns   map[string]string `yaml:"columns"`    // column name -> "allow", "mask", "deny"
	RowFilter string            `yaml:"row_filter"` // SQL WHERE predicate
}

// ColumnActions lists the actions a policy's `columns:` map accepts, in the
// order an error message should offer them.
var ColumnActions = []string{"allow", "mask", "deny"}

// ParseColumnAction maps one YAML column action to its ColumnPolicy.
//
// An unrecognised action is an ERROR, not a default. Before #802 the default
// arm returned ColumnAllow, so `columns: {src_ip: "***REDACTED***"}` — the
// spelling every version of docs/security.md recommended — parsed to a full
// grant, and an operator who believed a PII column was masked was served it
// in the clear. A column-access control that cannot be understood must refuse
// to load: loud beats plausible, and a security control never degrades to a
// grant.
func ParseColumnAction(column, action string) (ColumnPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(action)) {
	case "allow":
		return ColumnAllow, nil
	case "mask":
		return ColumnMask, nil
	case "deny":
		return ColumnDeny, nil
	default:
		return ColumnDeny, fmt.Errorf("column %q: unknown action %q (want one of %s)",
			column, action, strings.Join(ColumnActions, ", "))
	}
}

// ParsePolicies converts YAML policy configs into a PolicySet. It fails on the
// first unrecognised column action, naming the table, role, column and the
// value it could not read (#802).
func ParsePolicies(configs []PolicyConfig) (*PolicySet, error) {
	ps := NewPolicySet()
	for _, cfg := range configs {
		p := &AccessPolicy{
			Table:   cfg.Table,
			Role:    cfg.Role,
			Columns: make(map[string]ColumnPolicy, len(cfg.Columns)),
		}
		// Sorted so a config with two bad columns always names the same one.
		for _, col := range sortedColumnNames(cfg.Columns) {
			policy, err := ParseColumnAction(col, cfg.Columns[col])
			if err != nil {
				return nil, fmt.Errorf("policy for table %q role %q: %w", cfg.Table, cfg.Role, err)
			}
			p.Columns[col] = policy
		}
		if cfg.RowFilter != "" {
			p.RowFilter = cfg.RowFilter
		}
		ps.Add(p)
	}
	return ps, nil
}

func sortedColumnNames(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
