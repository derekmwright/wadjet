package auth

import "time"

// Attributes is a string-keyed bag of values carried by subjects, resources, and environments.
type Attributes map[string]any

// Get returns the value for a key, or nil if not present.
func (a Attributes) Get(key string) any {
	if a == nil {
		return nil
	}
	return a[key]
}

// GetString returns the string value for a key, or "" if not present or not a string.
func (a Attributes) GetString(key string) string {
	v, _ := a[key].(string)
	return v
}

// Subject represents the authenticated caller with attributes for policy evaluation.
type Subject struct {
	Identity   *Identity
	Attributes Attributes // merged from JWT claims, mTLS cert fields, config, API key def
}

// Resource describes what is being accessed.
type Resource struct {
	Type string     // "table", "column", "function"
	Name string     // e.g. "events", "users.ssn"
	Attributes Attributes // e.g. {"classification": "SECRET", "owner": "alice"}
}

// Action is the operation being performed.
type Action string

const (
	ActionRead    Action = "read"
	ActionWrite   Action = "write"
	ActionAdmin   Action = "admin"
	ActionCreate  Action = "create"
	ActionDrop    Action = "drop"
	ActionDescribe Action = "describe"
)

// Environment carries contextual attributes for policy evaluation.
type Environment struct {
	Time     time.Time
	SourceIP string
	Protocol string // "http", "pgwire", "grpc"
	Custom   Attributes
}

// Effect is the outcome of a policy rule.
type Effect int

const (
	EffectAllow Effect = iota
	EffectDeny
)

// String returns "allow" or "deny".
func (e Effect) String() string {
	if e == EffectDeny {
		return "deny"
	}
	return "allow"
}

// Condition is a single predicate in a policy rule.
// Attribute uses dot-path notation: "subject.role", "resource.name", "env.source_ip".
type Condition struct {
	Attribute string `yaml:"attribute" json:"attribute"`
	Op        string `yaml:"op" json:"op"`    // eq, neq, in, not_in, gt, lt, gte, lte, contains, exists
	Value     any    `yaml:"value" json:"value"`
}

// Obligation is a side-effect applied when a rule matches.
type Obligation struct {
	Type     string `yaml:"type" json:"type"`         // mask_column, deny_column, row_filter, query_limit
	Target   string `yaml:"target" json:"target"`     // column name, table name
	Value    string `yaml:"value" json:"value"`       // SQL predicate, mask expression
	MaskFunc string `yaml:"mask_func" json:"mask_func"` // redact, hash, nullify, partial
}

// PolicyRule is one rule within an access control policy.
type PolicyRule struct {
	ID          string      `yaml:"id" json:"id"`
	Description string      `yaml:"description" json:"description"`
	Effect      Effect      `yaml:"-" json:"-"`
	EffectStr   string      `yaml:"effect" json:"effect"` // "allow" or "deny" for serialization
	Priority    int         `yaml:"priority" json:"priority"` // lower = higher priority
	Subjects    []Condition `yaml:"subjects" json:"subjects"`
	Resources   []Condition `yaml:"resources" json:"resources"`
	Actions     []Action    `yaml:"actions" json:"actions"`
	Environment []Condition `yaml:"environment" json:"environment"`
	Obligations []Obligation `yaml:"obligations" json:"obligations"`
}

// AccessControlPolicy groups related rules.
type AccessControlPolicy struct {
	Name    string       `yaml:"name" json:"name"`
	Version int          `yaml:"version" json:"version"`
	Rules   []PolicyRule `yaml:"rules" json:"rules"`
	Enabled bool         `yaml:"enabled" json:"enabled"`
}

// ColumnDecision describes the access decision for a single column.
type ColumnDecision struct {
	Column   string `json:"column"`
	Allowed  bool   `json:"allowed"`
	MaskFunc string `json:"mask_func,omitempty"`
	MaskExpr string `json:"mask_expr,omitempty"`
}

// TableDecision is the fully-resolved access decision for a table.
type TableDecision struct {
	Allowed   bool             `json:"allowed"`
	Columns   []ColumnDecision `json:"columns,omitempty"`
	RowFilter string           `json:"row_filter,omitempty"`
	Reason    string           `json:"reason,omitempty"`
	RuleID    string           `json:"rule_id,omitempty"`
}

// Decision is the outcome of a single policy evaluation.
type Decision struct {
	Allowed     bool
	Effect      Effect
	MatchedRule string
	Obligations []Obligation
	Reason      string
}

// SerializedDecision is the pre-evaluated policy decision that travels
// with distributed tasks. Workers enforce these without access to policies.
type SerializedDecision struct {
	Allowed        bool                       `json:"allowed"`
	TableDecisions map[string]*TableDecision  `json:"table_decisions,omitempty"`
	MaxScanBytes   int64                      `json:"max_scan_bytes,omitempty"`
	MaxScanRows    int64                      `json:"max_scan_rows,omitempty"`
}

// ToSubject creates a Subject from an Identity for ABAC evaluation.
func (id *Identity) ToSubject() Subject {
	attrs := make(Attributes, len(id.Attributes)+3)
	for k, v := range id.Attributes {
		attrs[k] = v
	}
	// Always populate role, name, method as attributes
	if id.Role != "" {
		attrs["role"] = id.Role
	}
	if id.Name != "" {
		attrs["name"] = id.Name
	}
	if id.Method != "" {
		attrs["method"] = id.Method
	}
	return Subject{
		Identity:   id,
		Attributes: attrs,
	}
}
