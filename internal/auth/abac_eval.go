package auth

import (
	"fmt"
	"regexp"
	"strings"
)

// PolicyEvaluator evaluates access control policies using deny-overrides.
type PolicyEvaluator struct {
	policies []AccessControlPolicy
}

// NewPolicyEvaluator creates an evaluator from a set of policies.
func NewPolicyEvaluator(policies []AccessControlPolicy) *PolicyEvaluator {
	// Resolve EffectStr → Effect on all rules
	for i := range policies {
		for j := range policies[i].Rules {
			r := &policies[i].Rules[j]
			if strings.EqualFold(r.EffectStr, "deny") {
				r.Effect = EffectDeny
			} else {
				r.Effect = EffectAllow
			}
		}
	}
	return &PolicyEvaluator{policies: policies}
}

// Evaluate runs deny-overrides policy evaluation.
//
// Algorithm:
//  1. Collect all rules whose conditions match the request context.
//  2. If ANY matching rule has Effect=Deny, result is Deny (highest-priority deny cited).
//  3. If no deny matches and at least one Allow matches, result is Allow.
//     Obligations are merged from all matching Allow rules.
//  4. If no rules match at all, default is Deny (closed-world assumption).
func (pe *PolicyEvaluator) Evaluate(subject Subject, resource Resource, action Action, env Environment) Decision {
	if pe == nil {
		return Decision{Allowed: true, Reason: "no policy evaluator"}
	}

	attrs := pe.buildAttrMap(subject, resource, action, env)

	var allowRules []PolicyRule
	var denyRules []PolicyRule

	for _, policy := range pe.policies {
		if !policy.Enabled {
			continue
		}
		for _, rule := range policy.Rules {
			if !pe.ruleMatches(rule, attrs, action) {
				continue
			}
			if rule.Effect == EffectDeny {
				denyRules = append(denyRules, rule)
			} else {
				allowRules = append(allowRules, rule)
			}
		}
	}

	// Deny overrides
	if len(denyRules) > 0 {
		best := denyRules[0]
		for _, r := range denyRules[1:] {
			if r.Priority < best.Priority {
				best = r
			}
		}
		return Decision{
			Allowed:     false,
			Effect:      EffectDeny,
			MatchedRule: best.ID,
			Reason:      fmt.Sprintf("denied by rule %q: %s", best.ID, best.Description),
		}
	}

	// Allow
	if len(allowRules) > 0 {
		var obligations []Obligation
		bestID := allowRules[0].ID
		bestPriority := allowRules[0].Priority
		for _, r := range allowRules {
			obligations = append(obligations, r.Obligations...)
			if r.Priority < bestPriority {
				bestPriority = r.Priority
				bestID = r.ID
			}
		}
		return Decision{
			Allowed:     true,
			Effect:      EffectAllow,
			MatchedRule: bestID,
			Obligations: obligations,
			Reason:      fmt.Sprintf("allowed by rule %q", bestID),
		}
	}

	// Default deny
	return Decision{
		Allowed: false,
		Effect:  EffectDeny,
		Reason:  "no matching policy (default deny)",
	}
}

// EvaluateTableAccess evaluates table-level access and collects column/row obligations.
func (pe *PolicyEvaluator) EvaluateTableAccess(subject Subject, tableName string, action Action, env Environment) *TableDecision {
	resource := Resource{Type: "table", Name: tableName}
	decision := pe.Evaluate(subject, resource, action, env)

	td := &TableDecision{
		Allowed: decision.Allowed,
		Reason:  decision.Reason,
		RuleID:  decision.MatchedRule,
	}

	if !decision.Allowed {
		return td
	}

	// Process obligations from matching allow rules
	for _, ob := range decision.Obligations {
		switch ob.Type {
		case "row_filter":
			if td.RowFilter == "" {
				td.RowFilter = ob.Value
			} else {
				// AND multiple row filters
				td.RowFilter = "(" + td.RowFilter + ") AND (" + ob.Value + ")"
			}
		case "deny_column":
			td.Columns = append(td.Columns, ColumnDecision{
				Column:  ob.Target,
				Allowed: false,
			})
		case "mask_column":
			td.Columns = append(td.Columns, ColumnDecision{
				Column:   ob.Target,
				Allowed:  true,
				MaskFunc: ob.MaskFunc,
				MaskExpr: ob.Value,
			})
		case "query_limit":
			// The cost ceiling this identity is held to on this relation.
			// EnforcePlanPolicies narrows the statement's guard with it; the
			// obligation used to be dropped here, so docs/security.md said
			// "Not enforced" and a policy that named a ceiling had none.
			td.QueryLimits = applyQueryLimit(td.QueryLimits, ob)
		}
	}

	return td
}

// buildAttrMap flattens subject/resource/env attributes into a single lookup
// map with prefixed keys for condition evaluation.
func (pe *PolicyEvaluator) buildAttrMap(subject Subject, resource Resource, action Action, env Environment) map[string]any {
	attrs := make(map[string]any, len(subject.Attributes)+len(resource.Attributes)+len(env.Custom)+8)

	// Subject attributes (prefixed with "subject.")
	for k, v := range subject.Attributes {
		attrs["subject."+k] = v
	}

	// Resource attributes (prefixed with "resource.")
	attrs["resource.type"] = resource.Type
	attrs["resource.name"] = resource.Name
	for k, v := range resource.Attributes {
		attrs["resource."+k] = v
	}

	// Action
	attrs["action"] = string(action)

	// Environment attributes (prefixed with "env.")
	if !env.Time.IsZero() {
		attrs["env.time"] = env.Time.Format("15:04:05")
		attrs["env.hour"] = env.Time.Hour()
	}
	if env.SourceIP != "" {
		attrs["env.source_ip"] = env.SourceIP
	}
	if env.Protocol != "" {
		attrs["env.protocol"] = env.Protocol
	}
	for k, v := range env.Custom {
		attrs["env."+k] = v
	}

	return attrs
}

// ruleMatches returns true if all conditions in the rule match the attribute map.
func (pe *PolicyEvaluator) ruleMatches(rule PolicyRule, attrs map[string]any, action Action) bool {
	// Check action filter (OR semantics — any action matches)
	if len(rule.Actions) > 0 {
		matched := false
		for _, a := range rule.Actions {
			if a == action {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Subject conditions (AND semantics)
	for _, cond := range rule.Subjects {
		if !matchCondition(cond, attrs) {
			return false
		}
	}

	// Resource conditions (AND semantics)
	for _, cond := range rule.Resources {
		if !matchCondition(cond, attrs) {
			return false
		}
	}

	// Environment conditions (AND semantics)
	for _, cond := range rule.Environment {
		if !matchCondition(cond, attrs) {
			return false
		}
	}

	return true
}

// matchCondition evaluates a single condition against the attribute map.
func matchCondition(cond Condition, attrs map[string]any) bool {
	val, exists := attrs[cond.Attribute]

	switch cond.Op {
	case "exists":
		return exists
	case "not_exists":
		return !exists
	case "eq":
		return exists && compareEq(val, cond.Value)
	case "neq":
		return !exists || !compareEq(val, cond.Value)
	case "in":
		return exists && compareIn(val, cond.Value)
	case "not_in":
		return !exists || !compareIn(val, cond.Value)
	case "gt":
		return exists && compareOrd(val, cond.Value) > 0
	case "lt":
		return exists && compareOrd(val, cond.Value) < 0
	case "gte":
		return exists && compareOrd(val, cond.Value) >= 0
	case "lte":
		return exists && compareOrd(val, cond.Value) <= 0
	case "contains":
		return exists && containsStr(val, cond.Value)
	case "regex":
		return exists && matchRegex(val, cond.Value)
	default:
		return false
	}
}

func compareEq(a, b any) bool {
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

func compareIn(val, set any) bool {
	valStr := fmt.Sprintf("%v", val)
	switch s := set.(type) {
	case []any:
		for _, item := range s {
			if fmt.Sprintf("%v", item) == valStr {
				return true
			}
		}
	case []string:
		for _, item := range s {
			if item == valStr {
				return true
			}
		}
	}
	return false
}

func compareOrd(a, b any) int {
	af := toFloat(a)
	bf := toFloat(b)
	if af < bf {
		return -1
	}
	if af > bf {
		return 1
	}
	// Fall back to string comparison for non-numeric
	as := fmt.Sprintf("%v", a)
	bs := fmt.Sprintf("%v", b)
	if as < bs {
		return -1
	}
	if as > bs {
		return 1
	}
	return 0
}

func toFloat(v any) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case float32:
		return float64(n)
	case float64:
		return n
	}
	return 0
}

func containsStr(val, substr any) bool {
	return strings.Contains(fmt.Sprintf("%v", val), fmt.Sprintf("%v", substr))
}

func matchRegex(val, pattern any) bool {
	re, err := regexp.Compile(fmt.Sprintf("%v", pattern))
	if err != nil {
		return false
	}
	return re.MatchString(fmt.Sprintf("%v", val))
}
