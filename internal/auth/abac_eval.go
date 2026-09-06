package auth

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// PolicyEvaluator evaluates access control policies using deny-overrides.
type PolicyEvaluator struct {
	policies []AccessControlPolicy
}

// NewPolicyEvaluator creates an evaluator from a set of policies.
//
// An effect that resolves to neither "allow" nor "deny" becomes a DENY. It
// used to become an ALLOW — every string but a case-insensitive "deny" did,
// the empty string included — so `effect: dney` granted exactly the action the
// operator had written a deny for (#932). `ValidateABACPolicies` refuses such
// a rule at load and is the message an operator sees; this is the floor under
// a programmatic caller who never ran the validator, and a floor under a
// security decision falls the safe way.
func NewPolicyEvaluator(policies []AccessControlPolicy) *PolicyEvaluator {
	// Resolve EffectStr → Effect on all rules
	for i := range policies {
		for j := range policies[i].Rules {
			r := &policies[i].Rules[j]
			if strings.EqualFold(strings.TrimSpace(r.EffectStr), "allow") {
				r.Effect = EffectAllow
			} else {
				r.Effect = EffectDeny
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
		default:
			// An obligation this cannot apply is a REFUSAL, not a silence.
			// There was no default arm: `type: deny_colum` produced a
			// matching allow with no column restriction at all, so the column
			// the rule was written to hide came back in plaintext on every
			// door (#932). ValidateABACPolicies refuses the type at load;
			// this is the floor under a caller who skipped it, and it closes
			// rather than opens.
			return &TableDecision{
				Allowed: false,
				RuleID:  decision.MatchedRule,
				Reason: fmt.Sprintf("policy obligation type %q on rule %q cannot be "+
					"enforced (not one of deny_column, mask_column, row_filter, query_limit)",
					ob.Type, decision.MatchedRule),
			}
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

// relationAttributes are the attributes whose value is a RELATION NAME rather
// than an opaque string, and so are compared under the engine's IDENTIFIER
// rule instead of byte-exactly.
//
// A relation name has two legitimate spellings for one relation: the catalog's
// (`Hits`, the spelling a parquet dataset or an Iceberg import brings) and the
// one an unquoted reference arrives in, which the lexer folded (#731). The
// query planner reconciles them with `catalog.ResolveTableName`, so a
// statement naming `Hits` and a statement naming `hits` read the SAME table —
// and a policy scoped to that relation has to bind to both, or it binds to
// neither door consistently.
//
// It did not. `resource.name` went through the generic attribute comparator
// (`compareEq`, `fmt.Sprintf("%v")`), which is byte-exact, so with a catalog
// table `Hits` and a rule scoped `resource.name eq "hits"` the rule did not
// match — and an unmatched scoped rule is not a REFUSAL. The broad allow that
// every `roles:`-to-ABAC migration emits still matched, so `Evaluate` returned
// Allowed with NO obligations: the masked column came back in plaintext, the
// denied column came back at all, and a DML predicate on the masked column
// became a working oracle for its own stored value (#882, ADR-0033 rule 2).
// The legacy `PolicySet` had the same hole in the opposite direction, so on a
// CamelCase table there was no single spelling an operator could write that
// bound on both paths.
//
// The fix is not to fold `compareEq` — `eq` must stay byte-exact for ordinary
// attributes, or a rule reading `classification eq "SECRET"` would start
// matching `"secret"` and quietly widen every clearance in the file. Only the
// attributes that carry an IDENTIFIER get the identifier rule.
var relationAttributes = map[string]bool{
	"resource.name":  true,
	"resource.table": true,
}

// matchCondition evaluates a single condition against the attribute map.
func matchCondition(cond Condition, attrs map[string]any) bool {
	val, exists := attrs[cond.Attribute]
	if relationAttributes[cond.Attribute] {
		switch cond.Op {
		case "eq":
			return exists && relationEq(val, cond.Value)
		case "neq":
			return !exists || !relationEq(val, cond.Value)
		case "in":
			return exists && relationIn(val, cond.Value)
		case "not_in":
			return !exists || !relationIn(val, cond.Value)
		}
	}

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

// relationEq compares two RELATION names the way the engine resolves one:
// byte-exact, or — when at least one side is already FOLDED, i.e. carries no
// ASCII upper-case letter and so is the spelling an unquoted reference
// arrives in — equal ignoring ASCII case.
//
// The "at least one side is folded" test is what keeps this from being a
// blanket case-insensitive compare. Two spellings that BOTH carry upper case
// (`Hits` vs `HITS`) are two delimited names, and a delimited name is
// byte-exact everywhere else in the engine (ADR-0012 item 4,
// `catalog.ResolveTableName`), so they stay distinct here too.
//
// A policy is fail-closed under this rule: it can only ever bind to MORE
// spellings of the relation it names, never to a different relation — and
// `catalog.CreateTable` already refuses two tables whose names are case-twins,
// so "more spellings" cannot reach a second table.
func relationEq(a, b any) bool {
	as, bs := fmt.Sprintf("%v", a), fmt.Sprintf("%v", b)
	if as == bs {
		return true
	}
	if !batch.IsFoldedIdent(as) && !batch.IsFoldedIdent(bs) {
		return false
	}
	return batch.EqualFoldIdent(as, bs)
}

// relationIn is relationEq over a set — what `MigrateRBACToABAC` emits for a
// role's `tables:` list.
func relationIn(val, set any) bool {
	switch s := set.(type) {
	case []any:
		for _, item := range s {
			if relationEq(val, item) {
				return true
			}
		}
	case []string:
		for _, item := range s {
			if relationEq(val, item) {
				return true
			}
		}
	}
	return false
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
