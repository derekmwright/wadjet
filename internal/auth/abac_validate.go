package auth

import (
	"fmt"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// ValidateABACPolicies refuses unenforceable obligations at load/reload (#802).
// mask_column requires value or mask_func, and value must parse as a SQL
// expression; never drop or redefine an invalid mask. Legacy type-derived
// placeholders migrate as mask_func:redact. Deny/mask must name a target.
// Validate the closed effect/action/operator/attribute-namespace/obligation
// vocabulary too (#932): unknown words must refuse, not grant or never match.
// See docs/internals/auth-policy-load-validation.md for the design.
func ValidateABACPolicies(policies []AccessControlPolicy) error {
	for _, pol := range policies {
		for _, rule := range pol.Rules {
			if err := validateRuleVocabulary(pol.Name, rule); err != nil {
				return err
			}
			// The columns this rule takes away. A mask expression is evaluated
			// BELOW the security barrier, against the row as stored, so one
			// that reads a policed column publishes exactly the value the same
			// rule was written to hide.
			restricted := map[string]bool{}
			for _, ob := range rule.Obligations {
				switch ob.Type {
				case "deny_column", "mask_column":
					restricted[strings.ToLower(strings.TrimSpace(ob.Target))] = true
				}
			}
			for _, ob := range rule.Obligations {
				if err := validateObligation(pol.Name, rule.ID, ob, restricted); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// The security vocabulary. Each of these is a CLOSED set, and it is closed
// because the evaluator implements exactly these words and nothing else — a
// value outside the set cannot be enforced as the operator meant it, so the
// policy set does not load.
var (
	// validEffects: what NewPolicyEvaluator resolves. Anything else — a
	// misspelling, an empty string — is now a DENY there rather than an
	// allow, and refused here so the operator hears about it at load.
	validEffects = map[string]bool{"allow": true, "deny": true}

	// validActions: the auth.Action constants. `ruleMatches` compares an
	// action by value, so an unknown one makes the rule match nothing.
	validActions = map[Action]bool{
		ActionRead: true, ActionWrite: true, ActionAdmin: true,
		ActionCreate: true, ActionDrop: true, ActionDescribe: true,
	}

	// validOperators: exactly the cases matchCondition implements. Its
	// default arm returns false, so an unknown operator is a rule that never
	// matches.
	validOperators = map[string]bool{
		"eq": true, "neq": true, "in": true, "not_in": true,
		"gt": true, "lt": true, "gte": true, "lte": true,
		"contains": true, "regex": true, "exists": true, "not_exists": true,
	}

	// validObligationTypes: what EvaluateTableAccess applies.
	validObligationTypes = map[string]bool{
		"deny_column": true, "mask_column": true, "row_filter": true, "query_limit": true,
	}

	// resourceAttributes / environmentAttributes: the names buildAttrMap
	// PUBLISHES. The `subject.` namespace is deliberately open — a JWT claim
	// or an mTLS certificate field arrives under whatever name the issuer
	// chose — but the other two are produced here, so a name outside them
	// reads nothing.
	//
	// `path`, `url` and `host` are the attributes a TABLE-FUNCTION resource
	// carries (its destination, derived from the arguments before anything is
	// opened), and `resource.arg_<name>` is any named reader option; those are
	// accepted by prefix, in validateCondition.
	//
	// `resource.table` is absent on purpose: `relationAttributes` in
	// abac_eval.go names it as an identifier-valued attribute, but nothing
	// publishes it, so a condition on it matches nothing.
	resourceAttributes = map[string]bool{
		"resource.type": true, "resource.name": true,
		"resource.path": true, "resource.url": true, "resource.host": true,
	}
	environmentAttributes = map[string]bool{
		"env.time": true, "env.hour": true, "env.source_ip": true, "env.protocol": true,
	}
)

// validateRuleVocabulary refuses a rule using a word the evaluator does not
// implement. Every one of these is fail-OPEN when it is merely ignored: an
// unresolvable effect became an allow, and an unmatched deny beside a broad
// allow is the grant the deny was written to prevent.
func validateRuleVocabulary(policy string, rule PolicyRule) error {
	where := fmt.Sprintf("policy %q rule %q", policy, rule.ID)
	if !validEffects[strings.ToLower(strings.TrimSpace(rule.EffectStr))] {
		return fmt.Errorf("%s: effect %q is not \"allow\" or \"deny\"",
			where, rule.EffectStr)
	}
	for _, a := range rule.Actions {
		if !validActions[a] {
			return fmt.Errorf("%s: action %q is not one of read, write, admin, create, "+
				"drop, describe", where, a)
		}
	}
	for _, group := range [][]Condition{rule.Subjects, rule.Resources, rule.Environment} {
		for _, c := range group {
			if err := validateCondition(where, c); err != nil {
				return err
			}
		}
	}
	for _, ob := range rule.Obligations {
		if !validObligationTypes[ob.Type] {
			return fmt.Errorf("%s: obligation type %q is not one of deny_column, "+
				"mask_column, row_filter, query_limit", where, ob.Type)
		}
	}
	return nil
}

// validateCondition refuses an operator the evaluator does not implement and
// an attribute it does not publish.
func validateCondition(where string, c Condition) error {
	if !validOperators[c.Op] {
		return fmt.Errorf("%s: condition operator %q on %q is not one of eq, neq, in, "+
			"not_in, gt, lt, gte, lte, contains, regex, exists, not_exists",
			where, c.Op, c.Attribute)
	}
	switch {
	case strings.HasPrefix(c.Attribute, "subject."):
		// Open by design: identity enrichment brings claims and certificate
		// fields the product does not name. An empty name is still nothing.
		if len(c.Attribute) == len("subject.") {
			return fmt.Errorf("%s: condition attribute %q names no subject attribute",
				where, c.Attribute)
		}
		return nil
	case strings.HasPrefix(c.Attribute, "resource."):
		if resourceAttributes[c.Attribute] {
			return nil
		}
		// A table function's named arguments are published as
		// `resource.arg_<name>`, and the set of them is the reader's option
		// list rather than anything this package can enumerate.
		if strings.HasPrefix(c.Attribute, "resource.arg_") &&
			len(c.Attribute) > len("resource.arg_") {
			return nil
		}
		return fmt.Errorf("%s: condition attribute %q is not one of resource.type, "+
			"resource.name, resource.path, resource.url, resource.host or "+
			"resource.arg_<name>", where, c.Attribute)
	case strings.HasPrefix(c.Attribute, "env."):
		if environmentAttributes[c.Attribute] {
			return nil
		}
		return fmt.Errorf("%s: condition attribute %q is not one of env.time, env.hour, "+
			"env.source_ip, env.protocol", where, c.Attribute)
	default:
		return fmt.Errorf("%s: condition attribute %q has no namespace; write "+
			"subject.<name>, resource.<name> or env.<name>", where, c.Attribute)
	}
}

func validateObligation(policy, rule string, ob Obligation, restricted map[string]bool) error {
	where := fmt.Sprintf("policy %q rule %q", policy, rule)
	switch ob.Type {
	case "deny_column", "mask_column":
		if strings.TrimSpace(ob.Target) == "" {
			return fmt.Errorf("%s: %s obligation names no target column", where, ob.Type)
		}
	case "row_filter":
		// A row filter that is not a SQL predicate is injected and then
		// silently does nothing: `InjectRowFilter` returns the plan unchanged
		// when ParseExpression fails, so the policy that was supposed to
		// restrict the rows restricted none of them. Same doctrine as the
		// masks — a control that cannot be applied refuses to load.
		if strings.TrimSpace(ob.Value) == "" {
			return fmt.Errorf("%s: row_filter obligation carries no predicate", where)
		}
		if _, err := plansql.ParseExpression(ob.Value); err != nil {
			return fmt.Errorf("%s: row_filter is not a SQL predicate: %q: %w",
				where, ob.Value, err)
		}
	case "query_limit":
		// The obligation was accepted and then dropped by the evaluator for
		// as long as it has existed: docs/security.md said "Not enforced" in
		// the table of obligation types, and an operator who wrote one
		// believed a ceiling was in place that was not. It is enforced now
		// (through the same cost guard `query_limits:` uses), so the two
		// spellings it can get wrong have to refuse rather than be dropped.
		if _, _, err := QueryLimitObligation(ob); err != nil {
			return fmt.Errorf("%s: %w", where, err)
		}
	}
	if ob.Type != "mask_column" {
		return nil
	}
	if ob.Value == "" {
		if ob.MaskFunc != "" {
			return nil // the legacy form: a type-derived placeholder
		}
		return fmt.Errorf("%s: mask_column on %q gives neither a value nor a mask_func; "+
			"write the replacement as a SQL expression, e.g. value: \"'REDACTED'\"",
			where, ob.Target)
	}
	ast, err := plansql.ParseExpression(ob.Value)
	if err != nil {
		return fmt.Errorf("%s: mask_column on %q has a value that is not a SQL expression: %q "+
			"(a literal string needs its quotes, e.g. value: \"'REDACTED'\"): %w",
			where, ob.Target, ob.Value, err)
	}
	if lost, ok := maskExpressionLosesText(ob.Value, ast); !ok {
		return fmt.Errorf("%s: mask_column on %q has a value the parser does not read as written: "+
			"%q loses %q (a literal string needs its quotes, e.g. value: \"'REDACTED'\")",
			where, ob.Target, ob.Value, lost)
	}
	// The expression runs against the row AS STORED. One that reads a column
	// this same rule masks or denies publishes the value the rule takes away —
	// `value: "ssn"` on a masked `ssn` is a grant written as a mask.
	if col, bad := maskReadsRestricted(ast, restricted); bad {
		return fmt.Errorf("%s: mask_column on %q has a value that reads %q, "+
			"which this rule also restricts; a mask expression is evaluated against the "+
			"row as STORED, so it would publish the value the policy takes away",
			where, ob.Target, col)
	}
	return nil
}

// maskExpressionLosesText reports whether the parser read the value AS
// WRITTEN, by checking that every word in the input survives into the
// expression's own rendering.
//
// `value: "***REDACTED***"` — the spelling docs/configuration.md shipped for
// twelve releases — PARSES: it becomes the expression `* * *`, the word
// REDACTED is gone, and every masked column, string numeric and timestamp
// alike, came back as 0. A parse error is not the signal, because there is
// none; the signal is that the operator's text did not survive.
//
// The check is deliberately one-directional and word-level, so a renderer that
// adds parentheses or normalises spacing never refuses a valid mask.
func maskExpressionLosesText(value string, ast plansql.Node) (string, bool) {
	if ast == nil {
		return value, false
	}
	rendered := strings.ToLower(ast.String())
	word := strings.Builder{}
	flush := func() (string, bool) {
		w := word.String()
		word.Reset()
		if w == "" {
			return "", true
		}
		return w, strings.Contains(rendered, strings.ToLower(w))
	}
	for _, r := range value {
		if r == '_' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			word.WriteRune(r)
			continue
		}
		if w, ok := flush(); !ok {
			return w, false
		}
	}
	if w, ok := flush(); !ok {
		return w, false
	}
	return "", true
}

// maskReadsRestricted reports whether a parsed mask expression reads a column
// the same rule masks or denies. An expression the ref walker cannot see
// through is not a refusal: the walker errors on nodes whose columns are not
// visible from it, and refusing those would refuse valid masks.
func maskReadsRestricted(ast plansql.Node, restricted map[string]bool) (string, bool) {
	if len(restricted) == 0 {
		return "", false
	}
	refs, err := plansql.ColumnRefs(ast)
	if err != nil {
		return "", false
	}
	for _, ref := range refs {
		if restricted[strings.ToLower(ref.Column)] {
			return ref.Column, true
		}
	}
	return "", false
}
