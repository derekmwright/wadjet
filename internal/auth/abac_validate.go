package auth

import (
	"fmt"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// ValidateABACPolicies refuses a policy set whose obligations cannot be
// enforced as written. It runs where ParsePolicies' own refusal runs — at
// config load and at hot reload — and for the same reason (#802): a
// column-access control that cannot be understood must refuse to load, because
// the alternative is an operator who believes a column is masked and is served
// it in the clear.
//
// The two refusals:
//
//   - A `mask_column` obligation carrying NEITHER `value` nor `mask_func`.
//     The enforcement path dropped such an obligation, so the column came back
//     in the clear on every door. The type-derived placeholder ('***', 0,
//     false) belongs to the LEGACY `policies: columns: {col: mask}` form, which
//     MigrateRBACToABAC spells as `mask_func: redact`; an `abac_policies:`
//     obligation says what it means with `value:`.
//   - A `mask_column` whose `value` is not a SQL EXPRESSION. `value:
//     "***REDACTED***"` — the spelling docs/configuration.md shipped for
//     twelve releases — does not parse, and the fallback turned every masked
//     column, string numeric and timestamp alike, into `0`. A mask that
//     silently redefines itself is the same class of defect as one that
//     silently disappears.
//
// A `deny_column` or `mask_column` with no target names no column and is
// refused for the same reason.
func ValidateABACPolicies(policies []AccessControlPolicy) error {
	for _, pol := range policies {
		for _, rule := range pol.Rules {
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

func validateObligation(policy, rule string, ob Obligation, restricted map[string]bool) error {
	where := fmt.Sprintf("policy %q rule %q", policy, rule)
	switch ob.Type {
	case "deny_column", "mask_column":
		if strings.TrimSpace(ob.Target) == "" {
			return fmt.Errorf("%s: %s obligation names no target column", where, ob.Type)
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
