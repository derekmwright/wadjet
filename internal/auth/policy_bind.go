package auth

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// Binding a policy's NAMES to the catalog, once, at load.
//
// A policy names relations and columns, and so does a query — but they arrive
// spelled differently. A query's unquoted identifier is folded to lower case
// by the lexer (#731); a catalog keeps the spelling the parquet file or the
// Iceberg import gave it, where CamelCase is ordinary. The engine reconciles
// the two with ONE rule (`catalog.ResolveTableName` for a relation,
// `batch.ResolveSchemaIndex` for a column): byte-exact first, then a unique
// ASCII-case-insensitive match for a name that is itself folded, with a
// delimited name staying byte-exact.
//
// Before #882 the policy layer did not use that rule at all. It compared the
// operator's YAML bytes against whatever spelling the statement happened to
// carry, and the two enforcement paths disagreed about which spelling that
// was — so on a CamelCase relation there was no single `table:` an operator
// could write that bound on both. A policy that failed to bind did not refuse:
// beside the broad allow a `roles:` migration emits, it granted.
//
// Two things follow, and this file is the second:
//
//  1. the COMPARISON is fold-aware wherever a relation name is matched
//     (`relationEq`, `policyKey`) — the floor, so no spelling mismatch can
//     ever yield "no policy applies";
//  2. the BINDING happens ONCE, here, against the catalog, and a policy naming
//     a relation or column that does not resolve is REFUSED — the policy is
//     rewritten to the catalog's own spelling, so evaluation compares two
//     names that came from the same place.
//
// (2) is what makes a typo loud. Without it, `resource.name eq "hitz"` is
// indistinguishable from a relation that does not exist yet, and the rule
// carrying the obligations silently never matches — which is the same
// disclosure #882 was, reached by a different road. ADR-0033's rule stands: a
// policy that cannot be enforced does not load.
//
// The consequence an operator must know: a policy may not name a relation the
// catalog does not hold. Startup refuses, and a hot reload refuses and KEEPS
// THE PREVIOUS SET rather than installing a policy set weaker than the one
// running (#802's contract, applied to names).

// BindPoliciesToCatalog resolves every relation and column a policy set names
// against cat and returns the BOUND COPY — each name rewritten to the
// catalog's own spelling — or an error naming the first that does not resolve.
//
// It binds a COPY and never touches its inputs. Two reasons, and both are
// contracts this package already makes elsewhere. The evaluator it would
// otherwise rewrite is being READ by every query in flight
// (`PolicyEvaluator.ruleMatches`), so rewriting in place is a data race on a
// live security decision. And a bind that fails partway would leave the
// RUNNING set half-rewritten, which is the opposite of the promise
// `UpdateFromConfig` makes: a policy set that cannot be installed installs
// nothing and the previous one keeps running (#802).
//
// All arguments are optional: a deployment may run ABAC only, legacy
// `policies:` only, or both. A nil catalog binds nothing and returns the
// inputs unchanged — the caller has no catalog to resolve against, and the
// fold-aware comparison in relationEq / policyKey is the floor there.
func BindPoliciesToCatalog(ctx context.Context, cat *catalog.Catalog,
	abacIn []AccessControlPolicy, legacyIn *PolicySet) ([]AccessControlPolicy, *PolicySet, error) {
	if cat == nil {
		return abacIn, legacyIn, nil
	}
	abac, legacy := clonePolicies(abacIn), clonePolicySet(legacyIn)
	if err := bindPoliciesInPlace(ctx, cat, abac, legacy); err != nil {
		return nil, nil, err
	}
	return abac, legacy, nil
}

// clonePolicies deep-copies the parts bindPoliciesInPlace writes: the rules,
// their resource conditions and their obligations. Everything else is shared
// with the original, which is safe because nothing here mutates it.
func clonePolicies(in []AccessControlPolicy) []AccessControlPolicy {
	if in == nil {
		return nil
	}
	out := make([]AccessControlPolicy, len(in))
	copy(out, in)
	for i := range out {
		rules := make([]PolicyRule, len(in[i].Rules))
		copy(rules, in[i].Rules)
		for j := range rules {
			rules[j].Resources = append([]Condition(nil), in[i].Rules[j].Resources...)
			rules[j].Obligations = append([]Obligation(nil), in[i].Rules[j].Obligations...)
		}
		out[i].Rules = rules
	}
	return out
}

// clonePolicySet copies the map and each AccessPolicy value bindLegacyPolicies
// rewrites (its Table and its Columns map).
func clonePolicySet(in *PolicySet) *PolicySet {
	if in == nil {
		return nil
	}
	out := &PolicySet{policies: make(map[string]*AccessPolicy, len(in.policies))}
	for k, p := range in.policies {
		cp := *p
		if p.Columns != nil {
			cp.Columns = make(map[string]ColumnPolicy, len(p.Columns))
			for c, a := range p.Columns {
				cp.Columns[c] = a
			}
		}
		out.policies[k] = &cp
	}
	return out
}

func bindPoliciesInPlace(ctx context.Context, cat *catalog.Catalog,
	abac []AccessControlPolicy, legacy *PolicySet) error {
	for pi := range abac {
		for ri := range abac[pi].Rules {
			rule := &abac[pi].Rules[ri]
			where := fmt.Sprintf("policy %q rule %q", abac[pi].Name, rule.ID)
			named, err := bindRuleResources(ctx, cat, where, rule)
			if err != nil {
				return err
			}
			if err := bindRuleObligations(ctx, cat, where, rule, named); err != nil {
				return err
			}
		}
	}
	return bindLegacyPolicies(ctx, cat, legacy)
}

// bindRuleResources rewrites a rule's relation-naming conditions to the
// catalog's spelling and returns the relations the rule is scoped to. An empty
// return means the rule names no relation — it applies to every one, which is
// what the broad allow does, and there is nothing to resolve.
func bindRuleResources(ctx context.Context, cat *catalog.Catalog, where string, rule *PolicyRule) ([]string, error) {
	var named []string
	for ci := range rule.Resources {
		cond := &rule.Resources[ci]
		if !relationAttributes[cond.Attribute] {
			continue
		}
		switch cond.Op {
		case "eq", "neq":
			s, ok := cond.Value.(string)
			if !ok {
				continue
			}
			resolved, err := resolveRelation(ctx, cat, where, cond.Attribute, s)
			if err != nil {
				return nil, err
			}
			cond.Value = resolved
			if cond.Op == "eq" {
				named = append(named, resolved)
			}
		case "in", "not_in":
			items, ok := relationSetItems(cond.Value)
			if !ok {
				continue
			}
			out := make([]any, 0, len(items))
			for _, it := range items {
				// A role's `tables: ["*"]` is a WILDCARD, not a relation.
				if it == "*" {
					out = append(out, it)
					continue
				}
				resolved, err := resolveRelation(ctx, cat, where, cond.Attribute, it)
				if err != nil {
					return nil, err
				}
				out = append(out, resolved)
				if cond.Op == "in" {
					named = append(named, resolved)
				}
			}
			cond.Value = out
		}
	}
	return named, nil
}

// bindRuleObligations rewrites every column an obligation names to the
// spelling the relation's schema uses, and refuses one that names no column of
// any relation the rule is scoped to.
//
// A rule scoped to NO relation (the broad allow) is not checked: its
// obligations apply to whatever relation the statement names, so there is no
// schema to resolve against here. That is not a hole — such a rule's
// obligations still bind through the fold-aware column matching downstream;
// it is simply not a claim this function can check.
func bindRuleObligations(ctx context.Context, cat *catalog.Catalog, where string,
	rule *PolicyRule, named []string) error {
	if len(named) == 0 {
		return nil
	}
	for oi := range rule.Obligations {
		ob := &rule.Obligations[oi]
		switch ob.Type {
		case "deny_column", "mask_column":
		default:
			continue
		}
		target := strings.TrimSpace(ob.Target)
		if target == "" {
			continue // validateObligation reports this one, with its own message
		}
		resolved, err := resolveColumn(ctx, cat, where, named, target)
		if err != nil {
			return err
		}
		ob.Target = resolved
	}
	return nil
}

func bindLegacyPolicies(ctx context.Context, cat *catalog.Catalog, legacy *PolicySet) error {
	if legacy == nil {
		return nil
	}
	rebuilt := make(map[string]*AccessPolicy, len(legacy.policies))
	for _, p := range legacy.policies {
		if p.Table != "*" {
			where := fmt.Sprintf("policy for table %q role %q", p.Table, p.Role)
			resolved, err := resolveRelation(ctx, cat, where, "table", p.Table)
			if err != nil {
				return err
			}
			if len(p.Columns) > 0 {
				cols := make(map[string]ColumnPolicy, len(p.Columns))
				for col, action := range p.Columns {
					rc, err := resolveColumn(ctx, cat, where, []string{resolved}, col)
					if err != nil {
						return err
					}
					cols[rc] = action
				}
				p.Columns = cols
			}
			p.Table = resolved
		}
		rebuilt[policyKey(p.Table, p.Role)] = p
	}
	legacy.policies = rebuilt
	return nil
}

// resolveRelation is `catalog.ResolveTableName` plus the refusal: the resolver
// answers with the name it was given when nothing matches, so the caller has
// to ask separately whether the answer is a table that exists.
func resolveRelation(ctx context.Context, cat *catalog.Catalog, where, attr, name string) (string, error) {
	if amb := cat.AmbiguousTableNames(name); len(amb) > 1 {
		sort.Strings(amb)
		return "", fmt.Errorf("%s: %s %q matches more than one relation (%s) — "+
			"name one of them exactly", where, attr, name, strings.Join(amb, ", "))
	}
	resolved := cat.ResolveTableName(name)
	if _, err := cat.GetTable(ctx, resolved); err != nil {
		return "", fmt.Errorf("%s: %s %q names no relation in the catalog — "+
			"a policy that cannot be enforced does not load", where, attr, name)
	}
	return resolved, nil
}

// resolveColumn resolves a policed column against the schemas of the relations
// its rule is scoped to. It must resolve against EVERY one of them: a rule
// scoped to two relations claims the column is policed on both, and a target
// that exists on only one is a policy that is enforced on one and silently not
// on the other.
func resolveColumn(ctx context.Context, cat *catalog.Catalog, where string, relations []string, target string) (string, error) {
	var resolved string
	for _, rel := range relations {
		meta, err := cat.GetTable(ctx, rel)
		if err != nil || meta == nil {
			return "", fmt.Errorf("%s: relation %q is not readable", where, rel)
		}
		idx := batch.ResolveSchemaIndex(meta.Schema.Columns, target)
		if idx < 0 {
			return "", fmt.Errorf("%s: column %q does not exist in relation %q — "+
				"a policy that cannot be enforced does not load", where, target, rel)
		}
		name := meta.Schema.Columns[idx].Name
		if resolved != "" && resolved != name {
			return "", fmt.Errorf("%s: column %q resolves to %q and %q in the relations "+
				"this rule is scoped to", where, target, resolved, name)
		}
		resolved = name
	}
	return resolved, nil
}

func relationSetItems(v any) ([]string, bool) {
	switch s := v.(type) {
	case []string:
		return s, true
	case []any:
		out := make([]string, 0, len(s))
		for _, it := range s {
			str, ok := it.(string)
			if !ok {
				return nil, false
			}
			out = append(out, str)
		}
		return out, true
	}
	return nil, false
}
