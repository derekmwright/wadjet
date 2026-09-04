package auth

import (
	"context"
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// EnforceDMLPolicies applies ABAC to an INSERT / UPDATE / DELETE / MERGE
// before any row is read or written. It is called from the ONE DML entry point
// (wadjet.DB.ExecuteParsed), so the embedded door, the pgwire door and the
// HTTP API server all carry it.
//
// Two rules, and they are the SELECT door's rules said for a statement that
// writes (ADR-0033):
//
//  1. **A DML statement is an ActionWrite.** An identity whose policies grant
//     it no write on the table is refused with 42501 before anything happens.
//     A role allowed only `read` used to be able to run
//     `DELETE FROM t WHERE ssn = '<stored value>'` and destroy the row.
//
//  2. **A read inside the statement sees what a SELECT would see.** A column
//     the policy DENIES does not exist, so naming it in a predicate, a SET
//     target or a SET expression is 42703 — before #859 `UPDATE t SET dept='z'
//     WHERE salary = 700009` matched exactly the row with that salary, a
//     working oracle for a column the identity may not read. A MASKED column
//     reads as its mask, so `WHERE ssn = '<stored value>'` matches nothing and
//     `SET dept = ssn` writes '***' instead of copying the stored value into a
//     column the identity may read.
//
// Rule 2 is a SUBSTITUTION rather than a projection because a DML predicate is
// compiled, not planned (ADR-0031): there is no Scan for a security projection
// to sit on. Where the substitution cannot be done soundly the statement is
// REFUSED, never run against the stored row.
//
// No-ops when the provider is nil/disabled, no identity is attached, or the
// provider has no evaluator — the same contract EnforcePlanPolicies keeps.
func EnforceDMLPolicies(ctx context.Context, provider *Provider, cat *catalog.Catalog,
	parsed *plansql.ParsedQuery, protocol string) error {
	if provider == nil || !provider.Enabled() || parsed == nil {
		return nil
	}
	identity := IdentityFromContext(ctx)
	if identity == nil {
		return nil
	}
	evaluator := provider.Evaluator()
	if evaluator == nil {
		return nil
	}

	table, alias := dmlTarget(parsed)
	if table == "" {
		return nil
	}
	r := newPolicyResolver(ctx, cat, evaluator, identity.ToSubject(), Environment{Protocol: protocol})

	// 1. The write itself.
	if !evaluator.EvaluateTableAccess(r.subject, table, ActionWrite, r.env).Allowed {
		return sqlerr.New("42501", "permission denied for table %q", table)
	}
	// Reading the table at all is still a read decision: a statement that may
	// not read it may not use it as a predicate either.
	if td := r.decide(table); !td.Allowed {
		return sqlerr.New("42501", "permission denied for table %q: %s", table, td.Reason)
	}

	policies, err := r.columnPolicies(table)
	if err != nil {
		return err
	}
	if len(policies) == 0 {
		return nil
	}
	auditColumnDecision(provider, identity, table, policies)
	denied := map[string]bool{}
	for _, p := range policies {
		if p.Denied {
			denied[strings.ToLower(p.Column)] = true
		}
	}

	relation := alias
	if relation == "" {
		relation = table
	}

	// 2. Every raw expression the statement carries.
	rewrite := func(exprSQL string) (string, error) {
		if strings.TrimSpace(exprSQL) == "" {
			return exprSQL, nil
		}
		ast, perr := plansql.ParseExpression(exprSQL)
		if perr != nil {
			return exprSQL, nil // the executor's own parse reports it
		}
		refs, rerr := plansql.ColumnRefs(ast)
		if rerr != nil {
			// A node the ref walker cannot see through. The substitution
			// below cannot be trusted either, so refuse rather than run it
			// against the stored row.
			return "", sqlerr.Wrap("0A000", fmt.Errorf(
				"a security policy applies to %q and this expression cannot be rewritten to "+
					"honour it: %w", table, rerr))
		}
		for _, ref := range refs {
			if denied[strings.ToLower(ref.Column)] {
				return "", sqlerr.New("42703", "column %q does not exist", ref.Column)
			}
		}
		out, ok := logical.SubstituteMaskedColumns(ast, relation, policies)
		if !ok || out == nil {
			return "", sqlerr.Wrap("0A000", fmt.Errorf(
				"a security policy applies to %q and this expression cannot be rewritten to "+
					"honour it", table))
		}
		return out.String(), nil
	}

	// A SET or INSERT TARGET naming a denied column is 42703 the same way a
	// read of it is: the column does not exist for this identity.
	checkTarget := func(name string) error {
		if denied[strings.ToLower(strings.TrimSpace(name))] {
			return sqlerr.New("42703", "column %q of relation %q does not exist", name, table)
		}
		return nil
	}

	switch parsed.Type {
	case plansql.QueryDelete:
		w, err := rewrite(parsed.Delete.WhereSQL)
		if err != nil {
			return err
		}
		parsed.Delete.WhereSQL = w
	case plansql.QueryUpdate:
		w, err := rewrite(parsed.Update.WhereSQL)
		if err != nil {
			return err
		}
		parsed.Update.WhereSQL = w
		for i := range parsed.Update.SetClauses {
			if err := checkTarget(parsed.Update.SetClauses[i].Column); err != nil {
				return err
			}
			v, err := rewrite(parsed.Update.SetClauses[i].Value)
			if err != nil {
				return err
			}
			parsed.Update.SetClauses[i].Value = v
		}
	case plansql.QueryInsert:
		for _, c := range parsed.Insert.Columns {
			if err := checkTarget(c); err != nil {
				return err
			}
		}
	case plansql.QueryMerge:
		// MERGE reads the TARGET row in its ON condition and in every WHEN
		// clause, and its clauses carry raw SET/VALUES text this rewriter
		// does not decompose. Refusing is the honest disposition until it
		// does: running it would compile those reads against the stored row.
		return sqlerr.Wrap("0A000", fmt.Errorf(
			"MERGE is not available on %q for this identity: a column security policy applies "+
				"and the statement's clauses are not rewritten to honour it", table))
	}
	return nil
}

// dmlTarget is the table a DML statement writes, and the alias that hides its
// name when the statement gave one.
func dmlTarget(parsed *plansql.ParsedQuery) (table, alias string) {
	switch parsed.Type {
	case plansql.QueryInsert:
		if parsed.Insert != nil {
			return parsed.Insert.Table, ""
		}
	case plansql.QueryUpdate:
		if parsed.Update != nil {
			return parsed.Update.Table, parsed.Update.Alias
		}
	case plansql.QueryDelete:
		if parsed.Delete != nil {
			return parsed.Delete.Table, parsed.Delete.Alias
		}
	case plansql.QueryMerge:
		if parsed.Merge != nil {
			return parsed.Merge.Target, parsed.Merge.TargetAlias
		}
	}
	return "", ""
}
