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

// EnforceDMLPolicies runs at ExecuteParsed before INSERT/UPDATE/DELETE/MERGE
// reads or writes, on embedded, pgwire and HTTP doors (ADR-0033).
// Require ActionWrite (42501); denied columns are absent (42703), including
// predicates, SET targets and expressions. Masked reads use masks (#859).
// DML compiles predicates without a Scan security projection (ADR-0031), so
// substitute soundly or REFUSE; never fall back to stored restricted values.
// Nil/disabled provider is a no-op; enabled auth requires an identity and shared
// table access even without an evaluator. Bind errors refuse.
// See docs/internals/auth-dml-policy-boundary.md for the design.
func EnforceDMLPolicies(ctx context.Context, provider *Provider, cat *catalog.Catalog,
	parsed *plansql.ParsedQuery, protocol string) error {
	if provider == nil || !provider.Enabled() || parsed == nil {
		return nil
	}
	// A policy set that could not be BOUND to the catalog does not enforce,
	// and a statement does not run beside it. See Provider.BindError: an
	// attach that could not resolve a relation or a policed column is a
	// refusal, and the alternative — carrying on with the unbound set — is
	// a rule that matches nothing, which beside a broad allow is a grant
	// (#882, ADR-0033 rule 3).
	if err := provider.BindError(); err != nil {
		return sqlerr.Wrap("42501", err)
	}
	identity := IdentityFromContext(ctx)
	if identity == nil {
		// The same refusal the read path makes: auth is enabled and nobody is
		// here. See EnforcePlanPolicies.
		return sqlerr.New("42501", "permission denied: authentication required")
	}
	table, alias := dmlTarget(parsed)
	if table == "" {
		return nil
	}
	evaluator := provider.Evaluator()
	// The RELATION is named as the statement spelled it, and an unquoted
	// identifier folds to lower case at the lexer (#731). A catalog table
	// keeps the spelling it was registered under, and a mixed-case one is
	// ordinary — a parquet dataset or an Iceberg import brings its own name.
	// `catalog.ResolveTableName` is the concession that makes such a table
	// reachable unquoted, and the DML executors apply it (wadjet/dml.go's
	// `info.Table = db.catalog.ResolveTableName(info.Table)`) — but AFTER
	// this door had already decided. So one relation was policed under
	// `hits` here and read under `Hits` by EnforcePlanPolicies, and a rule
	// scoped `resource.name eq "Hits"` bound to the SELECT and not to the
	// write. Where a broader rule grants the identity access — which is what
	// every `roles:`-to-ABAC migration emits — the obligations simply
	// vanished: the write was permitted and the masked columns were not
	// masked. Decide on the same name the read decides on.
	if cat != nil {
		table = cat.ResolveTableName(table)
	}

	// 1. The write itself — the SHARED decision, in both provider shapes, so
	// the answer a metadata door gives about this relation and the answer this
	// door gives are one answer (ADR-0034 item 5). With an evaluator installed
	// this used to ask the evaluator alone, which skipped the role's `allow`
	// list; with none it enforced nothing at all.
	if err := tableAccess(ctx, provider, table, ActionWrite, protocol); err != nil {
		return err
	}

	// 2. Reading it — but ONLY when the statement reads it.
	//
	// This was a blanket requirement, so a role holding `write` and not `read`
	// was refused an INSERT and an unqualified DELETE that PostgreSQL allows:
	// PostgreSQL requires SELECT for the PREDICATE, not for the write (measured
	// on the oracle — `INSERT` and `DELETE FROM t` succeed on INSERT/DELETE
	// alone, `DELETE FROM t WHERE id=1` does not). `TableAccess(ActionWrite)`
	// said one thing and this door said another about the same identity, which
	// is the disagreement this ADR exists to remove.
	//
	// A statement that DOES read the relation still needs the read: a
	// predicate is a way to observe a stored value, which is the whole of
	// rule 2 below.
	readErr := tableAccess(ctx, provider, table, ActionRead, protocol)
	if readErr != nil && dmlReadsTarget(parsed) {
		return readErr
	}
	if evaluator == nil || readErr != nil {
		// No obligations to apply: either no policy set is installed, or the
		// identity may not read this relation and the statement does not.
		return nil
	}
	r := newPolicyResolver(ctx, cat, evaluator, identity.ToSubject(),
		DecisionEnvironment(ctx, protocol))

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

// dmlReadsTarget reports whether the statement OBSERVES the relation it
// writes — the condition PostgreSQL attaches the SELECT privilege to, and the
// only way a stored value can leave through a write.
//
//   - DELETE reads it when its WHERE names a column. `WHERE 1=0` names none,
//     and PostgreSQL asks for SELECT on the COLUMNS a statement references.
//   - UPDATE reads it when its WHERE names a column, or when a SET value is an
//     expression naming one (`SET a = b`, `SET n = n + 1`). `SET a = 1` names
//     none and reads nothing.
//   - INSERT does not: its value expressions have no row to read from. (An
//     `INSERT … SELECT` source is a different relation and the plan path
//     polices it with `read`.)
//   - MERGE reads it: the ON condition and every WHEN clause do.
func dmlReadsTarget(parsed *plansql.ParsedQuery) bool {
	reads := func(exprSQL string) bool {
		if strings.TrimSpace(exprSQL) == "" {
			return false
		}
		ast, err := plansql.ParseExpression(exprSQL)
		if err != nil || ast == nil {
			return true // unreadable: assume it reads, and require the read
		}
		refs, rerr := plansql.ColumnRefs(ast)
		if rerr != nil {
			return true
		}
		return len(refs) > 0
	}
	switch parsed.Type {
	case plansql.QueryDelete:
		return parsed.Delete != nil && reads(parsed.Delete.WhereSQL)
	case plansql.QueryUpdate:
		if parsed.Update == nil {
			return false
		}
		if reads(parsed.Update.WhereSQL) {
			return true
		}
		for _, sc := range parsed.Update.SetClauses {
			if reads(sc.Value) {
				return true
			}
		}
		return false
	case plansql.QueryMerge:
		return true
	}
	return false
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
