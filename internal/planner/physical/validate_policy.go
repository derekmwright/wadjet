package physical

import (
	"context"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// policedColumnSource is a tableColumnSource with a column-policy's DENIED
// columns removed from every table's declared schema.
//
// A denied column does not exist for this identity, and "does not exist" is
// a statement the name binder already knows how to make: `column "salary"
// does not exist`, SQLSTATE 42703, PostgreSQL's own message shape. Before
// #859 a denied column was merely dropped from the security projection, so
// `SELECT salary FROM t` came back as a phantom all-NULL column on the
// single-process path and as the whole `SELECT *` row on the DAG, and
// `WHERE salary > 0` was answered from the RAW column below the projection
// — a working oracle for the value the policy denies.
type policedColumnSource struct {
	src tableColumnSource
	// deniedFor answers, for one table, the columns this identity may not
	// see (folded). It is a FUNCTION and not a map because the binder is what
	// enumerates the relations a statement reads — CTE bodies, derived
	// tables, subquery blocks and set-operation arms included — and it does
	// so before any plan exists. Asking it per table as the binder resolves
	// them is what lets the policy be known at the FIRST name-binding pass,
	// so `SELECT nosuchcol FROM t` cannot answer with a hint that lists a
	// column the policy denies.
	deniedFor func(table string) map[string]bool
	// denyTable is the TABLE decision, asked for one relation at the moment
	// the binder resolves it, and nil when no caller can ask (#946).
	//
	// It has to be here and not in front of the binder. The binder POOLS the
	// schemas of every relation a statement resolves before it writes its
	// hint: `SELECT id FROM emp WHERE id = (SELECT MAX(nocol) FROM secret)`
	// answered `unknown column "nocol" (available: acct, amt, dept, id, note,
	// salary, ssn)` — emp's columns UNIONED with secret's `note` — for an
	// identity that may not read `secret` at all. So the decision has to
	// precede the resolution of EVERY relation the binder touches, which is
	// CTE bodies, derived tables, subquery blocks and set-operation arms as
	// well as the FROM list, and this is the one seam that sees all of them.
	//
	// It is asked AFTER the relation is found, so a name that is not a table
	// keeps PostgreSQL's 42P01 and only an existing-but-denied relation earns
	// the 42501. `docs/security.md` and ADR-0034: metadata follows the table
	// decision, and an error's hint is metadata.
	denyTable func(table string) error
}

func (p policedColumnSource) GetTable(ctx context.Context, name string) (*catalog.TableMeta, error) {
	meta, err := p.src.GetTable(ctx, name)
	if err != nil || meta == nil {
		return meta, err
	}
	if p.denyTable != nil {
		if derr := p.denyTable(resolveTableSpelling(p.src, name)); derr != nil {
			return nil, derr
		}
	}
	drop := p.deniedFor(resolveTableSpelling(p.src, name))
	if len(drop) == 0 {
		drop = p.deniedFor(name)
	}
	if len(drop) == 0 {
		return meta, nil
	}
	// Copy: the catalog's TableMeta is shared and must not lose columns for
	// every other identity in the process.
	cols := make([]parquet.Column, 0, len(meta.Schema.Columns))
	for _, c := range meta.Schema.Columns {
		if drop[strings.ToLower(c.Name)] {
			continue
		}
		cols = append(cols, c)
	}
	out := *meta
	out.Schema = parquet.Schema{Columns: cols}
	return &out, nil
}

// ResolveTableName / AmbiguousTableNames forward so the wrapper keeps the
// binder's relation-name case concession (#731).
func (p policedColumnSource) ResolveTableName(name string) string {
	return resolveTableSpelling(p.src, name)
}

func (p policedColumnSource) AmbiguousTableNames(name string) []string {
	if r, ok := p.src.(tableNameResolver); ok {
		return r.AmbiguousTableNames(name)
	}
	return nil
}

// ValidateColumnsUnderPolicy is ValidateColumns run against the schema THIS
// IDENTITY can see: every column deniedFor names is removed from its table
// before binding, so a reference to one resolves to nothing and comes back as
// the ordinary 42703 — the same error, byte for byte, that a column the table
// really does not have produces. That equality IS the meaning of "denied":
// nothing about the column, not even its name in an error's hint, survives
// the policy.
//
// denyTable is the TABLE decision for the same relations, asked as the binder
// resolves each one so a DENIED relation's column list never reaches a hint
// (#946). It is separate from deniedFor because the two answers have different
// shapes and different refusals: a denied COLUMN does not exist (42703, and
// nothing about it survives), a denied RELATION is a refusal (42501, and
// nothing about it is published).
//
// Both nil, or a nil catalog, is the plain unfiltered validation.
func ValidateColumnsUnderPolicy(ctx context.Context, cat *catalog.Catalog, info *plansql.SelectInfo,
	deniedFor func(table string) map[string]bool, denyTable func(table string) error) error {
	if cat == nil || info == nil {
		return nil
	}
	if deniedFor == nil && denyTable == nil {
		return validateColumns(ctx, cat, info)
	}
	if deniedFor == nil {
		deniedFor = func(string) map[string]bool { return nil }
	}
	return validateColumns(ctx,
		policedColumnSource{src: cat, deniedFor: deniedFor, denyTable: denyTable}, info)
}

// applyContextColumnPolicies enforces the query's policy on a plan this
// planner built for ITSELF — the expression-subquery path and the DAG's
// scalar-producer path, neither of which passes through
// auth.EnforcePlanPolicies.
//
// EVERY RELATION THIS PLAN READS ASKS THE CONTEXT LOOKUP (#945). That is the
// same decision `EnforcePlanPolicies` asks for the relations the statement's
// own plan named and `EnforceOptimizedPlan` asks for a scan the optimizer
// minted — access first, then the obligations that follow from it. A scalar
// subquery in the SELECT list is SQL TEXT when enforcement runs, so
// `policedRelations` cannot see it and this is the FIRST place its relation is
// known; before this pass asked, `SELECT (SELECT MAX(id) FROM other)` answered
// the value on every door, under both provider shapes, for an identity whose
// role does not list `other` and for one an ABAC policy denies it to. The
// spelling does not matter — SELECT list, WHERE, CASE, CTE body — because the
// refusal is at the site that turns the text into a plan, not at a walk of the
// text.
//
// The pass used to return early whenever the RESOLVED policy set was empty,
// which is exactly the case a subquery-only relation produces: the outer
// statement carries no policed relation, so there is nothing in the set and
// the lookup — the one carrier that knows about the relation — went unasked.
// The obligations follow the same seam: a row filter bound to a relation
// reached only from a subquery restricted nothing, and a masked relation
// reached only from a subquery refused (no projection could be found above its
// scan) instead of answering the mask.
//
// It returns an error when the identity may not read a relation this plan
// reads, and when a policed scan could not be covered — the alternative to
// both is answering that subquery from the raw column.
func (p *Planner) applyContextColumnPolicies(ctx context.Context, plan *logical.Node) (*logical.Node, error) {
	pol := logical.ColumnPoliciesFromContext(ctx)
	lookup := logical.PolicyLookupFromContext(ctx)
	if plan == nil || (len(pol) == 0 && lookup == nil) {
		return plan, nil
	}
	// A SLICE, not a map, for the same reason EnforcePlanPolicies keeps one:
	// the filters are injected below and a map would order the Filter nodes
	// differently from run to run.
	type tableFilter struct{ table, filter string }
	var rowFilters []tableFilter
	merged, copied := pol, false
	if lookup != nil {
		for _, table := range logical.PolicedScanTables(plan) {
			cols, rowFilter, err := lookup(table)
			if err != nil {
				// The shared decision's own refusal — sqlerr 42501,
				// `permission denied for table "x"`, no rule id. Returned
				// BEFORE any pipeline is built, so nothing is read.
				return nil, err
			}
			if len(cols) > 0 && len(merged.For(table)) == 0 {
				if !copied {
					m := make(logical.TablePolicies, len(pol)+1)
					for k, v := range pol {
						m[k] = v
					}
					merged, copied = m, true
				}
				merged[strings.ToLower(table)] = cols
			}
			if rowFilter != "" {
				rowFilters = append(rowFilters, tableFilter{table, rowFilter})
			}
		}
	}
	plan, unprotected := merged.Apply(plan, func(table string) []string {
		return p.policyTableColumns(ctx, table)
	})
	if unprotected > 0 {
		return nil, logical.ErrColumnPolicyUnenforceable
	}
	// AFTER the security projection so they land BELOW it, directly above the
	// scan: the policy's own predicate reads the row as stored (ADR-0033
	// decision 6), which is the order EnforcePlanPolicies uses for the
	// statement's own plan.
	for _, rf := range rowFilters {
		plan = logical.InjectRowFilter(plan, rf.table, rf.filter)
	}
	return plan, nil
}

// applyContextColumnPoliciesToNewScans is applyContextColumnPolicies for the
// scans the OPTIMIZER minted — decorrelation re-parses a subquery and builds a
// fresh Scan, after the policy went in. A scan already under a security
// barrier is skipped.
//
// The LOOKUP alone is enough to run this pass: a subquery whose relation the
// resolved set never saw has no entry in that set, and the scan the optimizer
// mints for a nested `IN (SELECT …)` INSIDE that subquery is exactly the scan
// this pass exists for (#945).
func (p *Planner) applyContextColumnPoliciesToNewScans(ctx context.Context, plan *logical.Node) (*logical.Node, error) {
	pol := logical.ColumnPoliciesFromContext(ctx)
	lookup := logical.PolicyLookupFromContext(ctx)
	if len(pol) == 0 && lookup == nil {
		return plan, nil
	}
	plan, unprotected, err := pol.ApplyToNewScansWithLookup(plan, func(table string) []string {
		return p.policyTableColumns(ctx, table)
	}, lookup)
	if err != nil {
		return nil, err
	}
	if unprotected > 0 {
		return nil, logical.ErrColumnPolicyUnenforceable
	}
	return plan, nil
}

func (p *Planner) policyTableColumns(ctx context.Context, table string) []string {
	if p.catalog == nil {
		return nil
	}
	meta, err := p.catalog.GetTable(ctx, table)
	if err != nil || meta == nil {
		return nil
	}
	cols := make([]string, len(meta.Schema.Columns))
	for i, c := range meta.Schema.Columns {
		cols[i] = c.Name
	}
	return cols
}

// checkPolicyPlanOrderFromContext runs logical.CheckPolicyPlanOrder over a
// plan this planner built for itself — a subquery pipeline, a scalar producer
// — using the context's policies and per-table lookup.
//
// The invariant has to be asked about EVERY plan the query builds, not only
// the statement's own. A subquery is planned here, optimized here, and its
// predicates are pushed here; a predicate that ends up between a security
// projection and its scan reads the stored column just as surely as one in the
// outer plan, and the shape that does it — a derived table, a set operation or
// a correlation inside the subquery — is one no per-shape teaching can
// enumerate (#859 round 4).
func (p *Planner) checkPolicyPlanOrderFromContext(ctx context.Context, plan *logical.Node) error {
	pol := logical.ColumnPoliciesFromContext(ctx)
	lookup := logical.PolicyLookupFromContext(ctx)
	if len(pol) == 0 && lookup == nil {
		return nil
	}
	return logical.CheckPolicyPlanOrder(plan, func(table string) []logical.ColumnPolicy {
		if cols := pol.For(table); len(cols) > 0 {
			return cols
		}
		if lookup == nil {
			return nil
		}
		cols, _, err := lookup(table)
		if err != nil {
			// An ACCESS denial, not an ordering fault. It was the only place
			// this path ever met the lookup, and dropping it here is how a
			// denied relation reached a scalar subquery (#945); the refusal
			// now happens in applyContextColumnPolicies, above, before this
			// plan is optimized — so by the time control reaches here the
			// relation is one the identity may read.
			return nil
		}
		return cols
	})
}
