package auth

import (
	"context"
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// EnforcePlanPolicies applies ABAC to a query at plan level: table-access
// denial, column deny/mask injection and row-filter injection for every table
// the plan READS. It is THE shared enforcement path — the embedded engine
// (wadjet.DB.Query), the HTTP door and the coordinator's native-DAG executor
// all call it with the same inputs, so an identity sees identical policy
// behavior regardless of which door and which execution path answers.
//
// Masking and denial are PLAN-TIME, at the scan, unconditionally (#859):
//
//   - The security projection is built from the TABLE's catalog schema, which
//     is always known here, and never from the scan's pruned column list.
//     `SELECT *`, an aggregate-only SELECT list and a derived table all leave
//     that list empty, and those are exactly the queries a mask matters most
//     for.
//   - The relations to police come from the PLAN, not from the statement's
//     FROM list. `plansql.SelectInfo.Tables` carries a derived table under its
//     own subquery TEXT, a CTE reference under the CTE's name, and NOTHING at
//     all for the arms of a UNION — so a `UNION ALL` over a masked column was
//     unmasked on every door, and a query with a derived table or a CTE was
//     default-DENIED under the name `"(SELECT ...)"`.
//   - A column policy that cannot be applied REFUSES. A security control never
//     degrades to a grant (#802).
//
// The returned context carries the resolved policies so the physical planner
// applies the same projection to an expression subquery, which it plans on its
// own (physical.buildSubqueryPipeline).
//
// No-ops (returns the plan unchanged) when the provider is nil/disabled, no
// identity is attached to ctx, or the provider has no evaluator — matching the
// embedded engine's historical behavior. protocol labels the evaluation
// environment for policy conditions and audit.
func EnforcePlanPolicies(ctx context.Context, provider *Provider, cat *catalog.Catalog, selectInfo *plansql.SelectInfo, plan *logical.Node, protocol string) (context.Context, *logical.Node, error) {
	if provider == nil || !provider.Enabled() {
		return ctx, plan, nil
	}
	identity := IdentityFromContext(ctx)
	if identity == nil {
		return ctx, plan, nil
	}
	evaluator := provider.Evaluator()
	if evaluator == nil {
		return ctx, plan, nil
	}

	r := newPolicyResolver(ctx, cat, evaluator, identity.ToSubject(), Environment{Protocol: protocol})

	policies := make(logical.TablePolicies)
	// A SLICE, not a map: the filters are injected below, and iterating a map
	// would order the Filter nodes differently from run to run — which shows
	// up in EXPLAIN output for any statement with two policed tables.
	type tableFilter struct{ table, filter string }
	var rowFilters []tableFilter
	for _, tableName := range policedRelations(ctx, cat, selectInfo, plan) {
		td := r.decide(tableName)
		if !td.Allowed {
			return ctx, nil, fmt.Errorf("access denied to table %q: %s", tableName, td.Reason)
		}
		if td.RowFilter != "" {
			rowFilters = append(rowFilters, tableFilter{tableName, td.RowFilter})
		}
		cp, cperr := r.columnPolicies(tableName)
		if cperr != nil {
			return ctx, nil, cperr
		}
		if len(cp) > 0 {
			policies[strings.ToLower(tableName)] = cp
		}
	}

	if len(policies) > 0 {
		// A denied column DOES NOT EXIST for this identity: a reference to it
		// is 42703, never a NULL column and never a predicate quietly answered
		// from the raw value below the projection. The binder that already
		// produces that error does it here over the schema the policy leaves
		// behind — a second time, because the plan can name relations the
		// statement's FROM list does not (a UNION arm).
		if err := physical.ValidateColumnsUnderPolicy(ctx, cat, selectInfo, r.deniedColumns); err != nil {
			return ctx, nil, err
		}
		var unprotected int
		plan, unprotected = policies.Apply(plan, r.tableColumns)
		if unprotected > 0 {
			return ctx, nil, logical.ErrColumnPolicyUnenforceable
		}
		ctx = logical.ContextWithColumnPolicies(ctx, policies)
	}

	// Row filters go on AFTER the security projection so they land BELOW it,
	// directly above the scan: the POLICY's predicate reads the row as
	// stored, which is PostgreSQL's RLS ordering, while a predicate the USER
	// writes stays above the projection and compares the mask.
	for _, rf := range rowFilters {
		plan = logical.InjectRowFilter(plan, rf.table, rf.filter)
	}
	return ctx, plan, nil
}

// ValidateStatementColumns is the plan-time name binding every query entry
// point runs before it builds a logical plan, done over the schema the CALLING
// IDENTITY can see.
//
// It replaces a bare physical.Planner.ValidateColumns at those entry points.
// The unfiltered binder answers `SELECT nosuchcol FROM t` with a hint that
// lists the table's columns, and a column the policy DENIES has no business in
// that list: an identity that may not read `salary` may not learn that
// `salary` exists either. The policy is resolved LAZILY, per table, as the
// binder resolves relations — so it covers CTE bodies, derived tables,
// subquery blocks and set-operation arms without needing a plan.
//
// Table-level denial is NOT decided here. It stays in EnforcePlanPolicies,
// after the logical build, so the order in which a query on a denied table
// meets its two possible refusals does not change.
func ValidateStatementColumns(ctx context.Context, provider *Provider, cat *catalog.Catalog, info *plansql.SelectInfo, protocol string) error {
	if cat == nil || info == nil {
		return nil
	}
	var deniedFor func(string) map[string]bool
	if provider != nil && provider.Enabled() {
		if identity := IdentityFromContext(ctx); identity != nil {
			if evaluator := provider.Evaluator(); evaluator != nil {
				r := newPolicyResolver(ctx, cat, evaluator, identity.ToSubject(),
					Environment{Protocol: protocol})
				deniedFor = r.deniedColumns
			}
		}
	}
	return physical.ValidateColumnsUnderPolicy(ctx, cat, info, deniedFor)
}

// policyResolver evaluates one identity's table decisions on demand and
// remembers them, so the binder, the projection and the row filters all read
// the same answer for a table and the evaluator runs once per relation.
type policyResolver struct {
	ctx       context.Context
	cat       *catalog.Catalog
	evaluator *PolicyEvaluator
	subject   Subject
	env       Environment
	cache     map[string]*TableDecision
	cols      map[string][]string
}

func newPolicyResolver(ctx context.Context, cat *catalog.Catalog, evaluator *PolicyEvaluator,
	subject Subject, env Environment) *policyResolver {
	return &policyResolver{
		ctx: ctx, cat: cat, evaluator: evaluator, subject: subject, env: env,
		cache: map[string]*TableDecision{}, cols: map[string][]string{},
	}
}

func (r *policyResolver) decide(table string) *TableDecision {
	key := strings.ToLower(table)
	if td, ok := r.cache[key]; ok {
		return td
	}
	td := r.evaluator.EvaluateTableAccess(r.subject, table, ActionRead, r.env)
	r.cache[key] = td
	return td
}

// deniedColumns is the set of columns this identity may not see on a table,
// folded. A table the policy denies OUTRIGHT contributes no denied columns
// here: its refusal is EnforcePlanPolicies' table-access error, and answering
// "every column is missing" from the binder would replace it with a worse one.
func (r *policyResolver) deniedColumns(table string) map[string]bool {
	td := r.decide(table)
	if !td.Allowed || len(td.Columns) == 0 {
		return nil
	}
	var out map[string]bool
	for _, col := range td.Columns {
		if col.Allowed {
			continue
		}
		if out == nil {
			out = map[string]bool{}
		}
		out[strings.ToLower(col.Column)] = true
	}
	return out
}

// columnPolicies is the table's deny/mask list in the form the plan takes.
//
// A mask obligation carrying NEITHER a value nor a mask func is an ERROR, not
// a skip. It used to `continue`, so the obligation vanished and the column
// came back in the clear on every door — the exact shape ADR-0033 decision 4
// forbids, a control that cannot be applied degrading silently to a grant.
// Configuration cannot reach this any more (ValidateABACPolicies refuses it at
// load), so this is the guard for a provider built in process.
func (r *policyResolver) columnPolicies(table string) ([]logical.ColumnPolicy, error) {
	td := r.decide(table)
	if !td.Allowed {
		return nil, nil
	}
	// The columns this decision takes away, for the mask-expression check
	// below: a mask runs against the row AS STORED, so one that reads a
	// policed column publishes the value the policy hides.
	restricted := make(map[string]bool, len(td.Columns))
	for _, col := range td.Columns {
		if !col.Allowed || col.MaskFunc != "" || col.MaskExpr != "" {
			restricted[strings.ToLower(col.Column)] = true
		}
	}

	var out []logical.ColumnPolicy
	for _, col := range td.Columns {
		if !col.Allowed {
			out = append(out, logical.ColumnPolicy{Column: col.Column, Denied: true})
			continue
		}
		if col.MaskFunc == "" && col.MaskExpr == "" {
			return nil, fmt.Errorf("%w: mask_column on %q.%q names neither a value nor a mask_func",
				logical.ErrColumnPolicyUnenforceable, table, col.Column)
		}
		expr := r.maskExpression(col.MaskExpr, table, col.Column)
		ast, perr := plansql.ParseExpression(expr)
		if perr != nil {
			// Not a SQL expression. The projection's fallback would quote it
			// and carry on, which is a mask silently redefining itself.
			return nil, fmt.Errorf("%w: the mask for %q.%q is not a SQL expression: %q "+
				"(a literal string needs its quotes)",
				logical.ErrColumnPolicyUnenforceable, table, col.Column, expr)
		}
		if lost, ok := maskExpressionLosesText(expr, ast); !ok {
			return nil, fmt.Errorf("%w: the mask for %q.%q is not read as written: %q loses %q",
				logical.ErrColumnPolicyUnenforceable, table, col.Column, expr, lost)
		}
		if read, bad := maskReadsRestricted(ast, restricted); bad {
			return nil, fmt.Errorf("%w: the mask for %q.%q reads %q, which the same policy "+
				"restricts", logical.ErrColumnPolicyUnenforceable, table, col.Column, read)
		}
		out = append(out, logical.ColumnPolicy{Column: col.Column, MaskExpr: expr})
	}
	return out, nil
}

// tableColumns is the table's declared column list, from the catalog.
func (r *policyResolver) tableColumns(table string) []string {
	key := strings.ToLower(table)
	if cols, ok := r.cols[key]; ok {
		return cols
	}
	var cols []string
	if r.cat != nil {
		if meta, err := r.cat.GetTable(r.ctx, table); err == nil && meta != nil {
			cols = make([]string, len(meta.Schema.Columns))
			for i, c := range meta.Schema.Columns {
				cols[i] = c.Name
			}
		}
	}
	r.cols[key] = cols
	return cols
}

// policedRelations lists every base table this query READS, once each.
//
// The plan is the authority — it names the real relation behind a derived
// table, a CTE reference and each arm of a set operation. The statement's FROM
// list is unioned in for the names the catalog recognises as tables, so a
// table the plan does not carry a Scan for is still access-checked; the names
// it carries that are NOT tables (a derived table's subquery text, a CTE
// alias) are dropped rather than default-denied.
func policedRelations(ctx context.Context, cat *catalog.Catalog, info *plansql.SelectInfo, plan *logical.Node) []string {
	var out []string
	seen := map[string]bool{}
	add := func(name string) {
		if name == "" {
			return
		}
		key := strings.ToLower(name)
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, name)
	}
	for _, t := range logical.PolicedScanTables(plan) {
		add(t)
	}
	for _, t := range StatementBaseTables(ctx, cat, info) {
		add(t)
	}
	return out
}

// StatementBaseTables lists the relations a statement's FROM and JOIN clauses
// name that the catalog recognises as TABLES.
//
// It is the filter every access check needs, because plansql.SelectInfo.Tables
// is not a list of tables: a derived table appears under its own subquery TEXT
// (`"(SELECT ssn FROM t)"`) and a CTE reference under the CTE's name. Handing
// those to a default-deny evaluator refused every query with a derived table
// or a CTE under a policy that named neither (#859). A table function is not a
// catalog relation and is skipped for the same reason.
//
// With no catalog to ask, it returns the FROM-list names unchanged — the
// pre-#859 behavior for a caller that cannot tell a table from a subquery.
func StatementBaseTables(ctx context.Context, cat *catalog.Catalog, info *plansql.SelectInfo) []string {
	if info == nil {
		return nil
	}
	known := func(name string) bool {
		if name == "" || strings.ContainsAny(name, "( )") {
			return false
		}
		if cat == nil {
			return true
		}
		_, err := cat.GetTable(ctx, name)
		return err == nil
	}
	var out []string
	seen := map[string]bool{}
	add := func(name string) {
		key := strings.ToLower(name)
		if seen[key] {
			return
		}
		seen[key] = true
		out = append(out, name)
	}
	for _, t := range info.Tables {
		if !t.IsFunction && known(t.Name) {
			add(t.Name)
		}
	}
	for _, j := range info.Joins {
		if known(j.RightTable) {
			add(j.RightTable)
		}
	}
	return out
}

// maskExpression is the SQL expression that replaces a masked column.
//
// An explicit `value:` on the obligation wins. Otherwise the mask is chosen
// from the column's DECLARED TYPE, because the mask now replaces the column
// for every consumer above the scan: a bare `'***'` over a BIGINT column used
// to be harmless only because the mask was applied to result rows after
// execution, and as a plan-time projection it would make `SUM(acct)` a type
// error. This is the same table the deleted row-side defaultMaskValue used,
// moved to the one place that still decides.
func (r *policyResolver) maskExpression(explicit, table, column string) string {
	if explicit != "" {
		return explicit
	}
	if r.cat != nil {
		if meta, err := r.cat.GetTable(r.ctx, table); err == nil && meta != nil {
			for _, c := range meta.Schema.Columns {
				if !strings.EqualFold(c.Name, column) {
					continue
				}
				switch c.Type {
				case parquet.TypeInt32, parquet.TypeInt64, parquet.TypeFloat32,
					parquet.TypeFloat64, parquet.TypeDecimal:
					return "0"
				case parquet.TypeBool:
					return "false"
				}
				break
			}
		}
	}
	return "'***'"
}
