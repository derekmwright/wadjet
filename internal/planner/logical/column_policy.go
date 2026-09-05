package logical

import (
	"context"
	"errors"
	"fmt"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// ErrColumnPolicyUnenforceable is returned when a column policy names a table
// the plan reads but the security projection could not be built for it — no
// catalog schema and no scan annotation to build the projection from, or every
// column denied.
//
// It is an ERROR and not a silently unmasked answer on purpose: a security
// control that cannot be applied refuses, the way an unreadable `columns:`
// action refuses to load (#802). Loud beats plausible.
var ErrColumnPolicyUnenforceable = errors.New("column policy could not be enforced on this plan: refusing to answer unmasked")

// TablePolicies is the set of column policies in force for one query, keyed by
// the FOLDED base-table name.
//
// It travels on the context because a query is planned in more than one place.
// The statement's own plan is enforced by auth.EnforcePlanPolicies before the
// optimizer runs, but an expression subquery — `(SELECT MAX(ssn) FROM t)`, an
// IN set, an EXISTS — is a WHOLE SECOND QUERY that the physical planner
// parses, builds and optimizes on its own (physical.buildSubqueryPipeline),
// and it never passed through the enforcement path at all. Before #859 that
// second path answered from the raw column while the first one masked.
//
// The context is the only carrier both paths already share.
type TablePolicies map[string][]ColumnPolicy

type columnPolicyKey struct{}

// ContextWithColumnPolicies returns ctx carrying the column policies in force.
// A nil or empty map returns ctx unchanged, so an unpoliced query costs
// nothing and every reader can test for absence with len().
func ContextWithColumnPolicies(ctx context.Context, tp TablePolicies) context.Context {
	if len(tp) == 0 {
		return ctx
	}
	return context.WithValue(ctx, columnPolicyKey{}, tp)
}

// PolicyLookup answers, for one base table, what this identity's policy does
// to it: the column obligations, the row filter, and an error when the
// identity may not read the table at all.
//
// It rides the context beside the RESOLVED policies because the resolved set
// is only what the plan showed AT ENFORCEMENT TIME, and the plan grows. A
// table named only inside an `IN (SELECT … )` is not in the plan when
// auth.EnforcePlanPolicies runs — the subquery is still SQL TEXT — so it was
// never policed at all, and when the optimizer decorrelated that subquery into
// a semi-join the inner scan came out with NO security projection and its
// predicate read the STORED column. `… IN (SELECT id FROM t WHERE bal > 300)`
// over a `bal` masked to 0 returned exactly the rows above that threshold, and
// the client picks the threshold (#859 round 3).
//
// A lookup makes the invariant reachable at every pass that can mint a scan:
// EVERY scan of a policed relation in the FINAL plan carries that relation's
// projection.
type PolicyLookup func(table string) (cols []ColumnPolicy, rowFilter string, err error)

type policyLookupKey struct{}

// ContextWithPolicyLookup returns ctx carrying the per-table policy lookup.
func ContextWithPolicyLookup(ctx context.Context, l PolicyLookup) context.Context {
	if l == nil {
		return ctx
	}
	return context.WithValue(ctx, policyLookupKey{}, l)
}

// PolicyLookupFromContext returns the per-table policy lookup, or nil.
func PolicyLookupFromContext(ctx context.Context) PolicyLookup {
	if ctx == nil {
		return nil
	}
	l, _ := ctx.Value(policyLookupKey{}).(PolicyLookup)
	return l
}

type policyEnforcedKey struct{}

// ContextWithPolicyEnforced marks a context whose query had ANY policy applied
// — a column projection or a row filter. A column policy is discoverable from
// ColumnPoliciesFromContext; a row-filter-only policy is not, and a dispatch
// site that ships a statement's TEXT to a worker has to refuse for both.
func ContextWithPolicyEnforced(ctx context.Context) context.Context {
	return context.WithValue(ctx, policyEnforcedKey{}, true)
}

// PolicyEnforced reports whether any policy shaped this query's plan.
func PolicyEnforced(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	on, _ := ctx.Value(policyEnforcedKey{}).(bool)
	return on
}

// ColumnPoliciesFromContext returns the column policies in force, or nil.
func ColumnPoliciesFromContext(ctx context.Context) TablePolicies {
	if ctx == nil {
		return nil
	}
	tp, _ := ctx.Value(columnPolicyKey{}).(TablePolicies)
	return tp
}

// SubstituteMaskedColumns rewrites every reference to a MASKED column of
// `relation` with that column's mask expression, and returns the result.
//
// It is the security projection applied to an expression the planner never
// sees. A DML predicate is COMPILED, not planned (ADR-0031): `DELETE FROM t
// WHERE ssn = '<stored value>'` never meets a Scan, so no projection can sit
// under it, and the comparison read the row as stored — a probe oracle for the
// masked column, and a destructive one. Substituting the mask into the
// expression is that projection, done where this statement can carry it: the
// predicate then compares '***' and matches nothing, and `SET dept = ssn`
// writes the mask.
//
// ok=false means the rewrite could not be done soundly (an aggregate output,
// a subquery, a node the substituter cannot see through) and the caller must
// refuse rather than run an expression that reads the stored row.
func SubstituteMaskedColumns(expr plansql.Node, relation string, policies []ColumnPolicy) (plansql.Node, bool) {
	if expr == nil || len(policies) == 0 {
		return expr, true
	}
	outs := make(map[string]projOutput, len(policies))
	for _, p := range policies {
		if p.Denied || p.MaskExpr == "" {
			continue
		}
		ast, err := plansql.ParseExpression(p.MaskExpr)
		if err != nil {
			return nil, false
		}
		outs[strings.ToLower(p.Column)] = projOutput{def: &plansql.ParenNode{Inner: ast}}
	}
	if len(outs) == 0 {
		return expr, true
	}
	names := map[string]bool{}
	if relation != "" {
		names[strings.ToLower(relation)] = true
	}
	return substituteColRefs(expr, projRefs{outs: outs, names: names})
}

// ApplyToNewScans injects the security projection over any policed scan that
// is NOT already under one.
//
// The optimizer MINTS SCANS. `decorrelateInSubqueries`, `decorrelateExists`
// and `decorrelateScalarSubqueries` re-parse a subquery from its SQL text and
// build a fresh Scan for its FROM item, and those scans are created AFTER
// enforcement has run — so before this pass a decorrelated
// `WHERE a.ssn IN (SELECT ssn FROM t b)` compared the outer's MASK against the
// inner's STORED column and answered 0 where both sides masked answer every
// row. No value escaped (a semi-join emits only outer columns), but the answer
// was wrong, and the same seam is the one a future rewrite could make leak.
//
// A scan already beneath a SecurityBarrier is skipped, so the pass is safe to
// run after every Optimize.
func (tp TablePolicies) ApplyToNewScans(plan *Node, columnsOf func(table string) []string) (*Node, int) {
	out, n, _ := tp.applyToNewScans(plan, columnsOf, nil)
	return out, n
}

// ApplyToNewScansWithLookup is ApplyToNewScans for a plan that may name a
// relation the resolved set never saw — the inner of a decorrelated semi-join,
// whose table lived in SQL text when enforcement ran. `lookup`, when given,
// answers for such a table; its row filter goes in BELOW the projection, the
// order ADR-0033 decision 6 fixes.
func (tp TablePolicies) ApplyToNewScansWithLookup(plan *Node, columnsOf func(table string) []string,
	lookup PolicyLookup) (*Node, int, error) {
	return tp.applyToNewScans(plan, columnsOf, lookup)
}

func (tp TablePolicies) applyToNewScans(plan *Node, columnsOf func(table string) []string,
	lookup PolicyLookup) (*Node, int, error) {
	if plan == nil || (len(tp) == 0 && lookup == nil) {
		return plan, 0, nil
	}
	unprotected := 0
	var firstErr error
	var walk func(n *Node, covered bool) *Node
	walk = func(n *Node, covered bool) *Node {
		if n == nil {
			return nil
		}
		childCovered := covered || (n.Type == NodeProject && n.SecurityBarrier)
		for i, c := range n.Children {
			n.Children[i] = walk(c, childCovered)
		}
		if covered || n.Type != NodeScan || n.TableName == "" || n.IsTableFunc {
			return n
		}
		policies := tp.For(n.TableName)
		rowFilter := ""
		if len(policies) == 0 && lookup != nil {
			cols, rf, err := lookup(n.TableName)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return n
			}
			policies, rowFilter = cols, rf
		}
		if len(policies) == 0 {
			if rowFilter != "" {
				return InjectRowFilter(n, n.TableName, rowFilter)
			}
			return n
		}
		// AFTER the optimizer, the authority is what THIS SCAN PRODUCES, not
		// the table's full schema: column pruning has already narrowed it,
		// and a projection that passed through a column the scan no longer
		// reads fails to build ("column %q does not exist in the input
		// schema"). Before the optimizer the opposite is true and the catalog
		// is the authority — that is decision 1, and it is why the two passes
		// choose differently rather than sharing one rule.
		cols := n.RequiredColumns
		if len(cols) == 0 {
			cols = n.ScanColumns
		}
		if len(cols) == 0 && columnsOf != nil {
			cols = columnsOf(n.TableName)
		}
		// Plus every MASKED column, whether or not this scan's pruned list
		// names it. A mask is computed from a literal — the scan never reads
		// the column — so publishing it costs nothing, and leaving it out is
		// what turned a predicate above the projection into a read of a
		// column the projection did not carry: `IN (SELECT id FROM t WHERE
		// ssn = '***')` evaluated `ssn` to NULL and answered no rows at all,
		// which is a wrong answer wearing the fix's clothes.
		have := make(map[string]bool, len(cols))
		for _, c := range cols {
			have[strings.ToLower(c)] = true
		}
		for _, p := range policies {
			if p.Denied || p.MaskExpr == "" {
				continue
			}
			if !have[strings.ToLower(p.Column)] {
				cols = append(cols, p.Column)
			}
		}
		out, n2 := InjectColumnPolicies(n, n.TableName, policies, cols)
		unprotected += n2
		if rowFilter != "" {
			// BELOW the projection it just got, directly above the scan:
			// InjectRowFilter walks down to the Scan, so the order is
			// barrier → filter → scan, ADR-0033 decision 6.
			out = InjectRowFilter(out, n.TableName, rowFilter)
		}
		return out
	}
	out := walk(plan, false)
	return out, unprotected, firstErr
}

// ErrPolicyOrderUnrepresentable is the refusal for a plan whose predicates
// cannot be placed above the security projection they must read through.
var ErrPolicyOrderUnrepresentable = errors.New(
	"this query is not available for this identity: a column security policy applies and a " +
		"predicate could not be placed above the security projection, where it must read the " +
		"mask rather than the stored column")

// CheckPolicyPlanOrder is the invariant every arm must satisfy, asserted on
// the FINAL logical plan:
//
//  1. every Scan of a policed relation carries that relation's security
//     projection directly above it; and
//  2. no Filter between that projection and the scan references a policed
//     column, unless it is the POLICY's own row filter.
//
// (1) is ADR-0033 decision 1 taken literally, and it is the question the
// earlier stage-level check could not ask: a stage that scans a policed table
// with NO projection at all was invisible to a check that only inspected
// stages which HAVE one — which is exactly the shape a decorrelated
// semi-join's inner side had (#859 round 3). (2) is decision 6.
//
// It refuses rather than repairs, because by this point the repair passes have
// run: reaching here means a shape this planner cannot express safely, and the
// branch's doctrine for that is 0A000, never a pin over a leak.
func CheckPolicyPlanOrder(plan *Node, policed func(table string) []ColumnPolicy) error {
	if plan == nil || policed == nil {
		return nil
	}
	var walk func(n *Node, barrier *Node) error
	walk = func(n *Node, barrier *Node) error {
		if n == nil {
			return nil
		}
		switch {
		case n.Type == NodeProject && n.SecurityBarrier:
			barrier = n
		case n.Type == NodeScan && n.TableName != "" && !n.IsTableFunc:
			cols := policed(n.TableName)
			if len(cols) == 0 {
				return nil
			}
			if barrier == nil {
				return fmt.Errorf("%w (scan of %q carries no security projection)",
					ErrPolicyOrderUnrepresentable, n.TableName)
			}
			// A predicate ATTACHED to the scan is below the projection
			// whatever the node order above it says, and a scan predicate
			// prunes row groups by the stored column's own statistics. Only
			// the policy's own row filter may read the row as stored.
			restricted := make(map[string]bool, len(cols))
			for _, c := range cols {
				restricted[strings.ToLower(c.Column)] = true
			}
			for _, group := range [][]Predicate{n.ScanPredicates, n.Predicates} {
				for _, pred := range group {
					if pred.FromPolicy {
						continue
					}
					if ScanPredicateIsRestricted(pred, restricted) {
						return fmt.Errorf("%w (predicate over a policed column attached to the scan of %q)",
							ErrPolicyOrderUnrepresentable, n.TableName)
					}
				}
			}
			for col := range n.PartitionFilter {
				if restricted[strings.ToLower(col)] {
					return fmt.Errorf("%w (partition filter over %q on the scan of %q)",
						ErrPolicyOrderUnrepresentable, col, n.TableName)
				}
			}
		case n.Type == NodeFilter && barrier != nil && !n.PolicyFilter:
			// Between a barrier and its scan, and not the policy's own.
			for _, scan := range PolicedScanTables(n) {
				restricted := map[string]bool{}
				for _, c := range policed(scan) {
					restricted[strings.ToLower(c.Column)] = true
				}
				if len(restricted) == 0 {
					continue
				}
				for _, pred := range n.Predicates {
					if col, bad := predicateNamesRestricted(pred, restricted); bad {
						return fmt.Errorf("%w (predicate over %q)",
							ErrPolicyOrderUnrepresentable, col)
					}
				}
			}
		}
		for _, c := range n.Children {
			if err := walk(c, barrier); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(plan, nil)
}

// predicateNamesRestricted reports whether a predicate reads a policed column.
// A predicate whose references cannot be read is treated as reading one: this
// guard refuses on doubt, because the alternative is a disclosure.
func predicateNamesRestricted(p Predicate, restricted map[string]bool) (string, bool) {
	ast := p.ASTExpr
	if ast == nil && p.Raw != "" {
		var err error
		if ast, err = plansql.ParseExpression(p.Raw); err != nil {
			return p.Raw, true
		}
	}
	if ast == nil {
		return "", false
	}
	refs, err := plansql.ColumnRefs(ast)
	if err != nil {
		// A node the walker cannot see through — a subquery, most often. Fall
		// back to the TEXT, with quoted literals removed first: a predicate
		// already SUBSTITUTED to `('***') = (SELECT MIN('true-ssn-01') …)`
		// reads no policed column, and refusing it would refuse a query the
		// projection has already made safe. What is left after the literals
		// go is identifiers, and a policed one among them is the doubt this
		// guard refuses on.
		return textNamesRestricted(ast.String(), restricted)
	}
	for _, ref := range refs {
		if restricted[strings.ToLower(ref.Column)] {
			return ref.Column, true
		}
	}
	return "", false
}

// textNamesRestricted looks for a policed column among an expression's
// identifier tokens, ignoring anything inside single quotes.
// TextNamesRestricted is textNamesRestricted, exported for the stage-level
// twin of this check in the physical planner.
func TextNamesRestricted(text string, restricted map[string]bool) (string, bool) {
	return textNamesRestricted(text, restricted)
}

func textNamesRestricted(text string, restricted map[string]bool) (string, bool) {
	var word strings.Builder
	inLit := false
	check := func() (string, bool) {
		w := word.String()
		word.Reset()
		if w == "" {
			return "", false
		}
		return w, restricted[strings.ToLower(w)]
	}
	for _, r := range text {
		if r == '\'' {
			if w, bad := check(); bad {
				return w, true
			}
			inLit = !inLit
			continue
		}
		if inLit {
			continue
		}
		if r == '_' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			word.WriteRune(r)
			continue
		}
		if w, bad := check(); bad {
			return w, true
		}
	}
	if w, bad := check(); bad {
		return w, true
	}
	return "", false
}

// PlanCarriesPolicyEnforcement reports whether this plan carries anything a
// policy put there: a security projection, or a row filter injected by
// row-level security.
//
// A caller that is about to hand a query to something that will RE-PLAN it
// from the SQL TEXT — the coordinator's async door dispatches a
// TaskTypePipeline task carrying `SQLText`, and the worker parses, builds and
// optimizes it again with no policy in reach — has to ask this first. The
// enforced plan does not survive that hop, and answering from the re-planned
// one hands the caller the stored values.
func PlanCarriesPolicyEnforcement(n *Node) bool {
	if n == nil {
		return false
	}
	if (n.Type == NodeProject && n.SecurityBarrier) || (n.Type == NodeFilter && n.PolicyFilter) {
		return true
	}
	for _, c := range n.Children {
		if PlanCarriesPolicyEnforcement(c) {
			return true
		}
	}
	return false
}

// For returns the policies for one table, matched case-insensitively.
func (tp TablePolicies) For(table string) []ColumnPolicy {
	if len(tp) == 0 || table == "" {
		return nil
	}
	return tp[strings.ToLower(table)]
}

// DeniedColumns maps each policed table (folded) to its denied columns
// (folded). These are the columns that, for this identity, DO NOT EXIST: a
// reference to one is 42703, not a NULL and not a mask.
func (tp TablePolicies) DeniedColumns() map[string]map[string]bool {
	if len(tp) == 0 {
		return nil
	}
	out := make(map[string]map[string]bool, len(tp))
	for table, policies := range tp {
		for _, p := range policies {
			if !p.Denied {
				continue
			}
			if out[table] == nil {
				out[table] = map[string]bool{}
			}
			out[table][strings.ToLower(p.Column)] = true
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// Apply injects the security projection for every policed table into plan.
// columnsOf resolves a table's declared column list (the catalog); it may
// return nil, in which case the scan falls back to its own annotations and,
// failing that, is reported unprotected. The second return value is the number
// of scans left UNPROTECTED — never zero-and-ignored: a caller that cannot
// protect a scan must refuse the query.
func (tp TablePolicies) Apply(plan *Node, columnsOf func(table string) []string) (*Node, int) {
	if len(tp) == 0 || plan == nil {
		return plan, 0
	}
	unprotected := 0
	for _, table := range PolicedScanTables(plan) {
		policies := tp.For(table)
		if len(policies) == 0 {
			continue
		}
		var cols []string
		if columnsOf != nil {
			cols = columnsOf(table)
		}
		var n int
		plan, n = InjectColumnPolicies(plan, table, policies, cols)
		unprotected += n
	}
	return plan, unprotected
}
