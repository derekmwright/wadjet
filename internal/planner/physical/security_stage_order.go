package physical

import (
	"context"
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// ErrSecurityOrderUnrepresentable is the DAG's refusal for a shape whose
// predicates it cannot place above the security projection.
var ErrSecurityOrderUnrepresentable = sqlerr.New("0A000",
	"this query is not available for this identity on the distributed path: a column "+
		"security policy applies and a predicate could not be placed above the security "+
		"projection, where it must read the mask rather than the stored column")

// CheckSecurityFilterOrder is the invariant a policed stage DAG must satisfy:
// **no predicate that runs BELOW a security projection may name a column that
// projection masks or denies.**
//
// The slot below the projection exists for the POLICY's own row filter, which
// is supposed to read the row as stored (ADR-0033 decision 6). Anything else
// there is reading the value the policy hides, and the row set it produces is
// arithmetic on that value: `WHERE bal > (SELECT MIN(bal) FROM t)` over a `bal`
// masked to 0 returned exactly the rows whose stored value was positive, per
// row, to a client that may not read the column (#859 round 2).
//
// walkStages routes user predicates to PostSecurityFilterExprs, so in the
// normal case this finds nothing. It is here because a later rewriting pass
// that copies FilterExprs without its companion — fuse_stage_chains,
// native_dag_rewrite, the join-carried-columns passes — would silently put one
// back, and the failure mode is a disclosure rather than a wrong count. When it
// fires the query REFUSES, the way the async door does: a policy that cannot be
// delivered never degrades to a grant.
func CheckSecurityFilterOrder(ctx context.Context, stages []Stage) error {
	pol := logical.ColumnPoliciesFromContext(ctx)
	if len(pol) == 0 {
		return nil
	}
	restricted := map[string]bool{}
	for _, policies := range pol {
		for _, p := range policies {
			restricted[strings.ToLower(p.Column)] = true
		}
	}
	if len(restricted) == 0 {
		return nil
	}
	// The STRONGER question review P2(r3) asks — "every stage that scans a
	// policed relation carries the projection" — is asked, and asked where it
	// is decidable: logical.CheckPolicyPlanOrder runs over the FINAL LOGICAL
	// PLAN, which the DAG and the single-process pipeline both consume, so a
	// scan of a policed relation with no projection refuses on every arm
	// before stage generation begins. It cannot be asked HERE without false
	// refusals: a stage may legitimately scan a policed table and carry no
	// projection when it publishes none of the policed columns — the
	// deferred-scalar producer for `(SELECT MIN('literal') FROM t)` reads
	// nothing from the row and its passthroughs are pruned away. This
	// stage-level check keeps the narrower question, as the second line
	// behind the plan-level one.
	for i := range stages {
		s := &stages[i]
		if len(s.SecurityProjectExprs) == 0 {
			continue
		}
		// The POLICY's own row filter belongs below the projection and reads
		// the stored column on purpose — PostgreSQL's RLS ordering, ADR-0033
		// decision 6. It is the one predicate exempt from this rule.
		policy := make(map[string]bool, len(s.PolicyFilterExprs))
		for _, p := range s.PolicyFilterExprs {
			policy[p] = true
		}
		for _, pred := range s.FilterExprs {
			if policy[pred] {
				continue
			}
			if col, bad := predicateReadsRestricted(pred, restricted); bad {
				return fmt.Errorf("%w (stage %s, predicate over %q)",
					ErrSecurityOrderUnrepresentable, s.ID, col)
			}
		}
	}
	return nil
}

// subtreeHasSecurityBarrier reports whether a security projection stands
// anywhere below this node. It is the guard on scan-level filter pushdown: a
// predicate above a barrier must not be evaluated against the file.
func subtreeHasSecurityBarrier(n *logical.Node) bool {
	if n == nil {
		return false
	}
	if n.Type == logical.NodeProject && n.SecurityBarrier {
		return true
	}
	for _, c := range n.Children {
		if subtreeHasSecurityBarrier(c) {
			return true
		}
	}
	return false
}

// predicateReadsRestricted reports whether a predicate names a policed column.
// A predicate the parser or the ref walker cannot read is treated as reading
// one: this guard refuses on doubt, because the alternative is a disclosure.
func predicateReadsRestricted(pred string, restricted map[string]bool) (string, bool) {
	if strings.TrimSpace(pred) == "" {
		return "", false
	}
	ast, err := plansql.ParseExpression(pred)
	if err != nil {
		return pred, true
	}
	refs, err := plansql.ColumnRefs(ast)
	if err != nil {
		// See logical.CheckPolicyPlanOrder: a node the walker cannot see
		// through falls back to the identifier tokens, quoted literals
		// removed, so an already-substituted predicate is not refused for
		// carrying a subquery.
		return logical.TextNamesRestricted(ast.String(), restricted)
	}
	for _, ref := range refs {
		if restricted[strings.ToLower(ref.Column)] {
			return ref.Column, true
		}
	}
	return "", false
}
