package physical

import (
	"errors"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// The BREADTH gate for the Filter/Project placement class.
//
// filter_carrier_test.go's corpus holds the shapes a defect was found in;
// this one holds the CROSS of every producer class with every consumer shape,
// which is where the next one will be. A 155-shape sweep of exactly this form
// found twelve live flags outside the corpus after the schema and
// reachability checks were promoted to run on every plan — three of them
// silent wrong answers.
//
// The assertion is not "every shape plans". It is that every shape ends in an
// ANSWER: either the DAG plans it, or PlanDistributed refuses it with
// ErrUnreachableGatherOutput and the coordinator routes it to the local
// engine. What must never happen is a plan that dispatches and answers
// WITHOUT the SELECT list, the predicate, or the ordering the query wrote —
// and every other refusal is a bug in this pass, so only that one is allowed.

// sweepProducers each yield a derived table `s` with columns (k, v).
func sweepProducers() map[string]string {
	return map[string]string{
		"scan":      `SELECT n_nationkey AS k, n_regionkey AS v FROM nation`,
		"sortlimit": `SELECT n_nationkey AS k, n_regionkey AS v FROM nation ORDER BY n_nationkey LIMIT 5`,
		"sort":      `SELECT n_nationkey AS k, n_regionkey AS v FROM nation ORDER BY n_nationkey`,
		"limit":     `SELECT n_nationkey AS k, n_regionkey AS v FROM nation LIMIT 5`,
		"offset":    `SELECT n_nationkey AS k, n_regionkey AS v FROM nation ORDER BY n_nationkey LIMIT 5 OFFSET 2`,
		"window":    `SELECT n_nationkey AS k, ROW_NUMBER() OVER (ORDER BY n_nationkey) AS v FROM nation`,
		"winpart":   `SELECT n_nationkey AS k, ROW_NUMBER() OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) AS v FROM nation`,
		"sortwin":   `SELECT n_nationkey AS k, ROW_NUMBER() OVER (ORDER BY n_nationkey) AS v FROM nation ORDER BY n_nationkey`,
		"agg":       `SELECT n_regionkey + 1 AS k, COUNT(*) AS v FROM nation GROUP BY n_regionkey + 1`,
		"agghaving": `SELECT n_regionkey + 1 AS k, COUNT(*) AS v FROM nation GROUP BY n_regionkey + 1 HAVING COUNT(*) > 1`,
		"join":      `SELECT a.n_nationkey AS k, b.r_regionkey AS v FROM nation a JOIN region b ON a.n_regionkey = b.r_regionkey`,
		"distinct":  `SELECT DISTINCT n_nationkey AS k, n_regionkey AS v FROM nation`,
		"union":     `SELECT n_nationkey AS k, n_regionkey AS v FROM nation UNION ALL SELECT r_regionkey, r_regionkey FROM region`,
	}
}

// sweepConsumers are the shapes that read one. %[1]s is the producer body.
func sweepConsumers() map[string]string {
	return map[string]string{
		"proj_then_filter":     `SELECT k * 2 AS d FROM (%[1]s) s WHERE s.v > 0`,
		"proj_then_filter_ord": `SELECT k * 2 AS d FROM (%[1]s) s WHERE s.v > 0 ORDER BY d`,
		"filter_only":          `SELECT k FROM (%[1]s) s WHERE s.v > 0`,
		"proj_only":            `SELECT k * 2 AS d FROM (%[1]s) s`,
		"proj_expr_only":       `SELECT UPPER(CAST(k AS VARCHAR)) AS d FROM (%[1]s) s`,
		"narrow_then_filter":   `SELECT s.k FROM (%[1]s) s WHERE s.v > 0 ORDER BY s.k`,
		"count_over":           `SELECT COUNT(*) AS n FROM (%[1]s) s WHERE s.v > 0`,
		"agg_over":             `SELECT SUM(k) AS n FROM (%[1]s) s WHERE s.v > 0`,
		"win_over":             `SELECT k, SUM(v) OVER () AS w FROM (%[1]s) s`,
		"win_over_filter":      `SELECT k, w FROM (SELECT k, SUM(v) OVER () AS w FROM (%[1]s) s) t WHERE t.k >= 0`,
		"join_over":            `SELECT COUNT(*) AS n FROM (%[1]s) s JOIN region r ON s.v = r.r_regionkey WHERE s.v > 0`,
		// An ORDER BY on an alias INSIDE a derived table whose consumer is
		// an aggregate: the outer COUNT(*) needs no columns, so the
		// projection that computes the key is pruned and the sort keys on a
		// name nothing emits.
		"agg_over_ordered_proj": `SELECT COUNT(*) AS n FROM (SELECT k * 2 AS d FROM (%[1]s) s ORDER BY d) x`,
	}
}

func TestStageShapePlacementSweep(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)
	plan := func(sql string) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("PANIC: %v", r)
			}
		}()
		parsed, perr := plansql.Parse(sql)
		if perr != nil {
			return fmt.Errorf("parse: %w", perr)
		}
		info, ierr := plansql.ExtractSelect(parsed)
		if ierr != nil {
			return fmt.Errorf("extract: %w", ierr)
		}
		node, lerr := logical.BuildFromSelect(info)
		if lerr != nil {
			return fmt.Errorf("logical: %w", lerr)
		}
		annotate := func(n *logical.Node) { NewPlanner(cat).AnnotateScanColumns(ctx, n) }
		annotate(node)
		node = logical.Optimize(node, annotate)
		p := NewPlanner(cat)
		p.WorkerCount = 3
		stages, serr := p.PlanDistributed(ctx, node)
		if serr != nil {
			return serr
		}
		return ValidateNativeDAGShape(stages)
	}

	check := func(t *testing.T, name, sql string) {
		t.Helper()
		err := plan(sql)
		switch {
		case err == nil:
		case errors.Is(err, ErrUnreachableGatherOutput):
			// Refused at PLAN time, which the coordinator answers locally.
			// Correct, and deliberately not silent.
			t.Logf("%s: routed local — %v", name, err)
		default:
			t.Errorf("%s: %v\n  SQL: %s", name, err, sql)
		}
	}

	for pname, body := range sweepProducers() {
		for cname, tmpl := range sweepConsumers() {
			name := pname + "/" + cname
			sql := fmt.Sprintf(tmpl, body)
			t.Run(name, func(t *testing.T) { check(t, name, sql) })
		}
	}
	// The CTE shapes, which have no derived-table spelling: a body
	// referenced twice is where a consumer-scoped filter lands on a stage
	// every reference reads.
	for name, sql := range map[string]string{
		"cte2/where_on_the_first_ref": `WITH c AS (SELECT n_nationkey AS k, n_regionkey AS v FROM nation ` +
			`WHERE n_nationkey < 100) SELECT COUNT(*) AS n FROM (SELECT k FROM c WHERE v > 2 ` +
			`UNION ALL SELECT k FROM c) u`,
		"cte2/proj_then_filter": `WITH c AS (SELECT n_nationkey AS k, n_regionkey AS v FROM nation) ` +
			`SELECT k * 2 AS d FROM c WHERE v > 0`,
		"cte_agg/proj_then_filter_ord": `WITH c AS (SELECT n_regionkey + 1 AS k, COUNT(*) AS v FROM nation ` +
			`GROUP BY n_regionkey + 1) SELECT k * 10 AS d FROM c WHERE k > 1 ORDER BY d`,
		"win/wrapped_one_level_down": `SELECT x FROM (SELECT SUM(n_nationkey) OVER () + 1 AS x FROM nation) s`,
		"win/derived_alias_forwarded": `SELECT s FROM (SELECT n_name AS s, ` +
			`ROW_NUMBER() OVER (ORDER BY n_nationkey) AS rn FROM nation) t`,
		// A CTE body that AGGREGATES on a computed key, referenced twice,
		// with nothing consumer-scoped anywhere: the projection on the
		// shared terminal is the BODY's, and refusing it refused the query.
		"cte_agg2/union_all": `WITH a AS (SELECT n_regionkey + 1 AS gk, COUNT(*) AS n ` +
			`FROM nation GROUP BY n_regionkey + 1) SELECT gk FROM a UNION ALL SELECT gk FROM a`,
		"cte_agg2/filter_on_each_ref": `WITH a AS (SELECT n_regionkey + 1 AS gk, COUNT(*) AS n ` +
			`FROM nation GROUP BY n_regionkey + 1) SELECT COUNT(*) AS n FROM ` +
			`(SELECT gk FROM a WHERE gk > 3 UNION ALL SELECT gk FROM a WHERE gk < 3) u`,
		"cte_agg2/self_join": `WITH a AS (SELECT n_regionkey + 1 AS gk, COUNT(*) AS n ` +
			`FROM nation GROUP BY n_regionkey + 1) SELECT COUNT(*) AS n FROM a ` +
			`JOIN a b ON a.gk = b.gk`,
		// A HAVING written on the group-key EXPRESSION, which the aggregate
		// emits as a column NAME.
		"cte_agg/having_on_the_key": `WITH a AS (SELECT n_regionkey + 1 AS gk, COUNT(*) AS n ` +
			`FROM nation GROUP BY n_regionkey + 1 HAVING n_regionkey + 1 > 2) ` +
			`SELECT gk, n FROM a WHERE gk > 3 ORDER BY gk`,
		// A derived-table alias forwarded past a window whose value lives in
		// a synthetic column, with a RENAME beside it.
		"win/alias_beside_a_rename": `SELECT k, s FROM (SELECT n_nationkey AS k, ` +
			`SUM(n_regionkey) OVER () + 1 AS s FROM nation) x WHERE k > 0 ORDER BY k`,
		// A computed group key read straight out, with no CTE and no
		// derived table: the aggregate emits it under its expression TEXT.
		"agg/computed_key_selected": `SELECT n_regionkey + 1 AS gk FROM nation ` +
			`GROUP BY n_regionkey + 1 ORDER BY gk`,
	} {
		name, sql := name, sql
		t.Run(name, func(t *testing.T) { check(t, name, sql) })
	}
}
