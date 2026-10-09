// SPDX-License-Identifier: MIT

package wadjet

import (
	"bufio"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// THE BINDER'S STAMP, COUNTED (ADR-0047 stage 1, RISKS M1 / M3 / M5) over the
// group-key table — every cell of coordinator.TestArcGKGroupKeySpellingEveryArm
// (testdata/arc_ci1_census_cells.tsv), on the embedded engine, which is the
// door that stamps, over the same ss_t / ss_i fixture:
//
//   - M1: no block is MIXED — bound in part, with the rest unbindable by its
//     scope (an unenumerable source, a star output, a field path, an
//     ambiguous or unresolved name). A mixed block would be matched by
//     spelling while its neighbours match by binding.
//   - M3: no match site compares a term of a bound block whose leaves lost
//     the binding (a carrier that re-parsed text), nor an unbound term with
//     bound keys, nor a bound term with keys that lost theirs.
//   - M5: every relation instance's column list is the list the built plan
//     publishes for it (logical.StarSourceColumns): the ordinal in a binding
//     names the column the plan carries at that position.
func TestArcCI1BindingCensusOverTheGroupKeyTable(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, ddl := range []string{
		"CREATE TABLE ss_t (id BIGINT, i INT, b BIGINT, f DOUBLE, n NUMERIC(10,2), s VARCHAR, o BOOLEAN, " +
			"d DATE, ts TIMESTAMP, u UUID, a ARRAY(INT))",
		"INSERT INTO ss_t VALUES " +
			"(1, 3, 30, 1.5, 2.25, 'abc', true, DATE '2024-03-04', TIMESTAMP '2024-03-04 12:00:00', " +
			"CAST('00000000-0000-4000-8000-000000000001' AS UUID), ARRAY[1,2]), " +
			"(2, -7, -70, -2.5, -3.5, 'Hello', false, DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00', " +
			"CAST('00000000-0000-4000-8000-000000000002' AS UUID), ARRAY[3]), " +
			"(3, 5, 9000000000, 0.25, 10.00, 'zz', true, DATE '9999-12-31', TIMESTAMP '9999-12-31 23:59:59.999', " +
			"CAST('00000000-0000-4000-8000-000000000003' AS UUID), ARRAY[4,5,6]), " +
			"(4, 0, 0, 0.0, 0.00, '', false, DATE '1000-01-01', TIMESTAMP '1000-01-01 00:00:00', " +
			"CAST('00000000-0000-4000-8000-000000000004' AS UUID), ARRAY[7]), " +
			"(5, 1, 1, 100.125, 0.01, 'x', true, DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59.999', " +
			"CAST('00000000-0000-4000-8000-000000000005' AS UUID), ARRAY[8]), " +
			"(6, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
		"CREATE TABLE ss_i (id BIGINT, v INT, g DOUBLE, m NUMERIC(10,2))",
		"INSERT INTO ss_i VALUES (1, 5, 0.5, 1.25), (2, 6, 0.25, NULL)",
	} {
		if _, err := db.Query(ctx, ddl); err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
	}
	f, err := os.Open("testdata/arc_ci1_census_cells.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var cells [][2]string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.SplitN(line, "\t", 3)
		if len(p) != 3 {
			t.Fatalf("malformed cell %q", line)
		}
		cells = append(cells, [2]string{p[0], p[2]})
	}
	var reps []physical.BlockBindingReport
	var viol []string
	physical.BindingProbe = func(r physical.BlockBindingReport) { reps = append(reps, r) }
	plansql.MatchProbe = func(site string, n plansql.Node, keys []string) {
		if v := ci1MatchViolation(n, keys); v != "" {
			viol = append(viol, v+" at "+site+": "+n.String())
		}
	}
	t.Cleanup(func() { physical.BindingProbe = nil; plansql.MatchProbe = nil })
	var blocks, stamped, declined, compared int
	for _, c := range cells {
		name, sql := c[0], c[1]
		reps, viol = nil, nil
		_, _ = db.Query(ctx, sql)
		for _, r := range reps {
			blocks++
			if r.Stamped {
				stamped++
			}
			if r.Declined {
				declined++
			}
			if r.Mixed {
				t.Errorf("M1 %s: a MIXED block (%s): %s", name, strings.Join(r.Unbound, ", "), r.Block)
			}
		}
		for _, v := range viol {
			t.Errorf("M3 %s: %s", name, v)
		}
		// M5. A LATERAL body's scans carry the body's alias in the logical
		// plan (setSubtreeAlias), so the plan's lookup by name answers the
		// LATERAL relation for them — a naming of the plan's, not of the
		// binding (stage 4); those cells are left out.
		if strings.Contains(strings.ToUpper(sql), "LATERAL") {
			continue
		}
		n, bad := ci1OrdinalCensus(ctx, t, db.Catalog(), sql)
		compared += n
		for _, b := range bad {
			t.Errorf("M5 %s: %s", name, b)
		}
	}
	t.Logf("%d cells; M1: %d blocks, %d stamped, %d declined (a dotted name or an unfolded FROM-less subquery), 0 mixed; M3: 0 violations; M5: %d instances compared, 0 disagreements",
		len(cells), blocks, stamped, declined, compared)
	if len(cells) < 1000 || stamped == 0 || compared == 0 {
		t.Fatalf("the census is vacuous: %d cells, %d stamped blocks, %d compared instances", len(cells), stamped, compared)
	}
}

// ci1MatchViolation is RISKS M3's rule for one comparison: the term's own
// leaves are all bound or all unbound, and the keys are on the same side.
// A planner-minted slot (`__agg_0`) is not a reference the query wrote.
// ci1MintedAggregateRef reports a reference the logical builder MINTS for an
// aggregate's output above the aggregate (plansql.ReplaceAllAggregates names
// it by the call's text, `"count(*)"`): a planner name, not a user reference,
// as `__agg_N` is. A HAVING over one beside a bound computed key is the same
// comparison at the statement's top level at 542b4f37 (`SELECT count(*) FROM
// ss_t x GROUP BY 2 * n HAVING count(*) > 0`); ADR-0047 stage 3 binds
// expression-subquery bodies, so the census now meets it in one
// (c738/outerWhereGrouped). It names no key, and the lookup answers so.
func ci1MintedAggregateRef(r *plansql.ColRef) bool {
	if r.Table != "" {
		return false
	}
	open := strings.IndexByte(r.Column, '(')
	return open > 0 && strings.HasSuffix(r.Column, ")") && plansql.IsAggregate(r.Column[:open])
}

func ci1MatchViolation(term plansql.Node, keys []string) string {
	tb, tu := false, false
	plansql.WalkColRefs(term, func(r *plansql.ColRef) {
		if r.Slot || strings.HasPrefix(r.Column, "__") || ci1MintedAggregateRef(r) {
			return
		}
		if r.Bound != nil {
			tb = true
		} else {
			tu = true
		}
	})
	kb := false
	for _, k := range keys {
		if strings.Contains(k, "\x00") {
			kb = true
		}
	}
	switch {
	case tb && tu:
		return "a term with bound and unbound leaves"
	case tu && kb:
		return "an unbound term against bound keys"
	case tb && len(keys) > 0 && !kb:
		return "a bound term against keys with no binding"
	}
	return ""
}

// ci1OrdinalCensus binds, builds and annotates sql and compares every closed
// relation instance the binder registered with the list the plan publishes
// under the same qualifier (RISKS M5). A qualifier two instances of the
// statement share with different lists, or one the plan's walk does not reach
// (an expression subquery's body is planned on its own), is not compared.
func ci1OrdinalCensus(ctx context.Context, t *testing.T, cat *catalog.Catalog, sql string) (int, []string) {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		return 0, nil
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil || info == nil {
		return 0, nil
	}
	var reps []physical.BlockBindingReport
	saved := physical.BindingProbe
	physical.BindingProbe = func(r physical.BlockBindingReport) { reps = append(reps, r) }
	defer func() { physical.BindingProbe = saved }()
	if err := physical.BindColumnsUnderPolicy(ctx, cat, info, nil, nil); err != nil {
		return 0, nil
	}
	plan, err := logical.BuildFromSelect(info)
	if err != nil {
		return 0, nil
	}
	physical.NewPlanner(cat).AnnotateScanColumns(ctx, plan)
	roots := []*logical.Node{plan}
	var walk func(n *logical.Node)
	walk = func(n *logical.Node) {
		if n == nil {
			return
		}
		if n.DerivedAlias != "" || n.CTEName != "" {
			roots = append(roots, n.Children...)
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(plan)
	lists := map[string]map[string]bool{}
	for _, r := range reps {
		for _, in := range r.Instances {
			q := strings.ToLower(in.Qual)
			if lists[q] == nil {
				lists[q] = map[string]bool{}
			}
			lists[q][strings.ToLower(strings.Join(in.Cols, ","))] = true
		}
	}
	n := 0
	var bad []string
	for _, r := range reps {
		for _, in := range r.Instances {
			if in.Open || len(lists[strings.ToLower(in.Qual)]) > 1 {
				continue
			}
			var sc []logical.StarColumn
			for _, root := range roots {
				if sc = logical.StarSourceColumns(root, in.Qual); sc != nil {
					break
				}
			}
			if sc == nil {
				continue
			}
			got := make([]string, len(sc))
			for i, c := range sc {
				got[i] = c.Resolve
			}
			n++
			if !strings.EqualFold(strings.Join(got, ","), strings.Join(in.Cols, ",")) {
				bad = append(bad, in.Qual+": binder "+strings.Join(in.Cols, ",")+" plan "+strings.Join(got, ","))
			}
		}
	}
	return n, bad
}
