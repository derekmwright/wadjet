package coordinator

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The grouping gate, on both arms.
//
// #590 and #591 are planner defects, so both paths inherit them — but not
// identically, which is exactly why this gate cannot be written as "the two
// arms agree". Before the fix the single-process path published a synthetic
// `__having_0` column in its rows and the stage DAG did not; the single-process
// path answered `SELECT g, c_bool ... GROUP BY g` with the columns [g c_bool]
// and the DAG with [g]. Two arms, two different wrong answers, and the pair
// that DID agree — `HAVING MAX(x) IS NULL` selecting every group — agreed
// because they share the lowering that was wrong.
//
// So each arm is held to a property SQL requires rather than to the other arm:
//
//	1. the result carries exactly the columns the SELECT list asked for;
//	2. `p` / `NOT p` / `p IS NULL` partition the ungated GROUP BY (TLP's
//	   invariant, which is what found both defects);
//	3. an ungrouped reference is refused with 42803 on both paths — the DAG's
//	   stage planner must not accept what the local path refuses.
//
// Agreement between the arms is then checked as well, which is free once both
// answers are in hand.
func TestGroupingSemanticsTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: the grouping gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)
	tbl := typematrix.Table

	arms := []struct {
		name string
		run  func(sql string) (*oracle.Result, error)
	}{
		{"single", func(sql string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, sql) }},
		{"dag", func(sql string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, sql) }},
	}

	// --- 1. The answer's SHAPE (#591) -----------------------------------
	//
	// A HAVING over an aggregate the SELECT list does not carry adds a
	// synthetic output column, and a grouping key the SELECT list does not
	// name is emitted by the aggregate too. Only the projection can trim
	// either, and it used to be elided as redundant whenever no select item
	// needed COMPUTING.
	t.Run("ResultCarriesOnlyTheSelectedColumns", func(t *testing.T) {
		for _, c := range []struct {
			name string
			sql  string
			want []string
		}{
			{"HavingBareAggregate",
				`SELECT g FROM ` + tbl + ` GROUP BY g HAVING BOOL_OR(c_bool) ORDER BY g`, []string{"g"}},
			{"HavingComparison",
				`SELECT g FROM ` + tbl + ` GROUP BY g HAVING COUNT(*) > 1 ORDER BY g`, []string{"g"}},
			{"HavingNullCheck",
				`SELECT g FROM ` + tbl + ` GROUP BY g HAVING MAX(c_i64) IS NOT NULL ORDER BY g`, []string{"g"}},
			{"HavingBesideSelectedAggregate",
				`SELECT g, COUNT(*) AS c FROM ` + tbl + ` GROUP BY g HAVING MAX(c_i64) > 0 ORDER BY g`,
				[]string{"g", "c"}},
			{"GroupedKeyNotSelected",
				`SELECT g FROM ` + tbl + ` GROUP BY g, c_bool ORDER BY g`, []string{"g"}},
			{"NoKeySelected",
				`SELECT COUNT(*) AS c FROM ` + tbl + ` GROUP BY g ORDER BY c`, []string{"c"}},
		} {
			t.Run(c.name, func(t *testing.T) {
				for _, arm := range arms {
					res, err := arm.run(c.sql)
					if err != nil {
						t.Errorf("%s arm failed: %v\n  SQL: %s", arm.name, err, c.sql)
						continue
					}
					if len(res.Rows) == 0 {
						t.Errorf("%s arm returned no rows, so this entry proves nothing\n  SQL: %s",
							arm.name, c.sql)
						continue
					}
					if !sameNames(res.Columns, c.want) {
						t.Errorf("%s arm declared columns %v, the SELECT list asked for %v\n  SQL: %s",
							arm.name, res.Columns, c.want, c.sql)
					}
					for _, row := range res.Rows {
						if len(row) != len(c.want) {
							t.Errorf("%s arm: a result row carries %d columns %v, the SELECT list asked "+
								"for %d %v\n  SQL: %s", arm.name, len(row), rowKeys(row), len(c.want),
								c.want, c.sql)
							break
						}
					}
				}
			})
		}
	})

	// --- 2. TLP's partition invariant (#591) ----------------------------
	t.Run("HavingPartitionsTheUngatedGroups", func(t *testing.T) {
		base := `SELECT g FROM ` + tbl + ` GROUP BY g`
		for _, p := range []string{
			"BOOL_OR(c_bool)",
			"BOOL_AND(c_bool)",
			"MAX(c_i64) > 0",
			"COUNT(*) > 1",
			"MIN(c_i64) IS NULL",
		} {
			t.Run(p, func(t *testing.T) {
				for _, arm := range arms {
					all, err := arm.run(base + " ORDER BY g")
					if err != nil {
						t.Errorf("%s arm failed on the ungated GROUP BY: %v", arm.name, err)
						continue
					}
					var union []string
					for _, tail := range []string{
						fmt.Sprintf(" HAVING (%s) ORDER BY g", p),
						fmt.Sprintf(" HAVING NOT (%s) ORDER BY g", p),
						fmt.Sprintf(" HAVING (%s) IS NULL ORDER BY g", p),
					} {
						res, err := arm.run(base + tail)
						if err != nil {
							t.Errorf("%s arm failed: %v\n  SQL: %s", arm.name, err, base+tail)
							union = nil
							break
						}
						union = append(union, groupKeyStrings(res)...)
					}
					if union == nil {
						continue
					}
					want := groupKeyStrings(all)
					sort.Strings(union)
					sort.Strings(want)
					if !sameNames(union, want) {
						t.Errorf("%s arm: the three arms of %q do not partition the ungated GROUP BY\n"+
							"  union  (%d) %v\n  groups (%d) %v", arm.name, p, len(union), union, len(want), want)
					}
				}
			})
		}
	})

	// --- 3. The 42803 refusal, on both paths (#590) ---------------------
	t.Run("UngroupedReferenceIsRefusedOnBothPaths", func(t *testing.T) {
		for _, c := range []struct{ name, sql string }{
			{"SelectList", `SELECT g, c_bool FROM ` + tbl + ` GROUP BY g`},
			{"SelectListBesideAggregate", `SELECT COUNT(*), c_bool FROM ` + tbl + ` GROUP BY g`},
			{"HavingBareColumn", `SELECT g FROM ` + tbl + ` GROUP BY g HAVING c_bool`},
			{"HavingNegatedColumn", `SELECT g FROM ` + tbl + ` GROUP BY g HAVING NOT c_bool`},
			{"HavingColumnIsNull", `SELECT g FROM ` + tbl + ` GROUP BY g HAVING c_bool IS NULL`},
			{"HavingComparison", `SELECT g FROM ` + tbl + ` GROUP BY g HAVING c_i64 > 100`},
			{"OrderByColumn", `SELECT g FROM ` + tbl + ` GROUP BY g ORDER BY c_i64`},
		} {
			t.Run(c.name, func(t *testing.T) {
				for _, arm := range arms {
					res, err := arm.run(c.sql)
					if err == nil {
						t.Errorf("%s arm ANSWERED a statement PostgreSQL refuses with 42803: %d rows\n  SQL: %s",
							arm.name, len(res.Rows), c.sql)
						continue
					}
					if got := sqlerr.StateOf(err); got != "42803" {
						t.Errorf("%s arm: SQLSTATE = %q, want 42803 grouping_error (err: %v)\n  SQL: %s",
							arm.name, got, err, c.sql)
					}
				}
			})
		}
	})

	// --- 4. And the two arms must still agree ---------------------------
	t.Run("BothPathsAgree", func(t *testing.T) {
		for _, sql := range []string{
			`SELECT g FROM ` + tbl + ` GROUP BY g HAVING BOOL_OR(c_bool) ORDER BY g`,
			`SELECT g FROM ` + tbl + ` GROUP BY g HAVING NOT BOOL_OR(c_bool) ORDER BY g`,
			`SELECT g FROM ` + tbl + ` GROUP BY g HAVING (BOOL_OR(c_bool)) IS NULL ORDER BY g`,
			`SELECT g FROM ` + tbl + ` GROUP BY g HAVING MAX(c_i64) IS NULL ORDER BY g`,
			`SELECT g FROM ` + tbl + ` GROUP BY g HAVING MAX(c_i64) IS NOT NULL ORDER BY g`,
			`SELECT g FROM ` + tbl + ` GROUP BY g HAVING COUNT(*) > 1 AND MAX(c_i64) > 0 ORDER BY g`,
			`SELECT g FROM ` + tbl + ` GROUP BY g HAVING MAX(c_i64) BETWEEN 0 AND 100000 ORDER BY g`,
			`SELECT g, COUNT(*) AS c FROM ` + tbl + ` GROUP BY g HAVING COUNT(*) > 1 ORDER BY g`,
			`SELECT g FROM ` + tbl + ` GROUP BY g, c_bool ORDER BY g`,
		} {
			t.Run(sql, func(t *testing.T) {
				a, aerr := arms[0].run(sql)
				b, berr := arms[1].run(sql)
				if aerr != nil || berr != nil {
					t.Fatalf("single: %v\ndag: %v\n  SQL: %s", aerr, berr, sql)
				}
				if !sameNames(a.Columns, b.Columns) {
					t.Errorf("column lists differ: single %v, dag %v", a.Columns, b.Columns)
				}
				ga, gb := groupKeyStrings(a), groupKeyStrings(b)
				sort.Strings(ga)
				sort.Strings(gb)
				if !sameNames(ga, gb) {
					t.Errorf("row sets differ:\n  single (%d) %v\n  dag    (%d) %v", len(ga), ga, len(gb), gb)
				}
			})
		}
	})
}

// groupKeyStrings renders each row's first declared column as a string, which
// is enough to compare row sets whose key type varies by arm (int32 vs int64
// boxing) without deciding which boxing is right — a question other gates own.
func groupKeyStrings(res *oracle.Result) []string {
	if len(res.Columns) == 0 {
		return nil
	}
	key := res.Columns[0]
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		out = append(out, fmt.Sprint(r[key]))
	}
	return out
}

func rowKeys(row map[string]any) []string {
	out := make([]string, 0, len(row))
	for k := range row {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sameNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
