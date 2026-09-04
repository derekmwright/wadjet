package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// An INTERSECT / EXCEPT keys its dedup on the set operation's DECLARED RESULT
// schema, never on an arm's own encoding (#680).
//
// The operation lowers to a tagged concatenation under a hash aggregate that
// GROUPs BY the full result row, so the key an arm contributes has to be the
// RESULT's, whatever that arm computed it from. An arm that is itself a GROUP
// BY reaches the union having already keyed its own rows, and keying the
// result at that arm's storage type panicked the DAG in
// `HashAggregate.processRow → appendColumnValue` with `index out of range [0]`
// where the single-process path answered PostgreSQL's four rows.
//
// The headline shape ANSWERS on all three arms as of v0.18.30 — the panic is
// gone — so this gate is the regression proof rather than the fix, and it is
// written the way the claim is stated: adding a GROUP BY to one arm must not
// change the answer. Each type of the matrix is asserted against its own
// UNGROUPED spelling, which is the reference the claim compares to and needs
// no second engine to compute; the two numwidth cells carry PostgreSQL 17.11's
// own rows for the query the issue was filed with.
//
// Per TYPE, because the key encoding is per type: a defect that keys DECIMAL
// at the arm's scale, or a network address as its storage bytes, is invisible
// over a fixture with one groupable column.
func TestAnIntersectKeysOnItsResultSchemaNotOnAnArmsOwn(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	arms := []struct {
		name string
		run  func(string) (*oracle.Result, error)
		co   *Coordinator
	}{
		{"single", func(q string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, q) }, nil},
		{"dag", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, q) }, coord},
		{"dag-shuffled", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordB, q) }, coordB},
	}

	// --- the issue's own query, against PostgreSQL 17.11's rows ----------
	//
	// numwidth's w_i64 holds ten integers and w_d2 a DECIMAL(9,2); the two
	// meet at numeric, so the INTERSECT is {-20, 0, 2, NULL} and the EXCEPT
	// the six integers no decimal row carries. Measured live.
	for _, tc := range []struct {
		name, sql string
		want      []string
	}{
		{"intersect_with_a_grouped_arm",
			`SELECT v FROM (SELECT w_i64 AS v FROM numwidth GROUP BY w_i64 ` +
				`INTERSECT SELECT w_d2 FROM numwidth) q`,
			[]string{"-20", "0", "2", "NULL"}},
		{"except_with_a_grouped_arm",
			`SELECT v FROM (SELECT w_i64 AS v FROM numwidth GROUP BY w_i64 ` +
				`EXCEPT SELECT w_d2 FROM numwidth) q`,
			[]string{"12", "13", "16777217", "2147483648", "3", "9007199254740993"}},
		// The grouped arm on the RIGHT, so a key taken from "the first arm"
		// rather than from the result is visible from the other side too.
		{"intersect_with_the_grouped_arm_second",
			`SELECT v FROM (SELECT w_d2 AS v FROM numwidth ` +
				`INTERSECT SELECT w_i64 FROM numwidth GROUP BY w_i64) q`,
			[]string{"-20", "0", "2", "NULL"}},
		// Both arms grouped.
		{"intersect_with_both_arms_grouped",
			`SELECT v FROM (SELECT w_i64 AS v FROM numwidth GROUP BY w_i64 ` +
				`INTERSECT SELECT w_d2 FROM numwidth GROUP BY w_d2) q`,
			[]string{"-20", "0", "2", "NULL"}},
	} {
		t.Run("#680/"+tc.name, func(t *testing.T) {
			for _, arm := range arms {
				var before a2Routes
				if arm.co != nil {
					before = a2ReadRoutes(arm.co)
				}
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm: %v\n  SQL: %s", arm.name, err, tc.sql)
				}
				if arm.co != nil {
					a2CheckRoutes(t, arm.name, before, a2ReadRoutes(arm.co), a2Routes{}, tc.sql)
				}
				got := setOpCanonRows(res)
				if strings.Join(got, " ") != strings.Join(tc.want, " ") {
					t.Errorf("%s arm rows\n  got  %v\n  want %v (PostgreSQL 17)\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}

	// --- per TYPE: a GROUP BY on one arm changes nothing -----------------
	for _, c := range typematrix.Columns() {
		if !c.Flat || !groupableSetOpKeyType(c.Type) {
			continue
		}
		for _, op := range []string{"INTERSECT", "EXCEPT"} {
			t.Run(fmt.Sprintf("#680/%s/%s", op, c.Name), func(t *testing.T) {
				grouped := fmt.Sprintf(
					`SELECT COUNT(*) AS n FROM (SELECT %s AS v FROM typemx GROUP BY %s `+
						`%s SELECT %s FROM typemx) q`, c.Name, c.Name, op, c.Name)
				plain := fmt.Sprintf(
					`SELECT COUNT(*) AS n FROM (SELECT %s AS v FROM typemx `+
						`%s SELECT %s FROM typemx) q`, c.Name, op, c.Name)
				for _, arm := range arms {
					ref, err := arm.run(plain)
					if err != nil {
						t.Fatalf("%s arm refused the ungrouped spelling: %v\n  SQL: %s",
							arm.name, err, plain)
					}
					got, err := arm.run(grouped)
					if err != nil {
						t.Fatalf("%s arm: %v\n  SQL: %s", arm.name, err, grouped)
					}
					want := setOpCanonRows(ref)
					if strings.Join(setOpCanonRows(got), " ") != strings.Join(want, " ") {
						t.Errorf("%s arm: a GROUP BY on one arm changed the answer\n"+
							"  grouped   %v\n  ungrouped %v\n  SQL: %s",
							arm.name, setOpCanonRows(got), want, grouped)
					}
				}
			})
		}
	}
}

// groupableSetOpKeyType names the flat types a GROUP BY takes here. The four
// container types are not flat and are excluded by the caller; VECTOR is flat
// in the schema but is not a group key in this engine, and listing it here
// rather than skipping silently is what makes its absence a statement.
func groupableSetOpKeyType(t parquet.TypeID) bool {
	return t != parquet.TypeVector
}
