package tpch

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/coordinator"
)

// The gate for DUPLICATE output column names, on both coordinator paths.
//
// A result may legally carry two output columns of one NAME — PostgreSQL
// answers `SELECT upper(a), upper(b)` with two columns called `upper`, and
// #513 made this engine agree. Nothing that existed could see a value swap
// between them:
//
//   - the two-path suite realigns arm B onto arm A BY NAME, so both arms
//     "agree" whatever either answers;
//   - the PostgreSQL wire oracle compares cells positionally, but only over
//     the single-process path;
//   - the pgwire transport tests feed a synthetic SliceStream, so they never
//     reach the gather's rename.
//
// Two things make this gate able to see one. It reads the coordinator's batch
// stream POSITIONALLY, which is the only way to address a duplicate-named
// column. And it compares against a REFERENCE spelling of the same query with
// DISTINCT aliases (`AS u1, AS u2`), which is the same question asked in a way
// no name-keyed layer can get wrong — so a swap, a collapse, a dropped column
// and a wrong sort order all show, where "every row has cell0 == cell1" would
// only have caught the collapse.
//
// Rows are compared both as an ORDERED sequence (the ORDER BY makes the
// sequence part of the answer) and as a MULTISET, so a reordering and a
// substitution are distinguishable in the failure.
func TestDuplicateOutputNamesOnBothCoordinatorPaths(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	fast, dag := setupTwoPathCluster(t, ctx)

	const (
		armLocal = "A (single-process)"
		armDAG   = "B (stage DAG)"
	)

	for _, tc := range []dupShape{
		// The producing fragment does NOT materialize the select list here,
		// so the gather sees distinct source names.
		{name: "two calls of one function",
			dup: `SELECT UPPER(n_name), UPPER(n_comment) FROM nation ORDER BY n_nationkey`,
			ref: `SELECT UPPER(n_name) AS u1, UPPER(n_comment) AS u2 FROM nation ORDER BY n_nationkey`},
		// An ORDER BY term the select list does not carry forces a projection
		// below the sort, which materializes BOTH aliases: the gather is then
		// handed two columns named `upper` and two renames spelled `upper`.
		{name: "under a hidden sort key",
			dup: `SELECT UPPER(n_name), UPPER(n_comment) FROM nation ORDER BY n_regionkey, n_name`,
			ref: `SELECT UPPER(n_name) AS u1, UPPER(n_comment) AS u2 FROM nation ORDER BY n_regionkey, n_name`},
		{name: "a computed column and an alias",
			dup: `SELECT UPPER(n_name), n_comment AS upper FROM nation ORDER BY n_regionkey, n_name`,
			ref: `SELECT UPPER(n_name) AS u1, n_comment AS u2 FROM nation ORDER BY n_regionkey, n_name`},
		{name: "an explicit duplicate alias",
			dup: `SELECT UPPER(n_name) AS u, UPPER(n_comment) AS u FROM nation ORDER BY n_regionkey, n_name`,
			ref: `SELECT UPPER(n_name) AS u1, UPPER(n_comment) AS u2 FROM nation ORDER BY n_regionkey, n_name`},
		// Two PLAIN columns under one alias — the same defect with no computed
		// projection anywhere, which is how it was reachable before #513 made
		// duplicates ordinary.
		{name: "two plain columns under one alias",
			dup: `SELECT n_name AS u, n_comment AS u FROM nation ORDER BY n_regionkey, n_name`,
			ref: `SELECT n_name AS u1, n_comment AS u2 FROM nation ORDER BY n_regionkey, n_name`},
		// Reversed against the table's own column order, so a resolver that
		// happens to walk the schema cannot pass by accident.
		{name: "reversed plain columns",
			dup: `SELECT n_comment AS u, n_name AS u FROM nation ORDER BY n_nationkey`,
			ref: `SELECT n_comment AS u1, n_name AS u2 FROM nation ORDER BY n_nationkey`},
		// The duplicates are not adjacent: an ordinal rule has to count only
		// the columns carrying the name, not positions in the row.
		{name: "duplicates separated by another column",
			dup: `SELECT n_name AS u, n_regionkey AS keep, n_comment AS u FROM nation ORDER BY n_nationkey`,
			ref: `SELECT n_name AS u1, n_regionkey AS keep, n_comment AS u2 FROM nation ORDER BY n_nationkey`},
		{name: "three duplicates",
			dup: `SELECT n_name AS u, n_comment AS u, n_name AS u FROM nation ORDER BY n_nationkey`,
			ref: `SELECT n_name AS u1, n_comment AS u2, n_name AS u3 FROM nation ORDER BY n_nationkey`},
		{name: "over a join",
			dup: `SELECT UPPER(n_name), UPPER(n_comment) FROM nation n
			      JOIN region r ON n.n_regionkey = r.r_regionkey ORDER BY r.r_name, n.n_name`,
			ref: `SELECT UPPER(n_name) AS u1, UPPER(n_comment) AS u2 FROM nation n
			      JOIN region r ON n.n_regionkey = r.r_regionkey ORDER BY r.r_name, n.n_name`},
		// LIMIT on top of the hidden-sort-key trim.
		{name: "limit under a sort trim",
			dup: `SELECT UPPER(n_name), UPPER(n_comment) FROM nation ORDER BY n_comment LIMIT 5`,
			ref: `SELECT UPPER(n_name) AS u1, UPPER(n_comment) AS u2 FROM nation ORDER BY n_comment LIMIT 5`},
		// Two outputs reading ONE source column: the answer really is that
		// column twice, so a resolver that refuses duplicates outright drops
		// a column here.
		{name: "distinct over duplicates",
			dup: `SELECT DISTINCT n_regionkey AS u, n_regionkey AS u FROM nation ORDER BY 1`,
			ref: `SELECT DISTINCT n_regionkey AS u1, n_regionkey AS u2 FROM nation ORDER BY 1`},

		// --- pinned residuals, each tracked and each still diverging -------
		{name: "window beside duplicates",
			dup: `SELECT UPPER(n_name), UPPER(n_comment), ROW_NUMBER() OVER (ORDER BY n_nationkey) AS rn
			      FROM nation ORDER BY n_regionkey, n_name`,
			ref: `SELECT UPPER(n_name) AS u1, UPPER(n_comment) AS u2, ROW_NUMBER() OVER (ORDER BY n_nationkey) AS rn
			      FROM nation ORDER BY n_regionkey, n_name`,
			pins: map[string]string{armDAG: "#558: a hidden ORDER BY term above a WINDOW has nowhere to be " +
				"materialized on the DAG — materializeSortKey needs a fragment that carries an OpProject " +
				"and a window stage is not one. Fails loud, no duplicates required"}},
		{name: "union all over duplicates",
			dup:       `SELECT n_name AS u, n_comment AS u FROM nation UNION ALL SELECT r_name, r_comment FROM region`,
			ref:       `SELECT n_name AS u1, n_comment AS u2 FROM nation UNION ALL SELECT r_name, r_comment FROM region`,
			unordered: true,
			pins: map[string]string{armLocal: "#556: the single-process set-operation lowering resolves the " +
				"arms' columns by NAME, so both outputs carry the second source column — all 30 rows. " +
				"The DAG answers it correctly, which is what isolates the collision as the cause"}},
		{name: "positional ORDER BY over duplicates",
			dup: `SELECT n_name AS u, n_regionkey AS u FROM nation ORDER BY 2, 1`,
			ref: `SELECT n_name AS u1, n_regionkey AS u2 FROM nation ORDER BY 2, 1`,
			pins: map[string]string{
				armLocal: "#557: resolvePositionalRefs rewrites ORDER BY N to the NAME of the N-th select " +
					"item, and a name is not an address once two items share it — the sort binds the first " +
					"column carrying it. VALUES are paired correctly; only the ORDER is wrong",
				armDAG: "#557, same cause on the other path",
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range []struct {
				label string
				c     *coordinator.Coordinator
			}{{armLocal, fast}, {armDAG, dag}} {
				pin, pinned := tc.pins[arm.label]
				detail := tc.compareOnArm(ctx, arm.c)
				switch {
				case detail == "" && pinned:
					t.Errorf("arm %s now agrees with the distinct-alias reference, so this known "+
						"divergence is FIXED:\n  %s\nDelete the pin on %q in "+
						"TestDuplicateOutputNamesOnBothCoordinatorPaths.", arm.label, pin, tc.name)
				case detail != "" && pinned:
					t.Logf("known divergence, NOT gated on arm %s: %s\n  %s", arm.label, detail, pin)
				case detail != "":
					t.Errorf("arm %s: %s\n  duplicate-name spelling: %s\n  reference spelling:      %s",
						arm.label, detail, oneLine(tc.dup), oneLine(tc.ref))
				}
			}
		})
	}
}

// dupShape is one query in two spellings: dup names two output columns the
// same, ref gives them distinct aliases. Everything else about them is
// identical, so the reference IS the answer.
type dupShape struct {
	name string
	dup  string
	ref  string
	// unordered: the statement has no total order of its own, so only the
	// row MULTISET is part of the answer.
	unordered bool
	// pins maps an arm label to the issue tracking a divergence that is not
	// gated there. The comparison still runs, and an arm that starts agreeing
	// FAILS — deleting the pin is the fix's proof.
	pins map[string]string
}

// compareOnArm runs both spellings on one coordinator and returns "" when they
// agree, or a description of the first way they do not.
func (tc dupShape) compareOnArm(ctx context.Context, c *coordinator.Coordinator) string {
	dupRows, dupCols, dupErr := streamRowValues(ctx, c, tc.dup)
	refRows, refCols, refErr := streamRowValues(ctx, c, tc.ref)
	if refErr != nil {
		return fmt.Sprintf("the REFERENCE spelling failed, so it cannot be ground truth for "+
			"anything: %v", refErr)
	}
	if dupErr != nil {
		return fmt.Sprintf("the duplicate-name spelling failed where the reference answered "+
			"%d rows: %v", len(refRows), dupErr)
	}
	if len(dupCols) != len(refCols) {
		return fmt.Sprintf("returned %d columns %v, the reference %d %v",
			len(dupCols), dupCols, len(refCols), refCols)
	}
	if len(dupRows) != len(refRows) {
		return fmt.Sprintf("returned %d rows, the reference %d", len(dupRows), len(refRows))
	}
	// Multiset first: it says WHAT is wrong (a value substituted or dropped)
	// independently of WHERE, which an ordered compare cannot separate.
	if a, b := rowMultiset(dupRows), rowMultiset(refRows); !equalStrings(a, b) {
		return fmt.Sprintf("the row MULTISET differs from the reference — a value is substituted "+
			"or dropped, not merely reordered\n    first differing row (sorted): %s",
			firstDiff(a, b))
	}
	if tc.unordered {
		return ""
	}
	for i := range dupRows {
		if got, want := renderRow(dupRows[i]), renderRow(refRows[i]); got != want {
			return fmt.Sprintf("row %d is %s, the reference %s — same multiset, so the ORDER "+
				"differs", i, got, want)
		}
	}
	return ""
}

func renderRow(cells []any) string {
	parts := make([]string, len(cells))
	for i, c := range cells {
		parts[i] = fmt.Sprintf("%v", c)
	}
	return "[" + strings.Join(parts, " | ") + "]"
}

func rowMultiset(rows [][]any) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = renderRow(r)
	}
	sort.Strings(out)
	return out
}

func equalStrings(a, b []string) bool {
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

func firstDiff(a, b []string) string {
	for i := range a {
		if i >= len(b) {
			return a[i] + " (reference has no such row)"
		}
		if a[i] != b[i] {
			return a[i] + ", reference " + b[i]
		}
	}
	if len(b) > len(a) {
		return "(missing) , reference " + b[len(a)]
	}
	return "(none)"
}

func oneLine(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}

// streamRowValues reads a coordinator result POSITIONALLY, off the batch
// stream. Every other reader in this package boxes rows into name-keyed maps,
// which cannot represent two columns of one name — the blind spot this gate
// exists to close.
func streamRowValues(ctx context.Context, c *coordinator.Coordinator, sql string) ([][]any, []string, error) {
	res, err := c.ExecuteSQL(ctx, sql)
	if err != nil {
		return nil, nil, err
	}
	defer res.Close()
	if res.Error != "" {
		return nil, nil, fmt.Errorf("%s", res.Error)
	}
	var cols []string
	for _, c := range res.OutputSchema() {
		cols = append(cols, c.Name)
	}
	var out [][]any
	stream := res.Stream()
	defer stream.Close()
	for {
		b, err := stream.Next(ctx)
		if err != nil {
			return nil, cols, err
		}
		if b == nil {
			break
		}
		if len(cols) == 0 {
			for _, c := range b.Schema {
				cols = append(cols, c.Name)
			}
		}
		out = append(out, b.ToRowValues()...)
	}
	return out, cols, nil
}

// The gate for two DIFFERENT AGGREGATES sharing one output alias (#575).
//
// Sibling of #556/#557: a duplicate output column name breaks a NAME-based
// resolver, but here the colliding columns are two aggregate results, which a
// name-keyed layer collapses into one value — a silent wrong answer. Read
// POSITIONALLY and compared against a DISTINCT-alias reference, exactly like
// TestDuplicateOutputNamesOnBothCoordinatorPaths above.
func TestAggregateAliasCollisionOnBothCoordinatorPaths(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	fast, dag := setupTwoPathCluster(t, ctx)

	const (
		armLocal = "A (single-process)"
		armDAG   = "B (stage DAG)"
	)

	for _, tc := range []dupShape{
		{name: "two sums under one alias",
			dup:       `SELECT SUM(n_nationkey) AS u, SUM(n_regionkey) AS u FROM nation`,
			ref:       `SELECT SUM(n_nationkey) AS u1, SUM(n_regionkey) AS u2 FROM nation`,
			unordered: true},
		{name: "count and sum under one alias",
			dup:       `SELECT COUNT(*) AS u, SUM(n_nationkey) AS u FROM nation`,
			ref:       `SELECT COUNT(*) AS u1, SUM(n_nationkey) AS u2 FROM nation`,
			unordered: true},
		{name: "min and max under one alias",
			dup:       `SELECT MIN(n_name) AS u, MAX(n_name) AS u FROM nation`,
			ref:       `SELECT MIN(n_name) AS u1, MAX(n_name) AS u2 FROM nation`,
			unordered: true},
		{name: "three aggregates under one alias",
			dup:       `SELECT SUM(n_nationkey) AS u, COUNT(*) AS u, MAX(n_regionkey) AS u FROM nation`,
			ref:       `SELECT SUM(n_nationkey) AS u1, COUNT(*) AS u2, MAX(n_regionkey) AS u3 FROM nation`,
			unordered: true},
		// An aggregate colliding with a group-key alias where the AGGREGATE
		// comes FIRST — appearance order is NOT slot order here (the aggregate
		// output column sits AFTER the group key), so a resolver that counts
		// select-list appearances against a keys-first name list sends the
		// COUNT to the group-key slot and loses it.
		{name: "aggregate before a colliding group key",
			dup: `SELECT COUNT(*) AS n_regionkey, n_regionkey AS x FROM nation GROUP BY n_regionkey ORDER BY x`,
			ref: `SELECT COUNT(*) AS u1, n_regionkey AS u2 FROM nation GROUP BY n_regionkey ORDER BY u2`},
		{name: "grouped sum and count under one alias",
			dup: `SELECT n_regionkey AS g, SUM(n_nationkey) AS u, COUNT(*) AS u FROM nation GROUP BY n_regionkey ORDER BY n_regionkey`,
			ref: `SELECT n_regionkey AS g, SUM(n_nationkey) AS u1, COUNT(*) AS u2 FROM nation GROUP BY n_regionkey ORDER BY n_regionkey`},
		// Regression guard: a GROUP BY key alias colliding with an aggregate
		// alias is already correct on both paths — proving the defect is
		// specifically two AGGREGATES sharing an alias.
		{name: "group key alias collides with aggregate alias",
			dup: `SELECT n_regionkey AS u, COUNT(*) AS u FROM nation GROUP BY n_regionkey ORDER BY n_regionkey`,
			ref: `SELECT n_regionkey AS u1, COUNT(*) AS u2 FROM nation GROUP BY n_regionkey ORDER BY n_regionkey`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range []struct {
				label string
				c     *coordinator.Coordinator
			}{{armLocal, fast}, {armDAG, dag}} {
				pin, pinned := tc.pins[arm.label]
				detail := tc.compareOnArm(ctx, arm.c)
				switch {
				case detail == "" && pinned:
					t.Errorf("arm %s now agrees, delete the pin on %q", arm.label, tc.name)
				case detail != "" && pinned:
					t.Logf("known divergence, NOT gated on arm %s: %s\n  %s", arm.label, detail, pin)
				case detail != "":
					t.Errorf("arm %s: %s\n  duplicate-alias spelling: %s\n  reference spelling:       %s",
						arm.label, detail, oneLine(tc.dup), oneLine(tc.ref))
				}
			}
		})
	}
}
