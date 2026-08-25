package tpch

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/coordinator"
)

// The gate that was missing.
//
// A result may legally carry two output columns of one NAME — PostgreSQL
// answers `SELECT upper(a), upper(b)` with two columns called `upper`, and
// #513 made this engine agree. Nothing that existed could see a value swap
// between them on the STAGE DAG:
//
//   - the two-path suite realigns arm B onto arm A BY NAME, so both arms
//     "agree" whatever either answers;
//   - the PostgreSQL wire oracle compares cells positionally, but only over
//     the single-process path;
//   - the pgwire transport tests feed a synthetic SliceStream, so they never
//     reach the gather's rename.
//
// So the gather's own projection — which resolved each rename BY NAME and
// handed two renames the same column — went unseen: 25 rows of
// `ALGERIA | ALGERIA` where PostgreSQL answers
// `ALGERIA | NATION ALGERIA COMMENT`.
//
// This gate reads the coordinator's batch stream POSITIONALLY, which is the
// only way to address a duplicate-named column, and runs every shape on BOTH
// coordinators.
func TestDuplicateOutputNamesOnBothCoordinatorPaths(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	fast, dag := setupTwoPathCluster(t, ctx)

	for _, tc := range []struct {
		name string
		sql  string
	}{
		// The producing fragment does NOT materialize the SELECT list here,
		// so the gather sees distinct source names — the shape that worked.
		{"two calls of one function",
			`SELECT UPPER(n_name), UPPER(n_comment) FROM nation`},
		// An ORDER BY term the SELECT list does not carry forces a projection
		// below the sort, which materializes BOTH aliases: the gather is then
		// handed two columns named `upper` and two renames spelled `upper`.
		{"under a hidden sort key",
			`SELECT UPPER(n_name), UPPER(n_comment) FROM nation ORDER BY n_regionkey, n_name`},
		{"a computed column and an alias",
			`SELECT UPPER(n_name), n_comment AS upper FROM nation ORDER BY n_regionkey, n_name`},
		{"an explicit duplicate alias",
			`SELECT UPPER(n_name) AS u, UPPER(n_comment) AS u FROM nation ORDER BY n_regionkey, n_name`},
		// Two PLAIN columns under one alias — the same defect with no
		// computed projection anywhere, which is how it was reachable before
		// #513 made duplicates ordinary.
		{"two plain columns under one alias",
			`SELECT n_name AS u, n_comment AS u FROM nation ORDER BY n_regionkey, n_name`},
		// Over a join, so the gather's input is a join fragment rather than a
		// scan one.
		{"over a join",
			`SELECT UPPER(n_name), UPPER(n_comment) FROM nation n
			 JOIN region r ON n.n_regionkey = r.r_regionkey ORDER BY r.r_name, n.n_name`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range []struct {
				label string
				c     *coordinator.Coordinator
			}{{"A (single-process)", fast}, {"B (stage DAG)", dag}} {
				cells, cols, err := streamRowValues(ctx, arm.c, tc.sql)
				if err != nil {
					t.Fatalf("arm %s: %v\n  SQL: %s", arm.label, err, tc.sql)
				}
				if len(cols) != 2 || cols[0] != cols[1] {
					t.Fatalf("arm %s returned columns %v — not the duplicate-name shape this gate is about",
						arm.label, cols)
				}
				if len(cells) != 25 {
					t.Fatalf("arm %s returned %d rows, want 25", arm.label, len(cells))
				}
				same := 0
				for i, row := range cells {
					if len(row) != 2 {
						t.Fatalf("arm %s row %d has %d cells, want 2", arm.label, i, len(row))
					}
					if fmt.Sprint(row[0]) == fmt.Sprint(row[1]) {
						same++
					}
				}
				if same == len(cells) {
					t.Errorf("arm %s: every one of %d rows has cell 0 == cell 1 — the second output "+
						"column carries the first one's value, which is what resolving a duplicate "+
						"name BY NAME does\n  SQL: %s\n  first row: %v",
						arm.label, same, tc.sql, cells[0])
				}
			}
		})
	}
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
