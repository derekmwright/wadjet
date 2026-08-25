package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// TestVectorGroupKeyIsAPinnedFailure pins #576: a VECTOR column in a GROUP BY
// position — DISTINCT, GROUP BY, COUNT(DISTINCT) — fails with
//
//	batch: cannot store string into VECTOR vector (#361 silent-write guard)
//
// on a shipped, first-class type. It is a loud failure, not a wrong answer,
// and the #361 guard is doing its job by refusing the write.
//
// This is a PIN in the ADR-0013 sense, inverted for a failure rather than a
// divergence: it asserts the defect is still present, and it FAILS when the
// queries start succeeding. That failure is the fix's proof and the signal to
// delete this file — do not "repair" it by loosening the assertion.
//
// The surviving shapes are asserted alongside, because they are what localizes
// the defect: the key ENCODING is fine (a join matches on c_vec, UNION dedupes
// on it, ORDER BY sorts it, and the other three container types get through
// DISTINCT), so only the materialization of a VECTOR group key back into an
// output column is broken. A future change that breaks one of THOSE is a
// regression this file should catch too.
func TestVectorGroupKeyIsAPinnedFailure(t *testing.T) {
	db := tmOpen(t)
	ctx := context.Background()
	n := typematrix.Nested

	const want = "cannot store string into VECTOR"

	t.Run("still_broken", func(t *testing.T) {
		for _, q := range []struct{ name, sql string }{
			{"distinct", fmt.Sprintf(`SELECT DISTINCT c_vec FROM %s`, n)},
			{"group_by", fmt.Sprintf(`SELECT c_vec, COUNT(*) FROM %s GROUP BY c_vec`, n)},
			{"count_distinct", fmt.Sprintf(`SELECT COUNT(DISTINCT c_vec) FROM %s`, n)},
			{"distinct_limit", fmt.Sprintf(`SELECT DISTINCT c_vec FROM %s LIMIT 3`, n)},
		} {
			t.Run(q.name, func(t *testing.T) {
				_, err := tmRun(ctx, db, q.sql)
				if err == nil {
					t.Fatalf("#576 appears FIXED for %s — delete this pin, that is the fix's proof", q.name)
				}
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("#576 changed shape for %s: got %q, expected it to contain %q",
						q.name, err.Error(), want)
				}
			})
		}
	})

	// The half that works, and must keep working: everything that touches the
	// VECTOR key ENCODING rather than its materialization as a group key.
	t.Run("encoding_paths_still_work", func(t *testing.T) {
		for _, q := range []struct {
			name    string
			sql     string
			minRows int
		}{
			{"plain_select", fmt.Sprintf(`SELECT c_vec FROM %s LIMIT 3`, n), 3},
			{"order_by", fmt.Sprintf(`SELECT c_vec FROM %s ORDER BY c_vec LIMIT 3`, n), 3},
			{"union", fmt.Sprintf(`SELECT c_vec FROM %s UNION SELECT c_vec FROM %s`, n, n), 1},
			{"join_on_vec", fmt.Sprintf(`SELECT COUNT(*) FROM %s a JOIN %s b ON a.c_vec = b.c_vec`, n, n), 1},
			{"distinct_array", fmt.Sprintf(`SELECT DISTINCT c_arr FROM %s`, n), 1},
			{"distinct_row", fmt.Sprintf(`SELECT DISTINCT c_row FROM %s`, n), 1},
			{"distinct_map", fmt.Sprintf(`SELECT DISTINCT c_map FROM %s`, n), 1},
		} {
			t.Run(q.name, func(t *testing.T) {
				res, err := tmRun(ctx, db, q.sql)
				if err != nil {
					t.Fatalf("%s must keep working — #576 is materialization, not encoding: %v", q.name, err)
				}
				if len(res.Rows) < q.minRows {
					t.Fatalf("%s returned %d rows, want at least %d", q.name, len(res.Rows), q.minRows)
				}
			})
		}
	})
}
