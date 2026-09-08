package coordinator

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"
)

// THE MERGE AT SCALE — the cell arc M1's brief asked for and its first round
// did not have: 5000 rows on both DAG arms, so the coordinator's merge has to
// COALESCE more than one batch.
//
// Every cell in `TestM1AMergedOrderIsTheQuerysOrder` is 8, 4 or 3 rows — ONE
// batch — so `coalesceForOrdering` returns its input untouched and nothing in
// that gate ever executes the path §8a's contract is about. `typemx` is 5000
// rows past `batch.DefaultBatchSize` (2048) with a NULL every few rows in
// every nullable column, which is what turns the coalesce into the thing under
// test.
//
// It found #1007 there: a NULL row skipped `copyVectorValue` entirely, so a
// `BytesColumn`'s closing offset stayed at zero and the NEXT non-null value was
// read from the arena's origin — 116 of 5000 `c_str` values came back as the
// concatenation of every value above them, on both DAG arms, silently.
//
// The assertion is the SINGLE arm's own answer, row for row, rather than a
// literal: 5000 rows of 22 columns is not a `want` string anyone can read, and
// the single-process path is the one PostgreSQL agreed with on every shape this
// arc measured. What is asserted is therefore the two-path invariant the DAG
// owes — same rows, same values, same sequence — over a result the merge had to
// build out of several batches.
func TestM1AMergedOrderIsTheQuerysOrderAtScale(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	for _, tc := range []struct{ name, sql string }{
		{
			// #1007's exact repro: a star DISTINCT over a self-join, which is
			// the one user DISTINCT that reaches the coordinator's dedup and
			// its re-sort (`starDistinctGroupKeys` declines a name two scans
			// publish), at a row count that needs more than one batch.
			name: "a star DISTINCT over a 5000-row self-join, ORDER BY id DESC",
			sql:  "SELECT DISTINCT * FROM typemx a JOIN typemx b ON b.id = a.id ORDER BY a.id DESC",
		},
		{
			// ASC, so a run that reverses is not the same run that passes.
			name: "the same ORDER BY id ASC",
			sql:  "SELECT DISTINCT * FROM typemx a JOIN typemx b ON b.id = a.id ORDER BY a.id",
		},
		{
			// A key that is itself NULLABLE and a VARLEN carrier, so the
			// ordering reads the column the corruption is in — `c_str` carries
			// a NULL every few rows, and `id` breaks the ties.
			name: "ordered by a nullable STRING key, ties broken by id",
			sql: "SELECT DISTINCT * FROM typemx a JOIN typemx b ON b.id = a.id " +
				"ORDER BY a.c_str, a.id",
		},
		{
			// The same with the STRING key DESC, which flips where the NULLs
			// land (NULLS FIRST for DESC) and so which rows sit either side of
			// the batch boundary.
			name: "ordered by a nullable STRING key DESC, ties broken by id",
			sql: "SELECT DISTINCT * FROM typemx a JOIN typemx b ON b.id = a.id " +
				"ORDER BY a.c_str DESC, a.id",
		},
		{
			// GROUP BY rather than DISTINCT — the other half of the brief's
			// gate list, at scale, over columns of three different carriers.
			name: "GROUP BY over a 5000-row self-join, three carriers",
			sql: "SELECT a.id, a.c_str, b.c_bytes FROM typemx a JOIN typemx b ON b.id = a.id " +
				"GROUP BY a.id, a.c_str, b.c_bytes ORDER BY a.id DESC",
		},
		{
			// A LIMIT past one batch, so the top-K heap is the comparator and
			// the coalesce still has to happen first.
			name: "the same under a LIMIT that still crosses a batch",
			sql: "SELECT DISTINCT * FROM typemx a JOIN typemx b ON b.id = a.id " +
				"ORDER BY a.id DESC LIMIT 3000",
		},
		{
			// An OFFSET that lands INSIDE the second batch, so a mis-ordered
			// merge returns a different page rather than a different sequence.
			name: "the same under an OFFSET inside the second batch",
			sql: "SELECT DISTINCT * FROM typemx a JOIN typemx b ON b.id = a.id " +
				"ORDER BY a.id DESC LIMIT 20 OFFSET 2500",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ref string
			var refArm string
			for _, arm := range arms {
				got, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s\n  arm %s: %v", tc.sql, arm.name, err)
				}
				if ref == "" {
					ref, refArm = got, arm.name
					if !strings.Contains(got, " rows=") {
						t.Fatalf("%s\n  arm %s produced no row count: %.200s",
							tc.sql, arm.name, got)
					}
					continue
				}
				if got == ref {
					continue
				}
				t.Errorf("%s\n  arm %s disagrees with %s — same rows, same values and "+
					"same sequence are what every arm owes a total order over a merged "+
					"result (#1002, #1007)\n  first difference: %s",
					tc.sql, arm.name, refArm, f1FirstRowDiff(refArm, ref, arm.name, got))
			}
		})
	}
}

// f1FirstRowDiff names the first row two renderings disagree on, because the
// renderings are 5000 rows long and a diff of the whole string says nothing a
// reader can act on.
func f1FirstRowDiff(aName, a, bName, b string) string {
	ar := strings.Split(a, " | ")
	br := strings.Split(b, " | ")
	n := min(len(ar), len(br))
	for i := 0; i < n; i++ {
		if ar[i] == br[i] {
			continue
		}
		return "row " + strconv.Itoa(i) + "\n    " + aName + ": " + trunc(ar[i]) +
			"\n    " + bName + ": " + trunc(br[i])
	}
	if len(ar) != len(br) {
		return "row counts differ: " + aName + " has " + strconv.Itoa(len(ar)) +
			", " + bName + " has " + strconv.Itoa(len(br))
	}
	return "(no row differs; the header does)"
}

func trunc(s string) string {
	if len(s) <= 200 {
		return s
	}
	return s[:200] + "…"
}
