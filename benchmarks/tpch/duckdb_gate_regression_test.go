package tpch

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// TestGateCatchesHistoricalBugs is the gate's own proof of work: for each
// wrong-answer bug that shipped past every gate on 2026-08-17, it
// reconstructs the pre-fix ANSWER and requires the DuckDB fingerprint gate
// to reject it — and, where the old gates were the reason the bug survived,
// shows those gates accepting the same wrong answer.
//
// The fixes are in the engine and cannot be reverted here, so each bug is
// reproduced at the level of the result: the predicate-free row set for a
// dropped predicate (which is literally what the DAG returned), a permuted
// row sequence for a dropped ORDER BY, a NULLed column for a lost alias.
// A gate that cannot demonstrate it catches yesterday's bugs is not a gate.
//
// Every case also asserts the gate ACCEPTS the correct answer first;
// otherwise "rejects the mutation" would be satisfied by a gate that rejects
// everything.
func TestGateCatchesHistoricalBugs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	db := ingestDuckDBFixture(t, ctx, duckdbFixtureRows(t))
	stored := loadDuckDBBaseline(t)
	corpus := make(map[string]duckdbCase, len(duckdbCorpus()))
	for _, c := range duckdbCorpus() {
		corpus[c.name] = c
	}

	// run answers one corpus query on the single-process engine and asserts
	// the gate accepts that answer, which is the control for every mutation
	// below.
	run := func(t *testing.T, name string) (duckdbCase, duckdbBaselineEntry, []map[string]any, []string) {
		t.Helper()
		c, ok := corpus[name]
		if !ok {
			t.Fatalf("no corpus entry %s", name)
		}
		rows, cols, err := runWadjet(ctx, db, c.sql)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if ok, detail := compareToDuckDB(c, stored[name], rows, cols); !ok {
			t.Fatalf("%s: the CORRECT answer does not match the stored DuckDB fingerprint (%s) — "+
				"the mutations below would prove nothing", name, detail)
		}
		return c, stored[name], rows, cols
	}

	reject := func(t *testing.T, c duckdbCase, want duckdbBaselineEntry, rows []map[string]any, cols []string, what string) {
		t.Helper()
		if ok, _ := compareToDuckDB(c, want, rows, cols); ok {
			t.Errorf("the gate ACCEPTED %s — it would not have caught this bug", what)
		}
	}

	// #312 — the stage DAG dropped a WHERE equality between two joined
	// tables that is not itself a join condition (c_nationkey =
	// s_nationkey), so Q05 answered with exactly the row set the
	// predicate-free query produces and its revenues came back ~25x
	// inflated. The row COUNT is identical either way, which is all the
	// DuckDB gate checked for this query at the time; worse, the recorded
	// distributed baseline had frozen the inflated numbers, so the wrong
	// answer WAS the expectation.
	t.Run("DroppedJoinPredicate_312", func(t *testing.T) {
		c, want, correct, cols := run(t, "Q05")

		predicate := "WHERE c_nationkey = s_nationkey\n\t\t\tAND r_name = 'ASIA'"
		if strings.Count(c.sql, predicate) != 1 {
			t.Fatalf("Q05 no longer contains the #312 predicate verbatim; update this reconstruction")
		}
		prefix := strings.Replace(c.sql, predicate, "WHERE r_name = 'ASIA'", 1)
		wrong, wrongCols, err := runWadjet(ctx, db, prefix)
		if err != nil {
			t.Fatalf("predicate-free Q05: %v", err)
		}

		if len(wrong) != len(correct) {
			t.Logf("note: the predicate-free row count (%d) differs from the correct one (%d) at this scale",
				len(wrong), len(correct))
		} else {
			t.Logf("row count is %d either way — a row-count gate cannot see this bug", len(correct))
		}
		inflation := sumColumn(wrong, "revenue") / sumColumn(correct, "revenue")
		t.Logf("revenue inflated %.1fx by the dropped predicate", inflation)
		if inflation < 2 {
			t.Fatalf("the reconstruction did not inflate revenue (%.2fx); it is not reproducing #312", inflation)
		}
		reject(t, c, want, wrong, wrongCols, "Q05 answered with the dropped join predicate (#312)")

		// And the direction that matters: ground truth comes from DuckDB, so
		// the stored fingerprint is the CORRECT answer's. A baseline recorded
		// from the engine would have frozen the inflated one and failed the
		// fixed engine instead.
		if ok, detail := compareToDuckDB(c, want, correct, cols); !ok {
			t.Errorf("stored DuckDB fingerprint does not accept the correct Q05: %s", detail)
		}
	})

	// #313 / #316 / #320 — the row SEQUENCE was lost while every value and
	// the row count stayed right: an aliased ORDER BY key resolved to
	// nothing on the DAG (#313), the aggregate-free form of the same shape
	// (#316), and an ORDER BY over an expression that was ignored on BOTH
	// paths at once (#320). A result compared without regard to order, or by
	// a per-column numeric sum, is by construction blind to all three.
	t.Run("LostOrderBy_313_316_320", func(t *testing.T) {
		for _, name := range []string{"Q09", "Q10_nolimit", "AliasedGroupKeyOrderBy", "AliasedSortNoAggregate", "OrderByExpression", "Q07"} {
			t.Run(name, func(t *testing.T) {
				c, want, correct, cols := run(t, name)
				if !c.ordered {
					t.Fatalf("%s is not gated as ordered, so this case proves nothing", name)
				}
				if len(correct) < 2 {
					t.Fatalf("%s returned %d rows — too few for a sequence to be lost", name, len(correct))
				}

				unsorted := reverseRows(correct)
				reject(t, c, want, unsorted, cols, name+" returned in the wrong order (#313/#316/#320)")

				// The same rows, compared without order, pass — which is
				// exactly why the mode has to be decided per query and not
				// chosen for convenience.
				unorderedWant := oracle.FingerprintOf(&oracle.Result{Columns: want.Columns, Rows: correct}, false)
				if ok, _ := unorderedWant.Match(oracle.FingerprintOf(&oracle.Result{Columns: want.Columns, Rows: unsorted}, false)); !ok {
					t.Errorf("the permutation changed more than the row order; the case is not isolating ORDER BY")
				}

				// And the gate that was green through all three: per-column
				// numeric sums are order-insensitive by construction.
				if a, b := columnSums(correct), columnSums(unsorted); a != b {
					t.Errorf("value signature saw the reordering (%s vs %s) — it is not reproducing the blind spot", a, b)
				}
			})
		}
	})

	// #314 — a join qualified only its build side's colliding columns, so
	// the alias that landed on the probe side came back NULL: Q07's
	// supp_nation was NULL for every row while cust_nation was populated.
	// Row count, column set and every numeric column were untouched, and the
	// harness value signature skips string columns entirely, so nothing
	// registered.
	t.Run("NulledStringColumn_314", func(t *testing.T) {
		c, want, correct, cols := run(t, "Q07")

		nulled := mutateColumn(correct, "supp_nation", func(any) any { return nil })
		if len(nulled) != len(correct) {
			t.Fatal("mutation changed the row count")
		}
		reject(t, c, want, nulled, cols, "Q07 with supp_nation NULLed on every row (#314)")
		if a, b := columnSums(correct), columnSums(nulled); a != b {
			t.Errorf("value signature saw the NULLed string column (%s vs %s) — it is not reproducing the blind spot", a, b)
		}

		// An empty string is not a NULL and not the right value either.
		emptied := mutateColumn(correct, "supp_nation", func(any) any { return "" })
		reject(t, c, want, emptied, cols, "Q07 with supp_nation emptied on every row")

		// The same result with the OTHER alias nulled must also fail, or the
		// gate would only be watching one of the two colliding columns.
		reject(t, c, want, mutateColumn(correct, "cust_nation", func(any) any { return nil }), cols,
			"Q07 with cust_nation NULLed on every row")
	})

	// #315's sibling shape and the star-expansion class: a column that comes
	// back under a different name, or an extra planner column leaking to the
	// client, is a different answer even when every value is right.
	t.Run("ColumnSetChanges", func(t *testing.T) {
		c, want, correct, cols := run(t, "Q05")

		renamed := make([]map[string]any, len(correct))
		for i, r := range correct {
			m := map[string]any{"revenue": r["revenue"], "nation_name": r["n_name"]}
			renamed[i] = m
		}
		reject(t, c, want, renamed, []string{"nation_name", "revenue"}, "Q05 with n_name renamed")

		extra := make([]map[string]any, len(correct))
		for i, r := range correct {
			m := map[string]any{"n_name": r["n_name"], "revenue": r["revenue"], "__sortkey_0": 1}
			extra[i] = m
		}
		reject(t, c, want, extra, append(append([]string(nil), cols...), "__sortkey_0"),
			"Q05 with the planner's materialized sort column leaking to the client")
	})

	// The LIMIT policy, stated as a test: a bare LIMIT is compared by count
	// because SQL does not say which rows come back — but the count is not
	// negotiable, and a limit that fails to bind is caught.
	t.Run("BareLimitIsCountOnlyButStillBinds", func(t *testing.T) {
		c, want, correct, cols := run(t, "BareLimit")
		if !c.countOnly || c.limit != 7 {
			t.Fatalf("BareLimit is gated as countOnly=%v limit=%d; the policy changed", c.countOnly, c.limit)
		}
		if len(correct) != 7 {
			t.Fatalf("BareLimit returned %d rows, want 7", len(correct))
		}
		// Different rows, same count: admissible, because the query does not
		// say which seven.
		other, _, err := runWadjet(ctx, db, "SELECT l_orderkey FROM lineitem WHERE l_orderkey > 1000 LIMIT 7")
		if err != nil {
			t.Fatal(err)
		}
		if ok, detail := compareToDuckDB(c, want, other, cols); !ok {
			t.Errorf("the gate rejected a different but equally admissible set of 7 rows: %s", detail)
		}
		// An unbound limit is the c5e77cf bug and must fail.
		unbounded, ucols, err := runWadjet(ctx, db, "SELECT l_orderkey FROM lineitem")
		if err != nil {
			t.Fatal(err)
		}
		reject(t, c, want, unbounded, ucols, "a bare LIMIT that did not bind (25 rows for LIMIT 3, c5e77cf)")
	})

	// The float tolerance has to be tight enough to be worth having: a value
	// error two orders of magnitude above accumulation noise must fail.
	t.Run("SmallValueDriftStillFails", func(t *testing.T) {
		c, want, correct, cols := run(t, "Q01")
		drifted := mutateColumn(correct, "sum_disc_price", func(v any) any {
			f, _ := v.(float64)
			return f * 1.001
		})
		reject(t, c, want, drifted, cols, "Q01 with sum_disc_price off by 0.1%")
	})
}

// reverseRows returns the rows in the opposite sequence — the minimal
// mutation that changes only the order.
func reverseRows(rows []map[string]any) []map[string]any {
	out := make([]map[string]any, len(rows))
	for i, r := range rows {
		out[len(rows)-1-i] = r
	}
	return out
}

// mutateColumn copies rows with one column rewritten.
func mutateColumn(rows []map[string]any, col string, f func(any) any) []map[string]any {
	out := make([]map[string]any, len(rows))
	for i, r := range rows {
		m := make(map[string]any, len(r))
		for k, v := range r {
			m[k] = v
		}
		m[col] = f(r[col])
		out[i] = m
	}
	return out
}

func sumColumn(rows []map[string]any, col string) float64 {
	var s float64
	for _, r := range rows {
		if f, ok := r[col].(float64); ok {
			s += f
		}
	}
	return s
}

// columnSums is the shape of gate that was green through #313 and #314: a
// per-column sum over the NUMERIC cells only, order-insensitive because
// addition is. It mirrors internal/harness's ValueSigAccum (which cannot be
// imported here — internal/harness imports this package) closely enough to
// demonstrate what that class of signature cannot see.
func columnSums(rows []map[string]any) string {
	sums := map[string]float64{}
	for _, r := range rows {
		for k, v := range r {
			switch x := v.(type) {
			case float64:
				sums[k] += x
			case float32:
				sums[k] += float64(x)
			case int:
				sums[k] += float64(x)
			case int32:
				sums[k] += float64(x)
			case int64:
				sums[k] += float64(x)
			}
		}
	}
	keys := make([]string, 0, len(sums))
	for k := range sums {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&sb, "%s:%.9e,", k, sums[k])
	}
	return sb.String()
}

// TestDuckDBBaselineProvenance: the stored file must be traceable to DuckDB
// end to end, and the loader must refuse it when it is not. "Where did this
// expected value come from" is the question the Q05 incident answered badly
// — the recorded distributed baseline had frozen a 25x-inflated revenue, so
// the expectation itself was the bug.
func TestDuckDBBaselineProvenance(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(".", duckdbBaselineFile))
	if err != nil {
		t.Fatalf("read %s: %v", duckdbBaselineFile, err)
	}
	var b duckdbBaseline
	if err := json.Unmarshal(data, &b); err != nil {
		t.Fatalf("parse %s: %v", duckdbBaselineFile, err)
	}
	if err := validateDuckDBBaseline(b); err != nil {
		t.Fatalf("committed baseline is not valid ground truth: %v", err)
	}
	if !strings.Contains(strings.ToLower(b.Generator), "duckdb") {
		t.Errorf("generator %q does not name DuckDB — the file's provenance is unrecorded", b.Generator)
	}
	for name, e := range b.Queries {
		if e.Source != "duckdb" {
			t.Errorf("entry %s: source %q", name, e.Source)
		}
	}
	t.Logf("%d entries, all DuckDB-derived, generated by: %s", len(b.Queries), b.Generator)

	// And the refusal: a single Wadjet-sourced entry must be fatal, not a
	// warning, or the guarantee is decorative.
	poisoned := duckdbBaseline{Generator: b.Generator, Queries: map[string]duckdbBaselineEntry{
		"Q05": {Source: "wadjet", RowCount: 5, Compare: "rows", Columns: []string{"n_name"}, Fine: "x", Coarse: "y"},
	}}
	if err := validateDuckDBBaseline(poisoned); err == nil {
		t.Error("a baseline entry recorded from Wadjet was accepted as ground truth")
	} else if !strings.Contains(err.Error(), "NOT ground truth") {
		t.Errorf("refusal is not loud enough: %v", err)
	}
	// A rows entry with no fingerprint would silently gate nothing.
	empty := duckdbBaseline{Queries: map[string]duckdbBaselineEntry{
		"Q05": {Source: "duckdb", RowCount: 5, Compare: "rows"},
	}}
	if err := validateDuckDBBaseline(empty); err == nil {
		t.Error("a rows entry with no fingerprint was accepted")
	}
}

// TestTopLevelOrderByDetection pins the decision that makes the gate
// order-sensitive in the right places. Getting it wrong in the permissive
// direction is silent: the query keeps passing, and a dropped ORDER BY stops
// being a failure.
func TestTopLevelOrderByDetection(t *testing.T) {
	cases := []struct {
		sql  string
		want bool
	}{
		{"SELECT a FROM t", false},
		{"SELECT a FROM t ORDER BY a", true},
		{"SELECT a FROM t ORDER BY a DESC, b", true},
		{"SELECT a FROM t ORDER BY LENGTH(a), a", true},                    // parens of a function, not a subquery
		{"SELECT a FROM t WHERE a IN (SELECT b FROM u ORDER BY b)", false}, // a subquery's own
		{"SELECT a FROM (SELECT b AS a FROM u ORDER BY b) x", false},
		{"SELECT a FROM t WHERE s = 'ORDER BY x'", false}, // inside a string literal
		{"SELECT a FROM t WHERE s = 'a(b' ORDER BY a", true},
		{"SELECT a FROM t ORDER BY a LIMIT 10", true},
		{"SELECT a FROM t LIMIT 10", false},
	}
	for _, tc := range cases {
		if got := hasTopLevelOrderBy(tc.sql); got != tc.want {
			t.Errorf("hasTopLevelOrderBy(%q) = %v, want %v", tc.sql, got, tc.want)
		}
	}

	// Every corpus entry's mode must match what its SQL says, which is also
	// what assertBaselineCoversCorpus enforces against the stored file.
	ordered := 0
	for _, c := range duckdbCorpus() {
		if c.countOnly {
			continue
		}
		if c.ordered != hasTopLevelOrderBy(c.sql) {
			t.Errorf("%s: ordered=%v but its SQL says %v", c.name, c.ordered, hasTopLevelOrderBy(c.sql))
		}
		if c.ordered {
			ordered++
		}
	}
	if ordered == 0 {
		t.Error("no corpus entry is order-sensitive — the ORDER BY class is ungated")
	}
	t.Logf("%d of %d corpus entries are compared order-sensitively", ordered, len(duckdbCorpus()))
}
