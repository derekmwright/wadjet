// SPDX-License-Identifier: MIT

package triage

import (
	"os"
	"strings"
	"testing"
)

func TestKnownDifferenceRules(t *testing.T) {
	page := "**Names are bounded.**\nRaises 42622 instead of truncation.\n\n**`TO_HEX(n)` widens.**\nHex differs.\n\n**`BYTEA` is refused.**\nType differs.\n\n**NATURAL JOIN is refused.**\nJoin differs.\n\n**Common words.**\nA value returns text; SELECT NULL FROM rows.\n\n**Prose only.**\n`TO_CHAR(n)` differs in its prose, not its heading.\n"
	known, err := LoadKnownDifferences(strings.NewReader(page))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, sql, cause, heading string }{
		{"state", "SELECT a", "SQLSTATE: 42622", "Names are bounded."},
		{"function", "SELECT to_hex(a)", "", "`TO_HEX(n)` widens."},
		{"type", "SELECT a::bytea", "", "`BYTEA` is refused."},
		{"clause", "SELECT a FROM x NATURAL\nJOIN y", "", "NATURAL JOIN is refused."},
		{"common word", "SELECT value FROM rows", "returned text", ""},
		{"identifier substring", "SELECT my_to_hex_column FROM x", "", ""},
		{"SQL number is not state", "SELECT 42622", "", ""},
		{"state substring", "SELECT a", "1426229", ""},
		{"prose function is not a keyword", "SELECT to_char(a)", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := known.Match(tc.sql, tc.cause)
			if ok != (tc.heading != "") || got.Heading != tc.heading {
				t.Fatalf("got %+v, %v; want %q", got, ok, tc.heading)
			}
		})
	}
}

func TestKnownDifferenceClassification(t *testing.T) {
	page, err := os.Open("../../../docs/postgres-differences.md")
	if err != nil {
		t.Fatal(err)
	}
	defer page.Close()
	known, err := LoadKnownDifferences(page)
	if err != nil {
		t.Fatal(err)
	}
	for _, log := range []string{
		"java.lang.AssertionError: SELECT x FROM t\n\tat check.run(Check.java:1)\nCaused by: ERROR: BYTEA is not a type here\n",
		"java.lang.AssertionError: the counts mismatch\n-- SELECT a FROM t QUALIFY row_number() OVER () = 1;\n\tat NoRECOracle.check(Check.java:1)\n",
		"java.lang.AssertionError: SELECT a FROM t\n\tat check.run(Check.java:1)\nCaused by: SQLSTATE 42622\n",
	} {
		r := NewReport()
		r.KnownDifferences = known
		if err := r.Classify("test", strings.NewReader(log)); err != nil {
			t.Fatal(err)
		}
		if r.Counts[CategoryKnownDifference] != 1 || r.Counts[CategoryUnexpectedError] != 0 || r.Counts[CategoryNoREC] != 0 {
			t.Fatalf("counts: %v", r.Counts)
		}
		if len(r.Findings) != 1 || r.Findings[0].Reason == "" || !strings.HasPrefix(r.Findings[0].Reference, "docs/postgres-differences.md:") {
			t.Fatalf("findings: %+v", r.Findings)
		}
	}
	r := NewReport()
	r.KnownDifferences = known
	if err := r.Classify("test", strings.NewReader("java.lang.AssertionError: SELECT value FROM t\n\tat check.run(Check.java:1)\nCaused by: unknown value\n")); err != nil {
		t.Fatal(err)
	}
	if r.Counts[CategoryUnexpectedError] != 1 || len(r.Findings) != 1 || !strings.Contains(r.Findings[0].Detail, "unknown value") {
		t.Fatalf("unexpected error was not retained: %+v", r)
	}
}

// A round whose header is SQLancer's generic "Found a potential bug" carries
// the engine's message on a later "-- On the database ..." line; the match
// and the report must read that line, not the header.
func TestRoundMessageReachesTheMatchAndTheReport(t *testing.T) {
	page, err := os.Open("../../../docs/postgres-differences.md")
	if err != nil {
		t.Fatal(err)
	}
	defer page.Close()
	known, err := LoadKnownDifferences(page)
	if err != nil {
		t.Fatal(err)
	}
	const head = "java.lang.AssertionError: Found a potential bug, please check log for detail.\n\tat sqlancer.Main$DBMSExecutor.run(Main.java:564)\n-- Time: 2026/09/17\nCREATE TABLE t0 (c0 INT);\n"
	const tail = "-- optimized: SELECT COUNT(*) FROM t0;\n-- unoptimized: SELECT SUM(count) FROM (SELECT 1 AS count FROM t0) AS res;\n"
	r := NewReport()
	r.KnownDifferences = known
	if err := r.Classify("test", strings.NewReader(head+"-- On the database set up by the statements above, the following queries trigger an unexpected error with message: ERROR: SQLSTATE 42622 identifier too long;\n"+tail)); err != nil {
		t.Fatal(err)
	}
	if r.Counts[CategoryKnownDifference] != 1 || len(r.Findings) != 1 || r.Findings[0].Reason == "" {
		t.Fatalf("documented SQLSTATE on the round's message line was not matched: %+v", r.Findings)
	}
	r = NewReport()
	r.KnownDifferences = known
	if err := r.Classify("test", strings.NewReader(head+"-- On the database set up by the statements above, the following queries trigger an unexpected error with message: ERROR: building physical plan: unknown function: symmetric;\n"+tail)); err != nil {
		t.Fatal(err)
	}
	r = NewReport()
	r.KnownDifferences = known
	if err := r.Classify("test", strings.NewReader(head+"-- On the database set up by the statements above, the following queries trigger an unexpected error with message: ERROR: building physical plan: join ON residual \"cast((t1.c6) as varchar)\" on a full join: not evaluable as a probe residual;\n"+tail)); err != nil {
		t.Fatal(err)
	}
	if r.Counts[CategoryKnownDifference] != 0 || r.Counts[CategoryUnexpectedError] != 1 {
		t.Fatalf("a type name inside the engine's quoted echo of the query must not match a page entry: %+v", r.Findings)
	}
	r = NewReport()
	r.KnownDifferences = known
	if err := r.Classify("test", strings.NewReader(head+"-- On the database set up by the statements above, the following queries trigger an unexpected error with message: ERROR: building physical plan: unknown function: symmetric;\n"+tail)); err != nil {
		t.Fatal(err)
	}
	f := r.Findings
	if r.Counts[CategoryUnexpectedError] != 1 || len(f) != 1 || !strings.Contains(f[0].Detail, "unknown function: symmetric") || len(f[0].Queries) != 2 || !strings.HasPrefix(f[0].Queries[1], "unoptimized: ") {
		t.Fatalf("undocumented message or its queries were not retained: %+v", f)
	}
	var out strings.Builder
	r.Print(&out)
	if !strings.Contains(out.String(), "unknown function: symmetric") || !strings.Contains(out.String(), "    query: optimized: ") {
		t.Fatalf("report does not show the engine message and queries:\n%s", out.String())
	}
}
