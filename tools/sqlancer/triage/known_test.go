// SPDX-License-Identifier: MIT

package triage

import (
	"os"
	"strings"
	"testing"
)

func TestKnownDifferenceRules(t *testing.T) {
	page := "**Names are bounded.**\nRaises 42622 instead of truncation.\n\n**Hex differs.**\n`TO_HEX(n)` widens.\n\n**Type differs.**\n`BYTEA` is refused.\n\n**Join differs.**\nNATURAL JOIN is refused.\n\n**Common words.**\nA value returns text; SELECT NULL FROM rows.\n"
	known, err := LoadKnownDifferences(strings.NewReader(page))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, sql, cause, heading string }{
		{"state", "SELECT a", "SQLSTATE: 42622", "Names are bounded."},
		{"function", "SELECT to_hex(a)", "", "Hex differs."},
		{"type", "SELECT a::bytea", "", "Type differs."},
		{"clause", "SELECT a FROM x NATURAL\nJOIN y", "", "Join differs."},
		{"common word", "SELECT value FROM rows", "returned text", ""},
		{"identifier substring", "SELECT my_to_hex_column FROM x", "", ""},
		{"SQL number is not state", "SELECT 42622", "", ""},
		{"state substring", "SELECT a", "1426229", ""},
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
		"java.lang.AssertionError: SELECT TO_HEX(a) FROM t\n\tat check.run(Check.java:1)\nCaused by: error\n",
		"java.lang.AssertionError: the counts mismatch\n-- SELECT TO_HEX(a) FROM t;\n\tat NoRECOracle.check(Check.java:1)\n",
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
