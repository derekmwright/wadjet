// SPDX-License-Identifier: MIT

package sql

import (
	"strings"
	"testing"
)

// The function registry (expr) installs the volatility oracle; this package's
// tests stand in for it with the builtins it marks.
func withTestOracle(t *testing.T) {
	t.Helper()
	prev := volatileOracle.Load()
	SetVolatileFunctionOracle(func(name string) bool {
		switch strings.ToLower(name) {
		case "random", "rand", "uuid":
			return true
		}
		return false
	})
	t.Cleanup(func() { volatileOracle.Store(prev) })
}

// A CTE body is volatile when a TOKEN calls a volatile function or samples a
// relation, wherever in the body — and never because a string, a column or a
// delimited identifier spells the word.
func TestTextIsVolatileReadsTokens(t *testing.T) {
	withTestOracle(t)
	cells := []struct {
		sql  string
		want bool
	}{
		{"SELECT random() AS r FROM t", true},
		{"SELECT RANDOM ( ) FROM t", true},
		{"SELECT pg_catalog.random() FROM t", true},
		{"SELECT rand() FROM t", true},
		{"SELECT CAST(uuid() AS TEXT) FROM t", true},
		{"SELECT gen_random_uuid() FROM t", false}, // not a function this engine has (42883)
		{"SELECT id FROM t TABLESAMPLE BERNOULLI (50)", true},
		{"SELECT id FROM t tablesample system (5)", true},
		{"SELECT id FROM t WHERE id IN (SELECT id FROM u WHERE random() < 0.5)", true},
		{"SELECT (WITH q AS (SELECT random() AS r) SELECT r FROM q) AS x", true},
		{"SELECT id FROM t", false},
		{"SELECT 'random()' AS s FROM t", false},
		{"SELECT random FROM t", false},
		{"SELECT CAST(id AS uuid) FROM t", false},
		{`SELECT "tablesample" FROM t`, false},
		{"SELECT now(), clock_timestamp() FROM t", false},
	}
	for _, c := range cells {
		if got := TextIsVolatile(c.sql); got != c.want {
			t.Errorf("TextIsVolatile(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}

// Every copy of a WITH item carries the item's identity; two items do not
// share one, even when they spell the same body.
func TestCTEIdentityTravelsWithTheValue(t *testing.T) {
	withTestOracle(t)
	p, err := Parse("WITH s AS (SELECT random() AS r), u AS (SELECT random() AS r) SELECT r FROM s")
	if err != nil {
		t.Fatal(err)
	}
	info, err := ExtractSelect(p)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.CTEs) != 2 {
		t.Fatalf("%d CTEs", len(info.CTEs))
	}
	cp := append([]CTEDef(nil), info.CTEs...)
	if cp[0].Identity() == nil || cp[0].Identity() != info.CTEs[0].Identity() {
		t.Error("a copied WITH item lost its identity")
	}
	if info.CTEs[0].Identity() == info.CTEs[1].Identity() {
		t.Error("two WITH items share one identity")
	}
	if !info.CTEs[0].EvaluatedOnce() {
		t.Error("a random() body is not evaluated once")
	}
}
