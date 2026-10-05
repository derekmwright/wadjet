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
	p, err := Parse("WITH s AS (SELECT random() AS r), u AS (SELECT random() AS r) SELECT a.r FROM s a, s b")
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
		t.Error("a random() body read twice is not evaluated once")
	}
	if info.CTEs[1].EvaluatedOnce() {
		t.Error("a random() body read by nobody is shared")
	}
}

// HOW OFTEN A STATEMENT READS A WITH ITEM decides whether it is shared: a
// volatile item read ONCE is planned as any other block (#1531 round 3). The
// count may only err high.
func TestStatementReadsOfAWithItem(t *testing.T) {
	withTestOracle(t)
	cells := []struct {
		sql   string
		reads []int // per WITH item
		once  []bool
	}{
		{"WITH s AS (SELECT random() r) SELECT r FROM s", []int{1}, []bool{false}},
		{"WITH s AS (SELECT random() r) SELECT r FROM s LIMIT 1", []int{1}, []bool{false}},
		{"WITH s AS (SELECT random() r) SELECT a.r FROM s a JOIN s b ON true", []int{2}, []bool{true}},
		{"WITH s AS (SELECT random() r) SELECT (SELECT r FROM s) <> (SELECT r FROM s)", []int{2}, []bool{true}},
		{"WITH s AS (SELECT random() r) SELECT EXISTS (SELECT 1 FROM s)", []int{1}, []bool{false}},
		{"WITH s AS (SELECT random() r) SELECT r FROM s UNION ALL SELECT r FROM s", []int{2}, []bool{true}},
		{"WITH s AS (SELECT random() r) SELECT s.r FROM s WHERE s.r > 0", []int{1}, []bool{false}},
		{"WITH s AS (SELECT random() r), t AS (SELECT r FROM s) SELECT a.r FROM t a, t b", []int{1, 2}, []bool{false, true}},
		{"WITH s AS (SELECT random() r), t AS (SELECT r FROM s), u AS (SELECT r FROM s) SELECT * FROM t, u", []int{2, 1, 1}, []bool{true, false, false}},
		{"WITH s AS (SELECT id FROM t) SELECT a.id FROM s a, s b", []int{2}, []bool{false}},
		// A reference inside a block with its own WITH counts twice: that
		// block's items are inlined at each of their references.
		{"WITH s AS (SELECT random() r) SELECT * FROM (WITH t AS (SELECT r FROM s) SELECT a.r FROM t a, t b) x", []int{2}, []bool{true}},
		{"WITH s AS (SELECT random() r) SELECT 'FROM s', u.s FROM u", []int{0}, []bool{false}},
		{"WITH s AS (SELECT id FROM t TABLESAMPLE BERNOULLI (50)) SELECT * FROM (s a JOIN s b ON a.id = b.id)", []int{2}, []bool{true}},
	}
	for _, c := range cells {
		p, err := Parse(c.sql)
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		info, err := ExtractSelect(p)
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		for i := range info.CTEs {
			if got := info.CTEs[i].Reads(); got != c.reads[i] {
				t.Errorf("%s: item %s read %d times, want %d", c.sql, info.CTEs[i].Name, got, c.reads[i])
			}
			if got := info.CTEs[i].EvaluatedOnce(); got != c.once[i] {
				t.Errorf("%s: item %s shared = %v, want %v", c.sql, info.CTEs[i].Name, got, c.once[i])
			}
		}
	}
}
