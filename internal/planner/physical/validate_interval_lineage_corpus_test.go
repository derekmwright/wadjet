// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The interval corpus (#1680b; the coordinator's testdata) read as data: its
// statements, its tables' DDL, and PostgreSQL 17.11's declared result types.
const ivCorpusDir = "../../coordinator/testdata/arc_1680b"

var ivDDL = regexp.MustCompile(`^CREATE TABLE (\w+) \((.*)\)$`)

// ivCorpusCatalog is the corpus's tables, from its DDL cells.
func ivCorpusCatalog(t *testing.T) brSchemaCatalog {
	t.Helper()
	cat := brSchemaCatalog{}
	files, _ := filepath.Glob(filepath.Join(ivCorpusDir, "*.statements.tsv"))
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(raw), "\n") {
			fs := strings.Split(line, "\t")
			if len(fs) != 4 || fs[2] != "ddl" {
				continue
			}
			m := ivDDL.FindStringSubmatch(fs[3])
			if m == nil {
				continue
			}
			var cols []parquet.Column
			for _, c := range strings.Split(m[2], ",") {
				parts := strings.Fields(c)
				typ := map[string]parquet.TypeID{"BIGINT": parquet.TypeInt64, "TEXT": parquet.TypeString,
					"TIMESTAMP": parquet.TypeTimestamp, "DATE": parquet.TypeDate}[parts[1]]
				cols = append(cols, parquet.Column{Name: parts[0], Type: typ, Nullable: true})
			}
			cat[strings.ToLower(m[1])] = parquet.Schema{Columns: cols}
		}
	}
	return cat
}

// ivCorpusCell is one SELECT of the corpus with PostgreSQL's declared types.
type ivCorpusCell struct {
	key, sql string
	pgTypes  []string
}

func ivCorpusCells(t *testing.T) []ivCorpusCell {
	t.Helper()
	var out []ivCorpusCell
	files, _ := filepath.Glob(filepath.Join(ivCorpusDir, "*.statements.tsv"))
	sort.Strings(files)
	for _, f := range files {
		stem := strings.TrimSuffix(filepath.Base(f), ".statements.tsv")
		pg := map[string]string{}
		raw, err := os.ReadFile(filepath.Join(ivCorpusDir, stem+"_pg17.tsv"))
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(raw), "\n") {
			if name, ans, ok := strings.Cut(line, "\t"); ok && !strings.HasPrefix(line, "#") {
				pg[name] = ans
			}
		}
		raw, err = os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(raw), "\n") {
			fs := strings.Split(line, "\t")
			if len(fs) != 4 || fs[2] != "sql" || !strings.HasPrefix(fs[3], "SELECT") && !strings.HasPrefix(fs[3], "WITH") {
				continue
			}
			ans := pg[fs[0]]
			if strings.HasPrefix(ans, "ERR ") || !strings.Contains(ans, " | ") {
				continue
			}
			head, _, _ := strings.Cut(ans, " | ")
			var types []string
			for _, c := range strings.Split(head, ",") {
				_, typ, _ := strings.Cut(c, ":")
				if i := strings.LastIndexByte(typ, '('); i > 0 {
					typ = typ[:i]
				}
				types = append(types, typ)
			}
			out = append(out, ivCorpusCell{stem + ":" + fs[0], fs[3], types})
		}
	}
	return out
}

// ivSyntactic is the interim's classification of a block's result columns:
// the provenance rule over each column's own expression (a star unknown).
func ivSyntactic(info *plansql.SelectInfo) ([]bool, bool) {
	if info.Union != nil {
		l, lok := ivSyntactic(info.Union.Left)
		r, rok := ivSyntactic(info.Union.Right)
		if !lok || !rok || len(l) != len(r) {
			return nil, false
		}
		out := make([]bool, len(l))
		for i := range l {
			out[i] = l[i] || r[i]
		}
		return out, true
	}
	var out []bool
	for _, c := range info.Columns {
		if c.Star || c.ASTExpr == nil {
			return nil, false
		}
		out = append(out, expr.IsIntervalNode(c.ASTExpr))
	}
	return out, true
}

// ivLineage is the binder's declared lineage of the statement's result.
func ivLineage(cat brSchemaCatalog, info *plansql.SelectInfo) ([]bool, bool) {
	b := &binder{ctx: context.Background(), src: cat, ctes: map[string]cteEntry{}}
	b.root = info
	_ = b.validateBlock(context.Background(), info, nil)
	out, ok := b.intervalOut[info]
	return out, ok
}

// ivLineageDisagreements are the corpus cells where the interim's syntactic
// classification and the declared lineage disagree, each where lineage is
// PostgreSQL's declared type and syntax is not: a producer the syntax
// cannot see (an aggregate, a window value function or a scalar subquery
// over an interval; a forwarded column; a timestamp difference).
var ivLineageDisagreements = map[string]bool{
	// a function PostgreSQL declares interval (make_interval, justify_*).
	"01_input:make/all":     true,
	"01_input:make/zero":    true,
	"05_arith:justify/null": true,
	"05_arith:justify/zero": true,
	// age, declared interval by the rulings on 1680c r3 (this engine has no
	// age; PostgreSQL declares every overload interval).
	"06_calendar:age/basic":                           true,
	"06_calendar:age/time":                            true,
	"06_calendar:tz/America/New_York/age":             true,
	"06_calendar:tz/UTC/age":                          true,
	"10_definitions:age/bc":                           true,
	"10_definitions:age/borrow":                       true,
	"10_definitions:age/date":                         true,
	"10_definitions:age/dateGapHavana":                true,
	"10_definitions:age/dateGapUTC":                   true,
	"10_definitions:age/dateMonthNY":                  true,
	"10_definitions:age/dateNY":                       true,
	"10_definitions:age/dateUTC":                      true,
	"10_definitions:age/infLeft":                      true,
	"10_definitions:age/infOpposed":                   true,
	"10_definitions:age/infRight":                     true,
	"10_definitions:age/null":                         true,
	"10_definitions:age/timeBorrow":                   true,
	"10_definitions:age/tz/America/New_York/fallBack": true,
	"10_definitions:age/tz/UTC/fallBack":              true,
	// an aggregate or window value function over an interval (min / max / sum / avg).
	"03_order:order/minMax":         true,
	"03_order:order/minMaxFinite":   true,
	"03_order:order/minTieTable":    true,
	"05_arith:agg/avgCascade":       true,
	"05_arith:agg/avgThirds":        true,
	"05_arith:agg/avgWindowSliding": true,
	"05_arith:agg/sumAvg":           true,
	"05_arith:agg/sumAvgSubset":     true,
	"05_arith:agg/sumEmpty":         true,
	"05_arith:agg/sumGroup":         true,
	"05_arith:agg/sumNull":          true,
	"05_arith:agg/sumWindow":        true,
	// a column forwarded across a derived table, a DISTINCT or a set operation.
	"04_keys:distinct/iv80_k/default":   true,
	"04_keys:distinct/iv80_k/hashagg":   true,
	"04_keys:distinct/iv80_k/sortagg":   true,
	"04_keys:distinct/iv80_kr/default":  true,
	"04_keys:distinct/iv80_kr/hashagg":  true,
	"04_keys:distinct/iv80_kr/sortagg":  true,
	"04_keys:exceptAll/iv80_k/default":  true,
	"04_keys:exceptAll/iv80_k/hashagg":  true,
	"04_keys:exceptAll/iv80_k/sortagg":  true,
	"04_keys:exceptAll/iv80_kr/default": true,
	"04_keys:exceptAll/iv80_kr/hashagg": true,
	"04_keys:exceptAll/iv80_kr/sortagg": true,
	"04_keys:intersect/iv80_k/default":  true,
	"04_keys:intersect/iv80_k/hashagg":  true,
	"04_keys:intersect/iv80_k/sortagg":  true,
	"04_keys:intersect/iv80_kr/default": true,
	"04_keys:intersect/iv80_kr/hashagg": true,
	"04_keys:intersect/iv80_kr/sortagg": true,
	"04_keys:union/iv80_k/default":      true,
	"04_keys:union/iv80_k/hashagg":      true,
	"04_keys:union/iv80_k/sortagg":      true,
	"04_keys:union/iv80_kr/default":     true,
	"04_keys:union/iv80_kr/hashagg":     true,
	"04_keys:union/iv80_kr/sortagg":     true,
	"10_definitions:closure/whereFalse": true,
	// a timestamp difference (the interim typed it by the declaration of its operands, not by syntax).
	"06_calendar:rows/diff":                 true,
	"06_calendar:tsdiff/days":               true,
	"06_calendar:tsdiff/inf":                true,
	"06_calendar:tsdiff/mixedNeg":           true,
	"06_calendar:tsdiff/neg":                true,
	"06_calendar:tsdiff/over24":             true,
	"06_calendar:tsdiff/time":               true,
	"06_calendar:tsdiff/us":                 true,
	"06_calendar:tsdiff/year":               true,
	"06_calendar:tz/America/New_York/minus": true,
	"06_calendar:tz/UTC/minus":              true,
	"10_definitions:types/tsMinusTs":        true,
	"10_definitions:types/tstzMinusTs":      true,
	"10_definitions:types/tstzMinusTstz":    true,
	"11_relational:lag/tsDiffBare":          true,
	// a scalar subquery whose slot is an interval.
	"11_relational:sub/scalarPublish": true,
}

// TestIntervalLineageAgreesWithSyntaxOverTheCorpus: over every SELECT of the
// interval corpus PostgreSQL answers, the interim's syntactic classification
// of each result column and the binder's declared lineage are computed side
// by side. Where they agree nothing is asked; where they disagree the cell
// must be listed and lineage must be PostgreSQL's declared type (interval or
// not). And lineage never says interval where PostgreSQL does not.
func TestIntervalLineageAgreesWithSyntaxOverTheCorpus(t *testing.T) {
	cat := ivCorpusCatalog(t)
	cells := ivCorpusCells(t)
	if len(cells) < 300 {
		t.Fatalf("only %d corpus cells read from %s", len(cells), ivCorpusDir)
	}
	disagreed, wrong, missed, compared := ivAgreement(cat, cells)
	for _, w := range wrong {
		t.Error(w)
	}
	for _, m := range missed {
		// Both classifications miss a producer PostgreSQL types interval:
		// the statement is refused for another reason (an unimplemented
		// function, a binder error), recorded in 1680c_r3.md.
		t.Log("not declared by either (refused on another ground): " + m)
	}
	var unlisted, stale []string
	for k := range disagreed {
		if !ivLineageDisagreements[k] {
			unlisted = append(unlisted, k)
		}
	}
	for k := range ivLineageDisagreements {
		if !disagreed[k] {
			stale = append(stale, k)
		}
	}
	sort.Strings(unlisted)
	sort.Strings(stale)
	for _, k := range unlisted {
		t.Errorf("syntax and lineage disagree on %s, which is not listed (ivLineageDisagreements)", k)
	}
	for _, k := range stale {
		t.Errorf("%s is listed as a disagreement but agrees", k)
	}
	t.Logf("%d corpus statements compared, %d disagreements (lineage right on each)", compared, len(disagreed))
}

// ivAgreement computes, over the corpus cells, the disagreements of the two
// classifications, the cases where lineage is wrong against PostgreSQL's
// declared type, and the interval columns neither declares.
func ivAgreement(cat brSchemaCatalog, cells []ivCorpusCell) (disagreed map[string]bool, wrong, missed []string, compared int) {
	disagreed = map[string]bool{}
	for _, c := range cells {
		parsed, err := plansql.Parse(c.sql)
		if err != nil {
			continue // not parsed: outside the program (ruling 2)
		}
		info, err := plansql.ExtractSelect(parsed)
		if err != nil || info == nil {
			continue
		}
		lin, ok := ivLineage(cat, info)
		if !ok || len(lin) != len(c.pgTypes) {
			continue
		}
		compared++
		syn, synOK := ivSyntactic(info)
		for i, l := range lin {
			pgIv := c.pgTypes[i] == "interval"
			if l && !pgIv {
				wrong = append(wrong, fmt.Sprintf("%s column %d: lineage declares interval, PostgreSQL declares %s", c.key, i, c.pgTypes[i]))
			}
			if !l && pgIv && (!synOK || i >= len(syn) || !syn[i]) {
				missed = append(missed, fmt.Sprintf("%s column %d", c.key, i))
			}
			if !synOK || i >= len(syn) || syn[i] == l {
				continue
			}
			disagreed[c.key] = true
			if l != pgIv {
				wrong = append(wrong, fmt.Sprintf("%s column %d: syntax %v, lineage %v, PostgreSQL %s: lineage is wrong", c.key, i, syn[i], l, c.pgTypes[i]))
			}
		}
	}
	return disagreed, wrong, missed, compared
}

// TestIntervalLineageAgreementNegativeControl: with forwarding off at one
// boundary, the agreement over the corpus no longer holds as listed (a
// listed disagreement the boundary carried agrees again).
func TestIntervalLineageAgreementNegativeControl(t *testing.T) {
	cat := ivCorpusCatalog(t)
	cells := ivCorpusCells(t)
	for _, seam := range []string{expr.LineageDerived, expr.LineageSubquery, expr.LineageSetOp} {
		func() {
			restore := expr.DisableIntervalLineageForTest(seam)
			defer restore()
			disagreed, _, _, _ := ivAgreement(cat, cells)
			stale := 0
			for k := range ivLineageDisagreements {
				if !disagreed[k] {
					stale++
				}
			}
			if stale == 0 {
				t.Errorf("forwarding off at %s: the corpus agreement still holds as listed", seam)
			}
			t.Logf("forwarding off at %s: %d listed disagreements no longer seen", seam, stale)
		}()
	}
}
