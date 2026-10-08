// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A SELECT ITEM THAT IS A GROUP BY KEY IS THE KEY, HOWEVER EITHER IS SPELLED
// (#1524, ADR-0026 §1). PostgreSQL matches a select-list, HAVING or ORDER BY
// expression to a GROUP BY key by comparing the PARSED, RESOLVED trees: `t.i +
// 1` and `i + 1` are one expression once `i` is resolved to `t`'s column, and
// the matched item is published with the KEY's value and declared type. This
// table is that seam enumerated once — spelling difference × key type × key
// shape × consumer × column origin — on five arms against PostgreSQL 17.11's
// rows (in order where the statement orders them) AND the declared type class
// of every result column.
//
// The fixture is arc SS's (ss_t / ss_i, arc_ss_scalar_subquery_type_arms_test.go):
// a NUMERIC key whose text order differs from its numeric order (`20.00`
// against `4.50`) and a NULL row, so a key published as text answers a
// different ORDER BY and a different declared type.

// gkType is one key type: the ss_t column, an arithmetic key over it (%s is
// the column reference), and an expression OVER the key that keeps the type
// (%s is the key) for the "inside a larger expression" consumer.
type gkType struct {
	key, col, arith, larger string
}

func gkTypes() []gkType {
	return []gkType{
		{"int", "i", "%s + 1", "(%s) * 2"},
		{"bigint", "b", "%s + 1", "(%s) * 2"},
		{"numeric", "n", "2 * %s", "(%s) * 2"},
		{"double", "f", "%s * 2", "(%s) * 2"},
		{"text", "s", "%s || 'x'", "upper(%s)"},
		{"date", "d", "%s + 1", "(%s) + 1"},
		{"timestamp", "ts", "%s + INTERVAL '1 hour'", "(%s) + INTERVAL '1 day'"},
		{"bool", "o", "NOT %s", "NOT (%s)"},
		{"array", "i", "ARRAY[%s, 9]", "cardinality(%s)"},
	}
}

// gkCells is the whole table. Every SQL is PostgreSQL's spelling too.
func gkCells() []ssCell {
	var out []ssCell
	add := func(name, sql string) { out = append(out, ssCell{name: name, sql: sql}) }
	addOrd := func(name, sql string) { out = append(out, ssCell{name: name, sql: sql, ordered: true}) }

	// THE SPELLING PAIR × KEY TYPE × CONSUMER. The four spellings of one
	// arithmetic key: the item qualified and the key bare (#1524's own), the
	// key qualified and the item bare (ADR-0012 names-scopes r10), and the two
	// alike as the controls.
	spellings := []struct{ name, item, key string }{
		{"itemQual", "t.", ""},
		{"keyQual", "", "t."},
		{"sameQual", "t.", "t."},
		{"sameBare", "", ""},
	}
	for _, ty := range gkTypes() {
		for _, sp := range spellings {
			it := fmt.Sprintf(ty.arith, sp.item+ty.col)
			ky := fmt.Sprintf(ty.arith, sp.key+ty.col)
			base := "pair/" + ty.key + "/" + sp.name + "/"
			from := " FROM ss_t t GROUP BY " + ky
			add(base+"sel", "SELECT "+it+", count(*)"+from)
			add(base+"alias", "SELECT "+it+" AS k, count(*) AS c"+from)
			addOrd(base+"ordinal", "SELECT "+it+" AS k, count(*) AS c"+from+" ORDER BY 1")
			addOrd(base+"ordinalUnaliased", "SELECT "+it+", count(*)"+from+" ORDER BY 1")
			addOrd(base+"ordWritten", "SELECT "+it+" AS k, count(*) AS c"+from+" ORDER BY "+it)
			addOrd(base+"ordKeySpelling", "SELECT "+it+" AS k, count(*) AS c"+from+" ORDER BY "+ky)
			add(base+"larger", "SELECT "+fmt.Sprintf(ty.larger, it)+" AS k, count(*) AS c"+from)
			add(base+"having", "SELECT "+it+" AS k, count(*) AS c"+from+" HAVING ("+it+") IS NOT NULL")
			add(base+"window", "SELECT "+it+" AS k, rank() OVER (ORDER BY "+it+") AS r"+from)
			add(base+"distinct", "SELECT DISTINCT "+it+" AS k"+from)
			addOrd(base+"derived", "SELECT s.k FROM (SELECT "+it+" AS k, count(*) AS c"+from+") s ORDER BY 1")
			add(base+"union", "SELECT "+it+" AS k"+from+" UNION ALL SELECT "+ky+" FROM ss_t t WHERE t.id = 1")
		}
	}

	// THE SPELLING FAMILY over the INTEGER and the NUMERIC key: every way the
	// item and the key can be spelled apart, each with the bare and the
	// ordered consumer. PostgreSQL's answer decides each one — a match is the
	// key's value and type, a miss is 42803.
	family := []struct{ name, item, key string }{
		{"caseItem", "T.I + 1", "i + 1"},
		{"caseKey", "t.i + 1", "I + 1"},
		{"caseQualOnly", "T.i + 1", "t.i + 1"},
		{"parenItem", "(t.i + 1)", "i + 1"},
		{"parenKey", "t.i + 1", "(i + 1)"},
		{"parenLeaf", "((t.i)) + 1", "i + 1"},
		{"wsItem", "t.i+1", "i  +  1"},
		{"commuted", "1 + t.i", "i + 1"},
		{"commutedBare", "1 + i", "i + 1"},
		{"castTwoWays", "CAST(t.i AS BIGINT)", "i::bigint"},
		{"castTwoWaysRev", "t.i::bigint", "CAST(i AS BIGINT)"},
		{"castSynonym", "CAST(t.i AS INT8)", "CAST(i AS BIGINT)"},
		{"funcCase", "ABS(t.i)", "abs(i)"},
		{"funcCaseKeyQual", "abs(i)", "ABS(t.i)"},
		{"quotedCol", "t.\"i\" + 1", "i + 1"},
		{"quotedQual", "\"t\".\"i\" + 1", "i + 1"},
		{"quotedKey", "t.i + 1", "\"i\" + 1"},
		{"nested", "(t.i + 1) * 2", "(i + 1) * 2"},
		{"subtermOfKey", "t.i", "i + 1"},
		{"constFold", "ABS(-1) * t.n", "1 * n"},
		{"constFoldSame", "ABS(-1) * t.n", "ABS(-1) * n"},
		{"numMulItem", "2 * t.n", "2 * n"},
		{"numMulKey", "2 * n", "2 * t.n"},
		{"numMulCommuted", "t.n * 2", "2 * n"},
		{"numSubItem", "t.n - 4", "n - 4"},
	}
	for _, f := range family {
		base := "spell/" + f.name + "/"
		from := " FROM ss_t t GROUP BY " + f.key
		add(base+"sel", "SELECT "+f.item+", count(*)"+from)
		addOrd(base+"ordinal", "SELECT "+f.item+" AS k, count(*) AS c"+from+" ORDER BY 1")
		addOrd(base+"ordinalUnaliased", "SELECT "+f.item+", count(*)"+from+" ORDER BY 1")
		add(base+"having", "SELECT count(*) AS c"+from+" HAVING ("+f.item+") IS NOT NULL")
	}
	// The table-name qualifier against an aliased relation (PostgreSQL:
	// 42P01, the alias hides the name) and against an unaliased one (a
	// qualifier is spelling).
	add("spell/tableQualAliased/sel", "SELECT ss_t.i + 1, count(*) FROM ss_t t GROUP BY i + 1")
	add("spell/tableQualAliasedKey/sel", "SELECT t.i + 1, count(*) FROM ss_t t GROUP BY ss_t.i + 1")
	add("spell/tableQualNoAlias/sel", "SELECT ss_t.i + 1, count(*) FROM ss_t GROUP BY i + 1")
	addOrd("spell/tableQualNoAlias/ordinal", "SELECT ss_t.n * 2 AS k, count(*) AS c FROM ss_t GROUP BY n * 2 ORDER BY 1")
	add("spell/tableQualNoAliasKey/sel", "SELECT i + 1, count(*) FROM ss_t GROUP BY ss_t.i + 1")
	addOrd("spell/tableQualNoAliasKey/ordinal", "SELECT n * 2 AS k, count(*) AS c FROM ss_t GROUP BY ss_t.n * 2 ORDER BY 1")
	// GROUP BY an ordinal and an output alias: the key IS the item, however
	// it is spelled, and a second spelling of it elsewhere is matched to it.
	addOrd("spell/groupOrdinal/ordinal", "SELECT 2 * t.n AS k, count(*) AS c FROM ss_t t GROUP BY 1 ORDER BY 1")
	addOrd("spell/groupOrdinal/ordWritten", "SELECT 2 * t.n AS k, count(*) AS c FROM ss_t t GROUP BY 1 ORDER BY 2 * n")
	add("spell/groupOrdinal/having", "SELECT 2 * t.n AS k, count(*) AS c FROM ss_t t GROUP BY 1 HAVING 2 * n > 0")
	addOrd("spell/groupAlias/ordinal", "SELECT 2 * t.n AS k, count(*) AS c FROM ss_t t GROUP BY k ORDER BY 1")
	addOrd("spell/groupAlias/ordWritten", "SELECT 2 * t.n AS k, count(*) AS c FROM ss_t t GROUP BY k ORDER BY 2 * n")
	add("spell/groupAlias/having", "SELECT 2 * t.n AS k, count(*) AS c FROM ss_t t GROUP BY k HAVING 2 * n > 0")
	add("spell/groupAliasBareItem/sel", "SELECT 2 * n AS k, 2 * t.n AS k2, count(*) AS c FROM ss_t t GROUP BY k")

	// THE KEY SHAPE × the two directions × the bare and ordered consumers.
	shapes := []struct{ name, tmpl string }{
		{"bareCol", "%si"},
		{"bareColNum", "%sn"},
		{"funcAbs", "abs(%si)"},
		{"funcUpper", "upper(%ss)"},
		{"funcRound", "round(%sn, 1)"},
		{"funcExtract", "extract(year FROM %sd)"},
		{"funcDateTrunc", "date_trunc('day', %sts)"},
		{"funcCoalesce", "coalesce(%si, 0)"},
		{"caseText", "CASE WHEN %si > 2 THEN 'big' ELSE 'small' END"},
		{"caseNum", "CASE WHEN %sn > 1 THEN %sn ELSE 0 END"},
		{"castBigint", "CAST(%si AS BIGINT)"},
		{"castText", "CAST(%sn AS TEXT)"},
		{"castNumeric", "CAST(%si AS NUMERIC(12,3))"},
		{"castDouble", "CAST(%sn AS DOUBLE PRECISION)"},
		{"boolCmp", "%si > 2"},
	}
	for _, sh := range shapes {
		for _, dir := range []struct{ name, item, key string }{{"itemQual", "t.", ""}, {"keyQual", "", "t."}, {"sameQual", "t.", "t."}} {
			it := strings.ReplaceAll(sh.tmpl, "%s", dir.item)
			ky := strings.ReplaceAll(sh.tmpl, "%s", dir.key)
			base := "shape/" + sh.name + "/" + dir.name + "/"
			from := " FROM ss_t t GROUP BY " + ky
			add(base+"sel", "SELECT "+it+", count(*)"+from)
			addOrd(base+"ordinal", "SELECT "+it+" AS k, count(*) AS c"+from+" ORDER BY 1")
		}
	}
	// A multi-column key list and the grouping-set spellings.
	for _, dir := range []struct{ name, item, key string }{{"itemQual", "t.", ""}, {"keyQual", "", "t."}} {
		q, k := dir.item, dir.key
		base := "multi/" + dir.name + "/"
		addOrd(base+"twoKeys", "SELECT "+q+"i + 1 AS k, "+q+"s AS s, count(*) AS c FROM ss_t t GROUP BY "+k+"i + 1, "+k+"s ORDER BY 1, 2")
		addOrd(base+"twoKeysRev", "SELECT 2 * "+q+"n AS k, "+q+"o AS o, count(*) AS c FROM ss_t t GROUP BY "+k+"o, 2 * "+k+"n ORDER BY 1, 2")
		addOrd(base+"mixed", "SELECT 2 * "+q+"n AS k, 2 * t.n AS k2, count(*) AS c FROM ss_t t GROUP BY 2 * "+k+"n ORDER BY 1")
		add(base+"rollup", "SELECT 2 * "+q+"n AS k, count(*) AS c FROM ss_t t GROUP BY ROLLUP (2 * "+k+"n)")
		add(base+"groupingSets", "SELECT "+q+"i + 1 AS k, "+q+"s AS s, count(*) AS c FROM ss_t t GROUP BY GROUPING SETS ((("+k+"i + 1)), ("+k+"s))")
		add(base+"cube", "SELECT "+q+"i + 1 AS k, count(*) AS c FROM ss_t t GROUP BY CUBE ("+k+"i + 1)")
	}

	// THE COLUMN ORIGIN × the two directions × the bare and ordered consumers.
	origins := []struct{ name, from, q, item, key string }{
		{"derivedStar", "(SELECT * FROM ss_t) t", "t.", "2 * %sn", "2 * %sn"},
		{"derivedRenamed", "(SELECT i AS j, n AS m FROM ss_t) t", "t.", "2 * %sm", "2 * %sm"},
		{"cte", "w", "w.", "2 * %sn", "2 * %sn"},
		{"cteAliased", "w x", "x.", "2 * %sn", "2 * %sn"},
		{"noAlias", "ss_t", "ss_t.", "2 * %sn", "2 * %sn"},
		{"joinOneSide", "ss_t t LEFT JOIN ss_i x ON x.id = t.id", "t.", "2 * %sn", "2 * %sn"},
		{"joinOtherSide", "ss_t t LEFT JOIN ss_i x ON x.id = t.id", "x.", "2 * %sm", "2 * %sm"},
		{"joinOneSideInt", "ss_t t LEFT JOIN ss_i x ON x.id = t.id", "t.", "%si + 1", "%si + 1"},
	}
	for _, o := range origins {
		with := ""
		if strings.HasPrefix(o.name, "cte") {
			with = "WITH w AS (SELECT * FROM ss_t) "
		}
		for _, dir := range []struct {
			name        string
			itemQ, keyQ bool
		}{{"itemQual", true, false}, {"keyQual", false, true}, {"sameQual", true, true}} {
			iq, kq := "", ""
			if dir.itemQ {
				iq = o.q
			}
			if dir.keyQ {
				kq = o.q
			}
			it := strings.ReplaceAll(o.item, "%s", iq)
			ky := strings.ReplaceAll(o.key, "%s", kq)
			base := "origin/" + o.name + "/" + dir.name + "/"
			from := " FROM " + o.from + " GROUP BY " + ky
			add(base+"sel", with+"SELECT "+it+", count(*)"+from)
			addOrd(base+"ordinal", with+"SELECT "+it+" AS k, count(*) AS c"+from+" ORDER BY 1")
			add(base+"having", with+"SELECT count(*) AS c"+from+" HAVING "+it+" > 0")
		}
	}
	// A JOIN whose two sides both carry the column: an unqualified reference
	// is ambiguous (PostgreSQL 42702), and the OTHER side's column is a
	// different expression (42803) — the identity must not erase a qualifier
	// that names a relation.
	j := " FROM ss_t a JOIN ss_t b ON a.id = b.id"
	add("origin/joinBoth/bareKey/sel", "SELECT a.i + 1, count(*)"+j+" GROUP BY i + 1")
	add("origin/joinBoth/bareItem/sel", "SELECT i + 1, count(*)"+j+" GROUP BY a.i + 1")
	add("origin/joinBoth/otherSide/sel", "SELECT b.i + 1, count(*)"+j+" GROUP BY a.i + 1")
	add("origin/joinBoth/otherSideHaving/sel", "SELECT count(*)"+j+" GROUP BY a.i + 1 HAVING b.i + 1 > 0")
	add("origin/joinBoth/otherSideNum/sel", "SELECT 2 * b.n, count(*)"+j+" GROUP BY 2 * a.n")
	addOrd("origin/joinBoth/sameSide/ordinal", "SELECT 2 * a.n AS k, count(*) AS c"+j+" GROUP BY 2 * a.n ORDER BY 1")
	addOrd("origin/joinBoth/sameSideCase/ordinal", "SELECT 2 * A.n AS k, count(*) AS c"+j+" GROUP BY 2 * a.N ORDER BY 1")
	add("origin/joinBoth/twoKeys/sel", "SELECT a.i + 1 AS x, b.i + 1 AS y, count(*)"+j+" GROUP BY b.i + 1, a.i + 1")
	// The UNION arm's declaration with NO grouping, the control that says
	// whether a grouped arm's DATE / TIMESTAMP / ARRAY failure is the key's.
	add("ctl/unionNoGroup/date", "SELECT t.d + 1 AS k FROM ss_t t UNION ALL SELECT t.d + 1 FROM ss_t t WHERE t.id = 1")
	add("ctl/unionNoGroup/timestamp", "SELECT t.ts + INTERVAL '1 hour' AS k FROM ss_t t UNION ALL SELECT t.ts + INTERVAL '1 hour' FROM ss_t t WHERE t.id = 1")
	add("ctl/unionNoGroup/array", "SELECT ARRAY[t.i, 9] AS k FROM ss_t t UNION ALL SELECT ARRAY[t.i, 9] FROM ss_t t WHERE t.id = 1")
	add("ctl/unionGroupedBoth/date", "SELECT t.d + 1 AS k FROM ss_t t GROUP BY t.d + 1 UNION ALL SELECT t.d + 1 FROM ss_t t GROUP BY t.d + 1")

	// AN AGGREGATE ALIASED LIKE THE KEY'S COLUMN — `count(*) AS n` beside a
	// key over the column `n` — is a different defect from this seam (the
	// HAVING check admits a qualified `t.n` through the output alias, and the
	// stage DAG binds the key's `n` to the aggregate output): measured here
	// with the item and key spelled ALIKE so the cells name it without a
	// spelling difference, and kept as a filing candidate.
	addOrd("collide/keyMul/ordinal", "SELECT 2 * n AS k, count(*) AS n FROM ss_t t GROUP BY 2 * n ORDER BY 1")
	addOrd("collide/derived/ordinal", "SELECT s.k FROM (SELECT 2 * n AS k, count(*) AS n FROM ss_t t GROUP BY 2 * n) s ORDER BY 1")
	add("collide/having/sel", "SELECT 2 * n AS k, count(*) AS n FROM ss_t t GROUP BY 2 * n HAVING 2 * n > 0")
	add("collide/havingQual/sel", "SELECT count(*) AS n FROM ss_t t GROUP BY 1 * n HAVING (ABS(-1) * t.n) IS NOT NULL")
	addOrd("collide/renamed/ordinal", "SELECT 2 * t.m AS k, count(*) AS n FROM (SELECT i AS j, n AS m FROM ss_t) t GROUP BY 2 * t.m ORDER BY 1")
	addOrd("collide/twoKeys/ordinal", "SELECT 2 * n AS k, o AS o, count(*) AS n FROM ss_t t GROUP BY o, 2 * n ORDER BY 1, 2")

	// A correlated OUTER column is a different relation from an inner one of
	// the same name: `o.n` is not the inner block's `n`, however the inner
	// block's FROM is written, and a qualifier naming the outer relation is
	// never spelling.
	addOrd("origin/outerSameName/item", "SELECT o.id, (SELECT 2 * o.n FROM ss_t x WHERE x.id = 1 GROUP BY 2 * n) AS y FROM ss_t o ORDER BY o.id")
	addOrd("origin/outerSameName/itemSameQual", "SELECT o.id, (SELECT 2 * o.n FROM ss_t x WHERE x.id = 1 GROUP BY 2 * x.n) AS y FROM ss_t o ORDER BY o.id")
	addOrd("origin/outerSameName/having", "SELECT o.id, (SELECT count(*) FROM ss_t x GROUP BY 2 * n HAVING 2 * o.n = 2 * x.n) AS y FROM ss_t o ORDER BY o.id")
	addOrd("origin/outerSameName/key", "SELECT o.id, (SELECT 2 * n FROM ss_t x WHERE x.id = 1 GROUP BY 2 * o.n) AS y FROM ss_t o ORDER BY o.id")
	add("origin/outerRef/sel", "SELECT o.id, (SELECT max(x.v + 1) FROM ss_i x GROUP BY x.v + 1 HAVING v + 1 > o.i ORDER BY 1 LIMIT 1) AS y FROM ss_t o")
	return out
}

func gkPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	return gkReadTSV(t, "testdata/arc_gk_group_key_spelling_pg17.tsv", 2)
}

func gkReadTSV(t *testing.T, path string, fields int) map[string]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, rest, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("malformed line %q in %s", line, path)
		}
		out[name] = rest
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestArcGKGenerate dumps the cells as name<TAB>ordered<TAB>sql for the oracle
// run (GK_GEN=<path>), and the PostgreSQL fixture beside it.
func TestArcGKGenerate(t *testing.T) {
	path := os.Getenv("GK_GEN")
	if path == "" {
		t.Skip("GK_GEN unset")
	}
	var b strings.Builder
	seen := map[string]bool{}
	for _, c := range gkCells() {
		if seen[c.name] {
			t.Fatalf("duplicate cell %s", c.name)
		}
		seen[c.name] = true
		fmt.Fprintf(&b, "%s\t%t\t%s\n", c.name, c.ordered, c.sql)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".fixture.sql", []byte(ssPGFixture()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// gkMoreCells is the table's second half: the probe cells, the join / path /
// origin cells, the GK-F4 cells and the ADR-0047 stage-1 join cells (testdata/arc_gk_group_key_spelling_more_cells.tsv), each measured on
// PostgreSQL 17.11 (…_more_pg17.tsv).
func gkMoreCells(t *testing.T) []ssCell {
	t.Helper()
	var out []ssCell
	for name, rest := range gkReadTSV(t, "testdata/arc_gk_group_key_spelling_more_cells.tsv", 3) {
		ordered, sql, ok := strings.Cut(rest, "\t")
		if !ok {
			t.Fatalf("malformed cell %s", name)
		}
		out = append(out, ssCell{name: name, ordered: ordered == "true", sql: sql})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// gkKeep is one kept line: the arms it holds on — `single` (the two
// single-process arms), `dag` (the three stage-DAG arms), one DAG arm by name,
// `all`, or `skip` (an answer the statement leaves to the engine, ADR-0013) —
// this engine's answer, and why.
type gkKeep struct{ arms, want, why string }

// gkSQLState separates the refusal class from its diagnostic text. An empty
// engine SQLSTATE is recorded explicitly; an answered query is 00000.
func gkSQLState(answer string) string {
	if rest, ok := strings.CutPrefix(answer, "ERR "); ok {
		state, _, _ := strings.Cut(rest, " ")
		if state == "" {
			return "unset"
		}
		return state
	}
	return "00000"
}

func gkBaseStates(t *testing.T) map[string]map[string]string {
	t.Helper()
	out := map[string]map[string]string{}
	arms := []string{"single", "spilled512k", "dag", "dag-shuffled", "dag-morsel4"}
	for name, rest := range gkReadTSV(t, "testdata/arc_ci1_base_sqlstates.tsv", 6) {
		states := strings.Split(rest, "\t")
		if len(states) != len(arms) {
			t.Fatalf("base SQLSTATE row %s has %d arms", name, len(states))
		}
		out[name] = map[string]string{}
		for i, arm := range arms {
			out[name][arm] = states[i]
		}
	}
	return out
}

func (k gkKeep) holdsOn(arm string) bool {
	dag := strings.HasPrefix(arm, "dag")
	switch k.arms {
	case "all", "skip":
		return true
	case "single":
		return !dag
	case "dag":
		return dag
	}
	return k.arms == arm
}

func gkKeptLines(t *testing.T) map[string][]gkKeep {
	t.Helper()
	f, err := os.Open("testdata/arc_gk_group_key_spelling_kept.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string][]gkKeep{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.SplitN(line, "\t", 4)
		if len(p) != 4 {
			t.Fatalf("malformed kept line %q", line)
		}
		switch p[1] {
		case "all", "single", "dag", "skip", "dag-shuffled", "dag-morsel4":
		default:
			t.Fatalf("kept line for %s: arms %q", p[0], p[1])
		}
		out[p[0]] = append(out[p[0]], gkKeep{arms: p[1], want: p[2], why: p[3]})
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestArcGKGroupKeySpellingEveryArm is the seam table on five arms (ADR-0047
// stage 1, #1524): generated cells (gkCells) and measured probes
// (gkMoreCells) — a select item, HAVING, ORDER BY, window or GROUPING term
// spelled apart from its GROUP BY key, over one relation, over a join, through
// derived tables, CTEs, set operations, subqueries and LATERAL bodies.
//
// The single-process arms (single, spilled512k) answer PostgreSQL 17.11 unless
// a kept line says otherwise; every such line is base-identical or the
// ALIKE spelling's own catalogued answer. The three stage-DAG arms are PINNED
// to the base's answer (e0f973e1) wherever that differs from PostgreSQL: the
// coordinator does not stamp the AST it plans (ADR-0047, Stages), so the DAG still
// matches by spelling, and stage 5 brings the binding to it. A pin that
// starts agreeing with PostgreSQL FAILS here and is deleted as the proof.
func TestArcGKGroupKeySpellingEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the group-key spelling table")
	}
	answers := gkPGAnswers(t)
	for name, a := range gkReadTSV(t, "testdata/arc_gk_group_key_spelling_more_pg17.tsv", 2) {
		answers[name] = a
	}
	cells := append(gkCells(), gkMoreCells(t)...)
	seen := map[string]bool{}
	for _, c := range cells {
		if seen[c.name] {
			t.Fatalf("duplicate cell %s", c.name)
		}
		seen[c.name] = true
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
	}
	kept := gkKeptLines(t)
	baseStates := gkBaseStates(t)
	for name, ks := range kept {
		if !seen[name] {
			t.Fatalf("kept cell %s is not in the table", name)
		}
		for _, k := range ks {
			if k.arms != "skip" && ssMatches(k.want, answers[name]) {
				t.Fatalf("kept line for %s (%s) answers PostgreSQL 17.11: a pin that agrees is not a pin — delete it", name, k.arms)
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := ssArms(t, ctx)
	var dump *os.File
	if p := os.Getenv("GK_DUMP"); p != "" {
		f, err := os.Create(p)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		dump = f
	}
	var dumpMu sync.Mutex
	for _, tc := range cells {
		pgWant := answers[tc.name]
		ks := kept[tc.name]
		t.Run(tc.name, func(t *testing.T) {
			got := make([]string, len(arms))
			var wg sync.WaitGroup
			for i, arm := range arms {
				wg.Add(1)
				go func() {
					defer wg.Done()
					res, err := arm.run(tc.sql, tc.ordered)
					if err != nil {
						res = "ERR " + sqlerr.StateOf(err) + " " + strings.ReplaceAll(err.Error(), "\n", " ")
					}
					got[i] = res
				}()
			}
			wg.Wait()
			if dump != nil {
				dumpMu.Lock()
				for i, arm := range arms {
					fmt.Fprintf(dump, "%s\t%s\t%s\n", tc.name, arm.name, got[i])
				}
				dumpMu.Unlock()
			}
			for i, arm := range arms {
				want, why := pgWant, "PostgreSQL 17.11"
				skip := false
				baseState, measured := baseStates[tc.name][arm.name]
				if !measured {
					t.Fatalf("%s/%s has no measured base SQLSTATE", tc.name, arm.name)
				}
				for _, k := range ks {
					if k.holdsOn(arm.name) {
						want, why, skip = k.want, "kept: "+k.why, k.arms == "skip"
						if strings.Contains(k.why, "base-identical") && gkSQLState(got[i]) != baseState {
							t.Errorf("%s/%s: base-identical label has SQLSTATE %s, base %s", tc.name, arm.name, gkSQLState(got[i]), baseState)
						}
					}
				}
				if strings.HasPrefix(arm.name, "dag") && gkSQLState(got[i]) != baseState {
					t.Errorf("%s/%s: DAG SQLSTATE %s, base %s", tc.name, arm.name, gkSQLState(got[i]), baseState)
				}
				if baseState == gkSQLState(pgWant) && gkSQLState(got[i]) != baseState {
					t.Errorf("%s/%s: base matched PostgreSQL SQLSTATE %s, got %s", tc.name, arm.name, baseState, gkSQLState(got[i]))
				}
				if skip {
					continue
				}
				if !ssMatches(got[i], want) {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (%s)", tc.sql, arm.name, got[i], want, why)
				}
			}
		})
	}
}
