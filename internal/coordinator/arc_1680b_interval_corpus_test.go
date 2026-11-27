// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// The corpus files (#1680b), one set per section under testdata/arc_1680b/:
//
//	<stem>.statements.tsv  name<TAB>settings<TAB>kind<TAB>sql
//	<stem>_pg17.tsv        name<TAB>PostgreSQL's answer
//	<stem>_engine.tsv      name<TAB>arms<TAB>class<TAB>this engine's answer
//
// An answer is "ok" (DDL), "(0 rows)", "ERR <SQLSTATE>[: message]" or
// "col:type(oid),… | row; row" with each field PostgreSQL's text (NULL for
// a NULL; tab and newline escaped). kind is "sql", "ddl" or "pg-only".
// The engine file's arms is "*" when the six arms answer alike, otherwise
// one line per arm; its class is agree (value and declared types equal
// PostgreSQL's), agree-value (value equal, a declared type differs),
// refused (an error), differs (an answer that is not PostgreSQL's), ok
// (DDL accepted) or pg-only.

const iv80Dir = "testdata/arc_1680b"

func iv80Path(stem, suffix string) string { return filepath.Join(iv80Dir, stem+suffix) }

func iv80Escape(s string) string {
	return strings.NewReplacer("\\", "\\\\", "\t", "\\t", "\n", "\\n").Replace(s)
}

func iv80Kind(c iv80Cell) string {
	switch {
	case c.ddl:
		return "ddl"
	case c.pgOnly:
		return "pg-only"
	}
	return "sql"
}

func iv80Settings(c iv80Cell) string {
	if c.settings == "" {
		return "-"
	}
	return c.settings
}

// iv80StatementsFile is the statements file of a section's cells.
func iv80StatementsFile(cells []iv80Cell) string {
	var b strings.Builder
	b.WriteString("# #1680b interval corpus statements (arc_1680b_interval_corpus_cells_test.go). name<TAB>settings<TAB>kind<TAB>sql\n")
	for _, c := range cells {
		b.WriteString(c.name + "\t" + iv80Settings(c) + "\t" + iv80Kind(c) + "\t" + iv80Escape(c.sql) + "\n")
	}
	return b.String()
}

// iv80ReadTSV reads name -> rest-of-line (first field split off) from one
// corpus file, keeping the order of first appearance.
func iv80ReadTSV(t *testing.T, path string) (map[string][]string, []string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := map[string][]string{}
	var order []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<22), 1<<22)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, rest, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("%s: malformed line %q", path, line)
		}
		if _, seen := out[name]; !seen {
			order = append(order, name)
		}
		out[name] = append(out[name], rest)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out, order
}

// iv80PGTypeName names a PostgreSQL result OID.
func iv80PGTypeName(oid uint32) string {
	switch oid {
	case 1186:
		return "interval"
	case 1114:
		return "timestamp"
	case 1184:
		return "timestamp with time zone"
	case 1082:
		return "date"
	case 1083:
		return "time"
	case 1266:
		return "time with time zone"
	case 1043:
		return "character varying"
	case 1187:
		return "interval[]"
	case 1009:
		return "text[]"
	}
	if n := ftPGTypeName(oid); !strings.HasPrefix(n, "oid") {
		return n
	}
	return "unknown"
}

// TestArc1680bMeasure runs every cell of every section against PostgreSQL
// (IV80_PG_DSN, e.g. postgres://postgres:pg@127.0.0.1:55480/postgres) and
// rewrites each section's statements and answers files; skipped otherwise.
func TestArc1680bMeasure(t *testing.T) {
	dsn := os.Getenv("IV80_PG_DSN")
	if dsn == "" {
		t.Skip("IV80_PG_DSN unset")
	}
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	run := func(sql string) *pgconn.Result {
		return conn.ExecParams(ctx, sql, nil, nil, nil, nil).Read()
	}
	one := func(sql string) string {
		r := run(sql)
		if r.Err != nil || len(r.Rows) != 1 {
			t.Fatalf("%s: %v", sql, r.Err)
		}
		return string(r.Rows[0][0])
	}
	reset := func() {
		for _, s := range []string{"RESET ALL", "SET statement_timeout = '30s'"} {
			if r := run(s); r.Err != nil {
				t.Fatal(r.Err)
			}
		}
	}
	reset()
	for _, tbl := range iv80Tables {
		run("DROP TABLE IF EXISTS " + tbl)
	}
	env := fmt.Sprintf("# %s; datcollate=%s datctype=%s server_encoding=%s; defaults IntervalStyle=%s DateStyle=%s TimeZone=%s; a cell's settings column overrides them for that cell only.\n",
		one("SELECT version()"), one("SELECT datcollate FROM pg_database WHERE datname = current_database()"),
		one("SELECT datctype FROM pg_database WHERE datname = current_database()"), one("SHOW server_encoding"),
		one("SHOW IntervalStyle"), one("SHOW DateStyle"), one("SHOW TimeZone"))
	if err := os.MkdirAll(iv80Dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, sec := range iv80Sections() {
		cells := sec.cells()
		var b strings.Builder
		b.WriteString("# PostgreSQL 17.11 answers for the #1680b interval corpus, section " + sec.stem + " (TestArc1680bMeasure; postgres:17-alpine, initdb --locale=C --encoding=UTF8, --memory=4g).\n")
		b.WriteString(env)
		b.WriteString("# name<TAB>answer: ok | (0 rows) | ERR <SQLSTATE>: message | col:type(oid),... | row; row\n")
		for _, c := range cells {
			reset()
			if c.settings != "" {
				for _, kv := range strings.Split(c.settings, ";") {
					k, v, _ := strings.Cut(kv, "=")
					if r := run("SET " + k + " = '" + v + "'"); r.Err != nil {
						t.Fatalf("%s: SET %s: %v", c.name, kv, r.Err)
					}
				}
			}
			r := run(c.sql)
			var ans string
			switch {
			case r.Err != nil:
				pe, ok := r.Err.(*pgconn.PgError)
				if !ok {
					t.Fatalf("%s: %v", c.name, r.Err)
				}
				ans = "ERR " + pe.Code + ": " + pe.Message
			case c.ddl:
				ans = "ok"
			default:
				header := make([]string, len(r.FieldDescriptions))
				for i, fd := range r.FieldDescriptions {
					header[i] = fmt.Sprintf("%s:%s(%d)", fd.Name, iv80PGTypeName(fd.DataTypeOID), fd.DataTypeOID)
				}
				rows := make([]string, len(r.Rows))
				for i, row := range r.Rows {
					f := make([]string, len(row))
					for j, v := range row {
						if v == nil {
							f[j] = "NULL"
						} else {
							f[j] = string(v)
						}
					}
					rows[i] = strings.Join(f, ",")
				}
				ans = ftRender(header, rows)
			}
			b.WriteString(c.name + "\t" + iv80Escape(ans) + "\n")
		}
		reset()
		if err := os.WriteFile(iv80Path(sec.stem, ".statements.tsv"), []byte(iv80StatementsFile(cells)), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(iv80Path(sec.stem, "_pg17.tsv"), []byte(b.String()), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// iv80PGValue is a PostgreSQL answer reduced to what an engine answer is
// compared against: column names without types, the error without its
// message.
func iv80PGValue(ans string) string {
	if strings.HasPrefix(ans, "ERR ") {
		code, _, _ := strings.Cut(ans, ":")
		return code
	}
	return ivNormalize(ans)
}

// iv80PGTypes is a PostgreSQL header with the OIDs dropped, as ivTypeNames
// spells this engine's.
func iv80PGTypes(ans string) string {
	head := ivHeaderTypes(ans)
	cols := strings.Split(head, ",")
	for i, c := range cols {
		if j := strings.LastIndexByte(c, '('); j > 0 && strings.HasSuffix(c, ")") {
			cols[i] = c[:j]
		}
	}
	return strings.Join(cols, ",")
}

// iv80Classify is the class of one engine answer against PostgreSQL's.
func iv80Classify(engine, pg string) string {
	switch {
	case strings.HasPrefix(engine, "ERR ") || strings.HasPrefix(engine, "PANIC"):
		return "refused"
	case ivNormalize(engine) != iv80PGValue(pg):
		return "differs"
	case strings.Contains(engine, " | ") && ivTypeNames(ivHeaderTypes(engine)) != iv80PGTypes(pg):
		return "agree-value"
	}
	return "agree"
}

// iv80EngineText prints a TIMESTAMP column's values (the harness renders
// the box's epoch milliseconds) as PostgreSQL's text, so the class compares
// the instant and not the rendering. An answer whose rows do not split into
// its header's columns is left as it is.
func iv80EngineText(ans string) string {
	head, body, ok := strings.Cut(ans, " | ")
	if !ok {
		return ans
	}
	cols := strings.Split(head, ",")
	var tsCols []int
	for i, c := range cols {
		if strings.HasSuffix(c, ":TIMESTAMP") {
			tsCols = append(tsCols, i)
		}
	}
	if len(tsCols) == 0 {
		return ans
	}
	rows := strings.Split(body, "; ")
	for r, row := range rows {
		f := strings.Split(row, ",")
		if len(f) != len(cols) {
			return ans
		}
		for _, i := range tsCols {
			ms, err := strconv.ParseInt(f[i], 10, 64)
			if err != nil {
				continue
			}
			f[i] = time.UnixMilli(ms).UTC().Format("2006-01-02 15:04:05.999999")
		}
		rows[r] = strings.Join(f, ",")
	}
	return head + " | " + strings.Join(rows, "; ")
}

// iv80EngineRun runs every cell of every section on every arm and answers
// each section's engine file.
func iv80EngineRun(t *testing.T) map[string]string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	t.Cleanup(cancel)
	writer, arms := ivArmsFixture(t, ctx)
	out := map[string]string{}
	for _, sec := range iv80Sections() {
		pg, _ := iv80ReadTSV(t, iv80Path(sec.stem, "_pg17.tsv"))
		var b strings.Builder
		b.WriteString("# This engine's disposition of the #1680b interval corpus, section " + sec.stem + ", per arm (TestArc1680bRecordEngine; held by TestArc1680bEngineBaseline).\n")
		b.WriteString("# Settings are PostgreSQL's only: the engine runs each statement as written. A TIMESTAMP value is printed as text (iv80EngineText). name<TAB>arms<TAB>class<TAB>answer\n")
		for _, c := range sec.cells() {
			want := ""
			if a, ok := pg[c.name]; ok {
				want = a[0]
			}
			switch {
			case c.pgOnly:
				b.WriteString(c.name + "\t*\tpg-only\t-\n")
				continue
			case c.ddl:
				got := writer(c.sql)
				cls := "ok"
				if got != "ok" {
					cls = "refused"
				}
				b.WriteString(c.name + "\twriter\t" + cls + "\t" + iv80Escape(got) + "\n")
				continue
			}
			answers := make([]string, len(arms))
			same := true
			for i, arm := range arms {
				answers[i] = iv80EngineText(arm.run(c.sql))
				if answers[i] != answers[0] {
					same = false
				}
			}
			if same {
				b.WriteString(c.name + "\t*\t" + iv80Classify(answers[0], want) + "\t" + iv80Escape(answers[0]) + "\n")
				continue
			}
			for i, arm := range arms {
				b.WriteString(c.name + "\t" + arm.name + "\t" + iv80Classify(answers[i], want) + "\t" + iv80Escape(answers[i]) + "\n")
			}
		}
		out[sec.stem] = b.String()
	}
	return out
}

// TestArc1680bRecordEngine rewrites each section's engine file
// (IV80_RECORD_ENGINE=1); skipped otherwise.
func TestArc1680bRecordEngine(t *testing.T) {
	if os.Getenv("IV80_RECORD_ENGINE") == "" {
		t.Skip("IV80_RECORD_ENGINE unset")
	}
	for stem, body := range iv80EngineRun(t) {
		if err := os.WriteFile(iv80Path(stem, "_engine.tsv"), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// TestArc1680bEngineBaseline holds the engine files: every cell's answer on
// every arm is the recorded one. It asserts nothing about agreement with
// PostgreSQL: it is the baseline the implementation items move, re-recorded
// (IV80_RECORD_ENGINE=1) with every moved cell named in the item's note.
func TestArc1680bEngineBaseline(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: six arms over the interval corpus")
	}
	got := iv80EngineRun(t)
	for _, sec := range iv80Sections() {
		raw, err := os.ReadFile(iv80Path(sec.stem, "_engine.tsv"))
		if err != nil {
			t.Fatal(err)
		}
		want := strings.Split(string(raw), "\n")
		have := strings.Split(got[sec.stem], "\n")
		n := len(want)
		if len(have) > n {
			n = len(have)
		}
		moved := 0
		for i := 0; i < n; i++ {
			var w, h string
			if i < len(want) {
				w = want[i]
			}
			if i < len(have) {
				h = have[i]
			}
			if w != h {
				moved++
				if moved <= 20 {
					t.Errorf("%s: the engine's disposition moved\n  recorded %s\n  now      %s", sec.stem, w, h)
				}
			}
		}
		if moved > 20 {
			t.Errorf("%s: %d lines moved in all", sec.stem, moved)
		}
	}
}

// TestArc1680bCorpusWellFormed: every section's statements file is the cells
// as written, every statement has PostgreSQL's answer (typed, for an answer
// with rows) and this engine's disposition on every arm.
func TestArc1680bCorpusWellFormed(t *testing.T) {
	armNames := []string{"single", "spilled512k", "dag", "dag-shuffled", "dag-morsel4", "dag-fastpath"}
	total := 0
	for _, sec := range iv80Sections() {
		cells := sec.cells()
		raw, err := os.ReadFile(iv80Path(sec.stem, ".statements.tsv"))
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != iv80StatementsFile(cells) {
			t.Errorf("%s: the statements file is not the cells as written: re-measure (TestArc1680bMeasure)", sec.stem)
		}
		pg, pgOrder := iv80ReadTSV(t, iv80Path(sec.stem, "_pg17.tsv"))
		eng, _ := iv80ReadTSV(t, iv80Path(sec.stem, "_engine.tsv"))
		seen := map[string]bool{}
		for _, c := range cells {
			total++
			if seen[c.name] {
				t.Errorf("%s: duplicate cell %s", sec.stem, c.name)
			}
			seen[c.name] = true
			a, ok := pg[c.name]
			if !ok || len(a) != 1 {
				t.Errorf("%s: %s has no single PostgreSQL answer", sec.stem, c.name)
				continue
			}
			ans := a[0]
			switch {
			case c.ddl:
				if ans != "ok" {
					t.Errorf("%s: DDL %s failed on PostgreSQL: %s", sec.stem, c.name, ans)
				}
			case ans == "(0 rows)":
			case strings.HasPrefix(ans, "ERR "):
				if len(ans) < 9 {
					t.Errorf("%s: %s: malformed error %q", sec.stem, c.name, ans)
				}
			default:
				head, _, ok := strings.Cut(ans, " | ")
				if !ok {
					t.Errorf("%s: %s: malformed answer %q", sec.stem, c.name, ans)
					continue
				}
				for _, col := range strings.Split(head, ",") {
					if !strings.Contains(col, ":") || !strings.HasSuffix(col, ")") {
						t.Errorf("%s: %s: column %q carries no type OID", sec.stem, c.name, col)
					}
				}
			}
			lines := eng[c.name]
			switch {
			case c.pgOnly || c.ddl:
				if len(lines) != 1 {
					t.Errorf("%s: %s: want one engine line, have %d", sec.stem, c.name, len(lines))
				}
			case len(lines) == 1 && strings.HasPrefix(lines[0], "*\t"):
			default:
				var arms []string
				for _, l := range lines {
					a, _, _ := strings.Cut(l, "\t")
					arms = append(arms, a)
				}
				if strings.Join(arms, ",") != strings.Join(armNames, ",") {
					t.Errorf("%s: %s: engine disposition arms %v, want * or %v", sec.stem, c.name, arms, armNames)
				}
			}
			for _, l := range lines {
				f := strings.SplitN(l, "\t", 3)
				if len(f) != 3 {
					t.Errorf("%s: %s: malformed engine line %q", sec.stem, c.name, l)
					continue
				}
				switch f[1] {
				case "agree", "agree-value", "refused", "differs", "ok", "pg-only":
				default:
					t.Errorf("%s: %s: unknown class %q", sec.stem, c.name, f[1])
				}
			}
		}
		if len(pgOrder) != len(cells) {
			t.Errorf("%s: %d PostgreSQL answers for %d cells", sec.stem, len(pgOrder), len(cells))
		}
	}
	if total < 500 {
		t.Fatalf("only %d cells: the corpus must cover the sections", total)
	}
}

// --- The reference model --------------------------------------------------

// iv80CmpValue is PostgreSQL's interval_cmp_value (src/backend/utils/adt/
// timestamp.c, REL_17_11): (months * 30 + days) * 86400000000 + micros, in
// 128 bits. It is the proposed comparison and hash key; this test checks it
// against PostgreSQL's measured order and grouping of the generated values.
func iv80CmpValue(v iv80ModelValue) *big.Int {
	days := new(big.Int).Mul(big.NewInt(int64(v.months)), big.NewInt(30))
	days.Add(days, big.NewInt(int64(v.days)))
	span := new(big.Int).Mul(days, big.NewInt(86400000000))
	return span.Add(span, big.NewInt(v.micros))
}

// iv80Int64Wrap is the same value computed in wrapping int64 arithmetic: what
// a 64-bit implementation would compare.
func iv80Int64Wrap(v iv80ModelValue) int64 {
	return (int64(v.months)*30+int64(v.days))*86400000000 + v.micros
}

// TestArc1680bReferenceModel: the 128-bit normalized value reproduces
// PostgreSQL's ORDER BY and GROUP BY over the generated values exactly; SQL
// equality implies PostgreSQL's hash equality (measured) and the model key's
// equality (by construction); a hash of the FIELDS would split SQL-equal
// groups, and a 64-bit normalized value would mis-order the values past 2^63.
func TestArc1680bReferenceModel(t *testing.T) {
	vals := iv80ModelValues()
	pg, _ := iv80ReadTSV(t, iv80Path("09_model", "_pg17.tsv"))
	body := func(name string) []string {
		a := pg[name]
		if len(a) != 1 {
			t.Fatalf("no answer for %s", name)
		}
		_, rows, ok := strings.Cut(a[0], " | ")
		if !ok {
			t.Fatalf("%s: %s", name, a[0])
		}
		return strings.Split(rows, "; ")
	}
	// The stored fields are the input's, unnormalized.
	for i, r := range body("model/fields") {
		f := strings.Split(r, ",")
		v := vals[i]
		want := fmt.Sprintf("%d,%d,%d,%d", i+1, v.months, v.days, v.micros)
		got := f[0] + "," + iv80TrimNumeric(f[1]) + "," + iv80TrimNumeric(f[2]) + "," + iv80TrimNumeric(f[3])
		if got != want {
			t.Fatalf("model/fields row %d: PostgreSQL stored %s, generated %s", i+1, got, want)
		}
	}
	// ORDER BY v, id.
	ids := make([]int, len(vals))
	for i := range ids {
		ids[i] = i + 1
	}
	sort.SliceStable(ids, func(a, b int) bool {
		c := iv80CmpValue(vals[ids[a]-1]).Cmp(iv80CmpValue(vals[ids[b]-1]))
		if c != 0 {
			return c < 0
		}
		return ids[a] < ids[b]
	})
	var modelOrder []string
	for _, id := range ids {
		modelOrder = append(modelOrder, strconv.Itoa(id))
	}
	if got := strings.Join(body("model/order"), " "); got != strings.Join(modelOrder, " ") {
		t.Errorf("ORDER BY: PostgreSQL %s\n  model %s", got, strings.Join(modelOrder, " "))
	}
	// The same order under wrapping int64 disagrees (the values past 2^63).
	sort.SliceStable(ids, func(a, b int) bool {
		x, y := iv80Int64Wrap(vals[ids[a]-1]), iv80Int64Wrap(vals[ids[b]-1])
		if x != y {
			return x < y
		}
		return ids[a] < ids[b]
	})
	var wrapOrder []string
	for _, id := range ids {
		wrapOrder = append(wrapOrder, strconv.Itoa(id))
	}
	if strings.Join(wrapOrder, " ") == strings.Join(modelOrder, " ") {
		t.Errorf("the generated values do not discriminate a 64-bit normalized value")
	}
	// GROUP BY: membership is equality of the 128-bit value.
	groups := map[string][]int{}
	for i, v := range vals {
		k := iv80CmpValue(v).String()
		groups[k] = append(groups[k], i+1)
	}
	var modelGroups []string
	for _, g := range groups {
		var s []string
		for _, id := range g {
			s = append(s, strconv.Itoa(id))
		}
		modelGroups = append(modelGroups, fmt.Sprintf("%d,%d,%s", g[0], len(g), strings.Join(s, " ")))
	}
	sort.Slice(modelGroups, func(a, b int) bool {
		x, _ := strconv.Atoi(strings.SplitN(modelGroups[a], ",", 2)[0])
		y, _ := strconv.Atoi(strings.SplitN(modelGroups[b], ",", 2)[0])
		return x < y
	})
	for _, name := range []string{"model/groups", "model/groupsSort"} {
		if got := strings.Join(body(name), "; "); got != strings.Join(modelGroups, "; ") {
			t.Errorf("%s: PostgreSQL %s\n  model %s", name, got, strings.Join(modelGroups, "; "))
		}
	}
	// Hashing: PostgreSQL's interval_hash is equal within every group
	// (measured); a hash of the fields is not.
	hash := map[int]string{}
	for _, r := range body("model/hash") {
		id, h, _ := strings.Cut(r, ",")
		n, _ := strconv.Atoi(id)
		hash[n] = h
	}
	fieldSplit, multi := 0, 0
	for _, g := range groups {
		if len(g) < 2 {
			continue
		}
		multi++
		distinctFields := false
		for _, id := range g[1:] {
			if hash[id] != hash[g[0]] {
				t.Errorf("PostgreSQL hashed SQL-equal ids %d and %d apart", g[0], id)
			}
			if vals[id-1] != vals[g[0]-1] {
				distinctFields = true
			}
		}
		if distinctFields {
			fieldSplit++
		}
	}
	if multi < 20 || fieldSplit < 20 {
		t.Fatalf("%d multi-member groups, %d with members of different fields: the generator must collide", multi, fieldSplit)
	}
	t.Logf("%d values, %d groups, %d multi-member, %d with differing fields; 128-bit model matches PostgreSQL's order and grouping", len(vals), len(groups), multi, fieldSplit)
}

// iv80TrimNumeric drops a numeric's zero fraction ("30.000000" -> "30").
func iv80TrimNumeric(s string) string {
	if i := strings.IndexByte(s, '.'); i >= 0 {
		frac := strings.TrimRight(s[i+1:], "0")
		if frac == "" {
			return s[:i]
		}
		return s[:i] + "." + frac
	}
	return s
}
