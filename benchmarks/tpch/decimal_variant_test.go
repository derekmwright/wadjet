package tpch

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/wadjet"
)

// The DECIMAL(15,2) correctness gate (ADR-0024).
//
// TestTPCHQueriesDecimal is TestDuckDBCompare's sibling over the fixture the
// TPC-H specification actually declares: the same dbgen SF0.01 rows, with the
// eight monetary columns kept as DuckDB's native DECIMAL(15,2) instead of cast
// to DOUBLE. Everything else — the query text, the two arms, the provenance
// rule that no expectation may come from Wadjet — is the float gate's.
//
// What it adds is EXACTNESS. The float gate compares through a digest that
// quantizes to six significant digits, because two correct engines summing
// float64 in different orders legitimately disagree past that. Nothing about
// a DECIMAL answer is approximate: `SUM(l_extendedprice)` is 3027140810.74
// and no accumulation order makes it anything else. So a column whose answer
// is decimal on both sides is compared DIGIT FOR DIGIT, at the scale
// decimalOutputTypes records — which is min(wadjet's scale, DuckDB's scale)
// where the two engines' type rules keep a different number of them
// (ADR-0012 item 9, ADR-0024 item 3).

const (
	duckdbDecimalDataDir      = "duckdb-data-decimal"                // committed: DECIMAL(15,2) dbgen SF0.01
	duckdbDecimalBaselineFile = "baseline-duckdb-decimal-sf001.json" // stored DuckDB-output fingerprints
	regenDecimalBaselineEnv   = "WADJET_REGENERATE_DECIMAL_BASELINE"
)

// decimalCol is one output column of one query whose answer is DECIMAL on at
// least one side, with both engines' DECLARATIONS and the scale the two are
// compared at.
//
// This table is a deliverable in its own right: ADR-0024 item 3 computes a
// result's (p,s) from its operands, and until this fixture existed no query in
// the tree exercised the rule. It is verified in BOTH directions —
// TestTPCHDecimalDeclaredTypes asserts wadjet declares `wadjet` and that no
// DECIMAL output column is missing from the table, and duckdbDecimalEntry
// asserts DuckDB declares `duckdb` when the baseline is regenerated.
type decimalCol struct {
	query  string // Q01 … Q22
	column string
	wadjet string // wadjet's declared type
	duckdb string // DuckDB's declared type (DESCRIBE over the same fixture)
	// scale is the number of fraction digits BOTH sides are held to. It is
	// each engine's declared scale where they agree, and min(scale) where
	// they do not — the rule ADR-0012 item 9 already accepts for AVG: two
	// engines exact to the digits they keep agree to the shorter of the two.
	scale int
	// why is set only where the two declarations differ, and says which
	// record settles it.
	why string
	// wireTypmod is what RowDescription must carry: "(15,2)" for a BARE
	// column reference, whose typmod PostgreSQL keeps, and "-1" for anything
	// computed — an aggregate, an operator, a CAST — which PostgreSQL
	// declares unconstrained (ADR-0024 item 5, verified against 17.11's
	// \gdesc). This is the wire half of the same table: a right value under
	// a wrong declaration is what a JDBC client sizes its column from.
	wireTypmod string
	// typePin, when set, records a declaration wadjet gets WRONG today and
	// names the issue. The row is still checked — the assertion FAILS if it
	// starts agreeing — so deleting the pin is the fix's proof.
	typePin string
}

const (
	typmodBare      = "(15,2)"
	typmodUnconstrd = "-1"
)

// pgNumericDivision is the reason wadjet's decimal `/` and AVG stay DECIMAL
// where DuckDB's go to DOUBLE. PostgreSQL is the semantics authority
// (ADR-0012): `numeric / numeric` and `avg(numeric)` are numeric there, so
// wadjet's answer is the conformant one and DuckDB's is the dialect
// divergence. Both engines are exact to the digits they keep.
const pgNumericDivision = "PostgreSQL keeps numeric (ADR-0012 rule 1); DuckDB's decimal division and " +
	"AVG return DOUBLE. Compared at wadjet's scale, which is the shorter of the two renderings."

// decimalOutputTypes lists every DECIMAL-answering output column of the 22
// queries under the DECIMAL(15,2) fixture. Columns not listed here are
// integer, string or float on both sides and are compared as the float gate
// compares them.
var decimalOutputTypes = []decimalCol{
	// Q01 — the rule's showcase. sum_base_price is SUM over the bare
	// column; sum_disc_price adds one multiplication; sum_charge adds a
	// second, and it is the one that trips ADR-0024 item 3's p>38 clause:
	// (15,2)*(16,2) is (32,4), times (16,2) again is (49,6), reduced to
	// (38,6) by giving up integer digits down to the fraction's floor.
	{query: "Q01", column: "sum_base_price", wadjet: "DECIMAL(38,2)", duckdb: "DECIMAL(38,2)", scale: 2, wireTypmod: typmodUnconstrd},
	{query: "Q01", column: "sum_disc_price", wadjet: "DECIMAL(38,4)", duckdb: "DECIMAL(38,4)", scale: 4, wireTypmod: typmodUnconstrd},
	{query: "Q01", column: "sum_charge", wadjet: "DECIMAL(38,6)", duckdb: "DECIMAL(38,6)", scale: 6, wireTypmod: typmodUnconstrd},
	{query: "Q01", column: "avg_price", wadjet: "DECIMAL(38,6)", duckdb: "DOUBLE", scale: 6, why: pgNumericDivision, wireTypmod: typmodUnconstrd},
	{query: "Q01", column: "avg_disc", wadjet: "DECIMAL(38,6)", duckdb: "DOUBLE", scale: 6, why: pgNumericDivision, wireTypmod: typmodUnconstrd},
	// Q02 — a BARE column reference through a five-way join, an ORDER BY
	// and a correlated scalar subquery. The declaration must survive all of
	// it: (15,2) as a type AND on the wire, because PostgreSQL keeps a bare
	// column's typmod (ADR-0024 item 5).
	{query: "Q02", column: "s_acctbal", wadjet: "DECIMAL(15,2)", duckdb: "DECIMAL(15,2)", scale: 2, wireTypmod: typmodBare,
		typePin: "#697 — a subquery anywhere in the statement drops the typmod of every BARE DECIMAL " +
			"output column. Q10's c_acctbal, the same kind of column with no subquery in the statement, " +
			"keeps it. Values are right; a JDBC client reading getPrecision()/getScale() gets 0."},
	// Q03..Q19 — SUM(l_extendedprice * (1 - l_discount)), the TPC-H revenue
	// expression, is DECIMAL(38,4) in both engines everywhere it appears.
	{query: "Q03", column: "revenue", wadjet: "DECIMAL(38,4)", duckdb: "DECIMAL(38,4)", scale: 4, wireTypmod: typmodUnconstrd},
	{query: "Q05", column: "revenue", wadjet: "DECIMAL(38,4)", duckdb: "DECIMAL(38,4)", scale: 4, wireTypmod: typmodUnconstrd},
	{query: "Q06", column: "revenue", wadjet: "DECIMAL(38,4)", duckdb: "DECIMAL(38,4)", scale: 4, wireTypmod: typmodUnconstrd},
	{query: "Q07", column: "revenue", wadjet: "DECIMAL(38,4)", duckdb: "DECIMAL(38,4)", scale: 4, wireTypmod: typmodUnconstrd},
	// Q08 — the silent half of #695. No row takes the CASE's decimal branch
	// at SF0.01, so nothing is ever written to the mistyped vector and the
	// query ANSWERS — under FLOAT64, where both other engines say numeric.
	{query: "Q08", column: "brazil_revenue", wadjet: "DECIMAL(38,4)", duckdb: "DECIMAL(38,4)", scale: 4, wireTypmod: typmodUnconstrd,
		typePin: "#695 — the CASE's numeric-literal ELSE branch wins the type fold, so this SUM is " +
			"declared FLOAT64. Q14 is the same defect where the decimal branch fires and it errors."},
	{query: "Q08", column: "total_revenue", wadjet: "DECIMAL(38,4)", duckdb: "DECIMAL(38,4)", scale: 4, wireTypmod: typmodUnconstrd},
	{query: "Q10", column: "revenue", wadjet: "DECIMAL(38,4)", duckdb: "DECIMAL(38,4)", scale: 4, wireTypmod: typmodUnconstrd},
	// Q10's c_acctbal is the control for #697: a bare DECIMAL column in a
	// statement with no subquery, and it keeps its typmod.
	{query: "Q10", column: "c_acctbal", wadjet: "DECIMAL(15,2)", duckdb: "DECIMAL(15,2)", scale: 2, wireTypmod: typmodBare},
	// Q11 — SUM(DECIMAL(15,2) * INT32). An integer is DECIMAL(10,0), so
	// the product is (26,2) and the SUM (38,2).
	{query: "Q11", column: "value", wadjet: "DECIMAL(38,2)", duckdb: "DECIMAL(38,2)", scale: 2, wireTypmod: typmodUnconstrd},
	{query: "Q14", column: "promo_revenue", wadjet: "DECIMAL(38,4)", duckdb: "DECIMAL(38,4)", scale: 4, wireTypmod: typmodUnconstrd},
	{query: "Q14", column: "total_revenue", wadjet: "DECIMAL(38,4)", duckdb: "DECIMAL(38,4)", scale: 4, wireTypmod: typmodUnconstrd},
	{query: "Q15", column: "total_revenue", wadjet: "DECIMAL(38,4)", duckdb: "DECIMAL(38,4)", scale: 4, wireTypmod: typmodUnconstrd},
	// Q17 — SUM(l_extendedprice) / 7.0. Division's scale rule is
	// max(6, s1+p2+1) = 6, and the precision reduction pins it at (38,6).
	{query: "Q17", column: "avg_yearly", wadjet: "DECIMAL(38,6)", duckdb: "DOUBLE", scale: 6, why: pgNumericDivision, wireTypmod: typmodUnconstrd},
	{query: "Q18", column: "o_totalprice", wadjet: "DECIMAL(15,2)", duckdb: "DECIMAL(15,2)", scale: 2, wireTypmod: typmodBare,
		typePin: "#697 — same as Q02, reached through `IN (SELECT … HAVING …)` instead of a correlated " +
			"scalar subquery."},
	{query: "Q19", column: "revenue", wadjet: "DECIMAL(38,4)", duckdb: "DECIMAL(38,4)", scale: 4, wireTypmod: typmodUnconstrd},
	{query: "Q22", column: "totacctbal", wadjet: "DECIMAL(38,2)", duckdb: "DECIMAL(38,2)", scale: 2, wireTypmod: typmodUnconstrd},
}

// decimalTierPastSF001 reports whether the PostgreSQL oracle is running the
// DECIMAL fixture at a tier larger than the committed SF0.01 one. Two
// divergences are DATA-dependent rather than query-dependent, so a pin has to
// say which tier it expects or it fails on the other:
//
//   - #695 on Q08. At SF0.01 its `CASE WHEN n2.n_name = 'BRAZIL' … END` takes
//     no row down the DECIMAL branch, so nothing is written to the mistyped
//     vector and the query ANSWERS under a wrong declared type. Past it rows
//     do, and the #361 silent-write guard raises instead — one defect, two
//     symptoms.
//   - The digits-kept divergence on Q17 (ADR-0012 item 9, deliberate).
//     `SUM(l_extendedprice) / 7.0` happens to terminate inside the six
//     fraction digits wadjet keeps at SF0.01, so the two engines agree
//     exactly; at SF0.1 the quotient repeats and PostgreSQL's unbounded
//     numeric keeps more of it.
func decimalTierPastSF001() bool {
	if FixtureFromEnv() != DecimalFixture {
		return false
	}
	raw := strings.TrimSpace(os.Getenv(postgresScaleEnv))
	if raw == "" {
		return false
	}
	f, err := strconv.ParseFloat(raw, 64)
	return err == nil && ScaleFactor(f) > SF001
}

// decimalScales indexes decimalOutputTypes by the query's BASE name (Q01),
// which is what both the _nolimit variant and the verbatim entry share.
func decimalScales(caseName string) map[string]int {
	base := strings.TrimSuffix(caseName, "_nolimit")
	out := map[string]int{}
	for _, d := range decimalOutputTypes {
		if d.query == base {
			out[d.column] = d.scale
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// The gate
// ---------------------------------------------------------------------------

// TestTPCHQueriesDecimal runs the 22 TPC-H queries on BOTH execution paths
// over the DECIMAL(15,2) fixture and holds each answer against DuckDB's,
// exactly where the answer is decimal.
//
//	go test -run TestTPCHQueriesDecimal ./benchmarks/tpch/           # the gate
//	WADJET_DUCKDB_COMPARE=1 …                                       # + live DuckDB, cell diffs
//	WADJET_REGENERATE_DECIMAL_BASELINE=1 …                          # rewrite the baseline
func TestTPCHQueriesDecimal(t *testing.T) {
	if testing.Short() {
		t.Skip("decimal gate stands up a three-worker cluster")
	}
	ctx := context.Background()
	corpus := decimalCorpus()

	if os.Getenv(regenDecimalBaselineEnv) == "1" {
		regenerateDecimalBaseline(t, corpus)
		return
	}

	stored := loadDecimalBaseline(t)
	assertDecimalBaselineCoversCorpus(t, corpus, stored)

	data := decimalFixtureRows(t)
	local := ingestDecimalFixture(t, ctx, data)
	_, dag := setupClusterFixture(t, ctx, DecimalFixture, data)

	live := os.Getenv("WADJET_DUCKDB_COMPARE") == "1" && duckdbPresent()
	setup := ""
	if live {
		setup = duckdbViews(t, duckdbDecimalDataDir)
	}

	for _, c := range corpus {
		t.Run(c.name, func(t *testing.T) {
			want := stored[c.name]
			if live {
				dRows, dCols, err := runDuckDB(setup, c.duckSQL())
				if err != nil {
					t.Fatalf("live duckdb: %v", err)
				}
				assertStoredMatchesLiveDecimal(t, c, want, dCols, dRows)
			}

			var lRows []map[string]any
			var lCols []string
			lRes, lErr := local.Query(ctx, c.sql)
			if lErr == nil {
				lRows, lCols = lRes.Rows, lRes.Columns
			}
			checkDecimalArm(t, c, armLocal, want, lRows, lCols, lErr)

			dRows, dCols, dErr := runArm(t, ctx, dag, c.sql)
			checkDecimalArm(t, c, armDAG, want, dRows, dCols, dErr)
		})
	}
}

// checkDecimalArm gates one arm, honouring a pin exactly as the float gate
// does: a pinned arm still RUNS, and the subtest fails if it starts agreeing.
func checkDecimalArm(t *testing.T, c duckdbCase, arm string, want duckdbBaselineEntry, rows []map[string]any, cols []string, err error) {
	t.Helper()
	pinned := c.knownBugArm == arm || c.knownBugArm == armBoth

	if err != nil {
		if pinned {
			t.Logf("arm %s PINNED (still failing, as recorded): %v\n  %s", arm, err, c.knownBug)
			return
		}
		t.Fatalf("arm %s: %v\n  SQL: %s", arm, err, c.sql)
	}

	ok, detail := compareToDuckDBDecimal(c, want, rows, cols)
	if pinned {
		if ok {
			t.Fatalf("arm %s now AGREES with DuckDB but is still pinned as %q — delete knownBugArm/knownBug "+
				"from the %s entry so the arm is gated again", arm, c.knownBug, c.name)
		}
		t.Logf("arm %s PINNED (still diverging, as recorded): %s\n  %s", arm, detail, c.knownBug)
		return
	}
	if !ok {
		t.Errorf("arm %s diverges from DuckDB on the DECIMAL fixture: %s\n  SQL: %s\n  arm's first row: %v",
			arm, detail, c.sql, firstRow(rows))
	}
}

// compareToDuckDBDecimal is compareToDuckDB with the decimal columns held to
// their digits instead of to six significant figures.
func compareToDuckDBDecimal(c duckdbCase, want duckdbBaselineEntry, rows []map[string]any, cols []string) (bool, string) {
	if c.countOnly {
		if diff := len(rows) - want.RowCount; diff < -c.tolerance || diff > c.tolerance {
			return false, fmt.Sprintf("row count %d, DuckDB %d (±%d allowed)", len(rows), want.RowCount, c.tolerance)
		}
		if c.limit > 0 && len(rows) > c.limit {
			return false, fmt.Sprintf("%d rows returned for LIMIT %d", len(rows), c.limit)
		}
		return true, ""
	}
	if len(cols) != len(want.Columns) {
		return false, fmt.Sprintf("%d columns %v, DuckDB %d %v", len(cols), cols, len(want.Columns), want.Columns)
	}
	aligned, missing := realign(rows, want.Columns)
	if missing != "" {
		return false, fmt.Sprintf("no column %q (arm: %v, DuckDB: %v)", missing, cols, want.Columns)
	}
	res := &oracle.Result{Columns: want.Columns, Rows: aligned}
	// A column the type table calls decimal whose cells are not decimal TEXT
	// did not come back as a DECIMAL, and comparing it would fall through to
	// the float digest — which is the six-significant-digit quantum this
	// gate exists to escape. Saying so is the point: a right value under a
	// float carrier is exactly #695's silent half.
	if col, cell, bad := nonDecimalCarrier(res, decimalScales(c.name)); bad {
		return false, fmt.Sprintf("%s is not a DECIMAL in this answer: cell %#v is %T, not the exact "+
			"text a DECIMAL column boxes as", col, cell, cell)
	}
	quantizeDecimalColumns(res, decimalScales(c.name))
	got := oracle.FingerprintOf(res, c.ordered)
	return want.Fingerprint().Match(got)
}

// nonDecimalCarrier finds the first cell of a decimal-answering column that is
// not exact decimal text.
func nonDecimalCarrier(res *oracle.Result, scales map[string]int) (col string, cell any, bad bool) {
	names := make([]string, 0, len(scales))
	for k := range scales {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, name := range names {
		for _, row := range res.Rows {
			v := row[name]
			if v == nil {
				continue
			}
			s, ok := v.(string)
			if !ok {
				return name, v, true
			}
			if _, ok := roundDecimalText(s, scales[name]); !ok {
				return name, v, true
			}
		}
	}
	return "", nil, false
}

// quantizeDecimalColumns renders every decimal-answering column at the scale
// the type table records, on whichever side it is called for. Both sides go
// through it with the SAME scale, so it can never make two different numbers
// compare equal — only the same number written with a different number of
// trailing digits.
func quantizeDecimalColumns(res *oracle.Result, scales map[string]int) {
	if len(scales) == 0 {
		return
	}
	for col, scale := range scales {
		for _, row := range res.Rows {
			s, ok := row[col].(string)
			if !ok {
				continue
			}
			if q, ok := roundDecimalText(s, scale); ok {
				row[col] = q
			}
		}
	}
}

// roundDecimalText re-renders a plain decimal literal with exactly scale
// fraction digits, rounding half away from zero — PostgreSQL's numeric
// rounding, and the rule ADR-0024 item 3 adopts for scale reduction. ok is
// false for anything that is not a plain decimal (a date, a name, an
// exponent-form float), which leaves the cell alone.
func roundDecimalText(s string, scale int) (string, bool) {
	t := strings.TrimSpace(s)
	if t == "" {
		return "", false
	}
	neg := false
	switch t[0] {
	case '-':
		neg, t = true, t[1:]
	case '+':
		t = t[1:]
	}
	intPart, fracPart := t, ""
	if i := strings.IndexByte(t, '.'); i >= 0 {
		intPart, fracPart = t[:i], t[i+1:]
	}
	if intPart == "" && fracPart == "" {
		return "", false
	}
	if !allDigits(intPart) || !allDigits(fracPart) {
		return "", false
	}
	digits := intPart + fracPart
	if digits == "" {
		return "", false
	}
	n, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return "", false
	}
	switch {
	case len(fracPart) > scale:
		drop := len(fracPart) - scale
		pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(drop)), nil)
		q, r := new(big.Int).QuoRem(n, pow, new(big.Int))
		// half away from zero: the magnitudes are non-negative here, so
		// doubling the remainder and comparing to the divisor is the test.
		if new(big.Int).Lsh(r, 1).Cmp(pow) >= 0 {
			q.Add(q, big.NewInt(1))
		}
		n = q
	case len(fracPart) < scale:
		pow := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(scale-len(fracPart))), nil)
		n.Mul(n, pow)
	}
	out := n.String()
	if scale > 0 {
		for len(out) <= scale {
			out = "0" + out
		}
		out = out[:len(out)-scale] + "." + out[len(out)-scale:]
	}
	if neg && strings.Trim(out, "0.") != "" {
		out = "-" + out
	}
	return out, true
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Corpus, fixture and baseline
// ---------------------------------------------------------------------------

// decimalCorpus is the 22 TPC-H queries under the same mode derivation the
// float gate uses (see duckdbCorpus), plus the pins for what the DECIMAL
// carrier breaks today.
func decimalCorpus() []duckdbCase {
	nums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		nums = append(nums, n)
	}
	sort.Ints(nums)

	out := make([]duckdbCase, 0, len(nums)+4)
	for _, n := range nums {
		name := fmt.Sprintf("Q%02d", n)
		sql := TPCHQueries[n].SQL
		c := duckdbCase{name: name, sql: sql, ordered: hasTopLevelOrderBy(sql)}
		if n == 2 || n == 22 {
			c.countOnly, c.tolerance = true, 4
			c.why = "row membership turns on an aggregate threshold; borderline rows shift with accumulation order"
		}
		if pin, arm, ok := decimalPin(n); ok {
			c.knownBug, c.knownBugArm = pin, arm
		}
		if lim := trailingLimit(sql); lim > 0 {
			stripped := strings.TrimRight(trailingLimitRe.ReplaceAllString(sql, ""), " \t\n")
			full := c
			full.name, full.sql = name+"_nolimit", stripped
			full.ordered = hasTopLevelOrderBy(stripped)
			out = append(out, full)

			c.countOnly, c.limit = true, lim
			c.why = "rows tied at the LIMIT boundary are interchangeable; the count is not"
			out = append(out, c)
			continue
		}
		out = append(out, c)
	}
	return out
}

// decimalPin records what the DECIMAL fixture breaks that the FLOAT64 fixture
// does not. Every one of these queries is green on the FLOAT64 fixture on both
// arms, so each is a defect the spec-conformant carrier reached and no gate in
// the tree could see before it existed. Each names its issue; deleting the
// entry is the fix's proof, and the assertion FAILS if a pinned arm starts
// agreeing.
func decimalPin(n int) (why, arm string, ok bool) {
	const caseOverLiteral = "#695 — a CASE/COALESCE/GREATEST/LEAST whose branches are a DECIMAL column " +
		"and a NUMERIC LITERAL declares the LITERAL's type (INT64 for `0`, FLOAT64 for `0.00`) instead " +
		"of DECIMAL. ADR-0024 item 2: a CHOICE construct over any DECIMAL branch is DECIMAL. Both " +
		"queries wrap the TPC-H revenue expression in `CASE WHEN … THEN … ELSE 0 END`."
	switch n {
	case 14:
		return caseOverLiteral + " Q14's decimal branch FIRES, so the evaluator writes its rendered " +
			"text into an integer vector and the #361 silent-write guard raises `cannot store string " +
			"into INT64 vector`.", armBoth, true
	case 8:
		return caseOverLiteral + " Q08's decimal branch takes NO row at SF0.01, so nothing is ever " +
			"written to the mistyped vector and the query ANSWERS — with brazil_revenue carried as " +
			"float64 where both other engines say numeric. Every VALUE agrees; the carrier does not, " +
			"which is this defect's silent face.", armBoth, true
	case 15, 22:
		return "#696 — on the stage DAG a DECIMAL column compared against a SCALAR SUBQUERY's value is " +
			"compared against that value's UNSCALED Int128 carrier, so the threshold is off by 10^scale. " +
			"`c_acctbal > (SELECT AVG(c_acctbal) …)` answers 0 rows where the truth is 796, and " +
			"`c_acctbal > (SELECT MIN(c_acctbal) …)` answers ALL 1500 where the truth is 1361 — it moves " +
			"in both directions, so a row count says nothing. The subquery itself computes the right " +
			"value; the substitution into the outer comparison is what drops the scale. Silent, and " +
			"DAG-only: the single-process arm stays gated.", armDAG, true
	}
	return "", "", false
}

// decimalFixtureRows reads the committed DECIMAL(15,2) parquet through
// Wadjet's reader, so both arms answer over the same bytes the stored
// fingerprints describe.
func decimalFixtureRows(t *testing.T) map[string][]map[string]any {
	t.Helper()
	dataDir := filepath.Join(".", duckdbDecimalDataDir)
	out := make(map[string][]map[string]any, len(AllTablesDecimal))
	for tableName, schema := range AllTablesDecimal {
		rows, err := loadParquetRows(filepath.Join(dataDir, tableName+".parquet"), schema)
		if err != nil {
			t.Fatalf("load %s: %v", tableName, err)
		}
		out[tableName] = rows
	}
	return out
}

// ingestDecimalFixture builds arm A over the DECIMAL schemas.
func ingestDecimalFixture(t *testing.T, ctx context.Context, data map[string][]map[string]any) *wadjet.DB {
	t.Helper()
	return openFixtureDB(t, ctx, DecimalFixture, data)
}

func duckdbPresent() bool {
	_, err := os.Stat(duckdbBin)
	return err == nil
}

func loadDecimalBaseline(t *testing.T) map[string]duckdbBaselineEntry {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(".", duckdbDecimalBaselineFile))
	if err != nil {
		t.Fatalf("read decimal baseline: %v — regenerate with %s=1", err, regenDecimalBaselineEnv)
	}
	var b duckdbBaseline
	if err := json.Unmarshal(data, &b); err != nil {
		t.Fatalf("parse decimal baseline: %v", err)
	}
	if err := validateDuckDBBaseline(b); err != nil {
		t.Fatalf("%s: %v", duckdbDecimalBaselineFile, err)
	}
	t.Logf("%s: %d DuckDB-derived entries (%s)", duckdbDecimalBaselineFile, len(b.Queries), b.Generator)
	return b.Queries
}

func assertDecimalBaselineCoversCorpus(t *testing.T, corpus []duckdbCase, stored map[string]duckdbBaselineEntry) {
	t.Helper()
	seen := make(map[string]bool, len(corpus))
	for _, c := range corpus {
		seen[c.name] = true
		if _, ok := stored[c.name]; !ok {
			t.Fatalf("%s has no entry for %s — regenerate with %s=1",
				duckdbDecimalBaselineFile, c.name, regenDecimalBaselineEnv)
		}
	}
	for name := range stored {
		if !seen[name] {
			t.Fatalf("%s carries %s, which the corpus no longer contains — regenerate with %s=1",
				duckdbDecimalBaselineFile, name, regenDecimalBaselineEnv)
		}
	}
}

func regenerateDecimalBaseline(t *testing.T, corpus []duckdbCase) {
	t.Helper()
	if !duckdbPresent() {
		t.Fatalf("%s=1 needs the DuckDB CLI at %s", regenDecimalBaselineEnv, duckdbBin)
	}
	version, err := exec.Command(duckdbBin, "--version").Output()
	if err != nil {
		t.Fatalf("duckdb --version: %v", err)
	}
	setup := duckdbViews(t, duckdbDecimalDataDir)
	assertDuckDBDeclaredTypes(t, setup)
	entries := make(map[string]duckdbBaselineEntry, len(corpus))
	for _, c := range corpus {
		rows, cols, err := runDuckDB(setup, c.duckSQL())
		if err != nil {
			t.Fatalf("duckdb %s: %v", c.name, err)
		}
		e := duckdbDecimalEntry(c, cols, rows)
		entries[c.name] = e
		t.Logf("%s (regen from DuckDB): rows=%d %s", c.name, e.RowCount, e.Compare)
	}
	b := duckdbBaseline{
		Generator: fmt.Sprintf("duckdb %s over %s (%s=1)",
			strings.TrimSpace(string(version)), duckdbDecimalDataDir, regenDecimalBaselineEnv),
		Note: "Ground truth for the DECIMAL(15,2) fixture (ADR-0024). Every entry is DuckDB output; " +
			"nothing here is derived from Wadjet. Decimal columns are digested at the scale " +
			"decimal_variant_test.go's decimalOutputTypes records, which is min(scale) where the two " +
			"engines' type rules keep a different number of digits.",
		Queries: entries,
	}
	data, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(".", duckdbDecimalBaselineFile), data, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	t.Logf("wrote %s with %d entries", duckdbDecimalBaselineFile, len(entries))
}

// assertDuckDBDeclaredTypes checks the `duckdb` half of decimalOutputTypes
// against DuckDB's own DESCRIBE. The table's whole claim is that it records
// what BOTH engines declare; without this it would record only what someone
// once wrote down about the second one, and the `scale` each comparison runs
// at is derived from that. It runs at regeneration, which is the only moment
// the DuckDB binary is required.
func assertDuckDBDeclaredTypes(t *testing.T, setup string) {
	t.Helper()
	want := make(map[string]string, len(decimalOutputTypes))
	for _, d := range decimalOutputTypes {
		want[d.query+"."+d.column] = d.duckdb
	}
	for n := 1; n <= 22; n++ {
		name := fmt.Sprintf("Q%02d", n)
		rows, _, err := runDuckDB(setup, "DESCRIBE "+TPCHQueries[n].SQL)
		if err != nil {
			t.Fatalf("duckdb DESCRIBE %s: %v", name, err)
		}
		for _, r := range rows {
			key := name + "." + r["column_name"]
			w, listed := want[key]
			if !listed {
				if strings.HasPrefix(r["column_type"], "DECIMAL") {
					t.Errorf("DuckDB declares %s as %s but decimalOutputTypes does not list it",
						key, r["column_type"])
				}
				continue
			}
			delete(want, key)
			if r["column_type"] != w {
				t.Errorf("DuckDB declares %s as %s, decimalOutputTypes says %s — the table is the "+
					"record of what both engines declare, so fix the table (and the compare scale "+
					"beside it) before regenerating", key, r["column_type"], w)
			}
		}
	}
	for key := range want {
		t.Errorf("decimalOutputTypes lists %s, which DuckDB's DESCRIBE does not return", key)
	}
}

// duckdbDecimalEntry turns one live DuckDB result into a stored entry, with
// the decimal columns held at the table's scale before digesting.
func duckdbDecimalEntry(c duckdbCase, cols []string, rows []map[string]string) duckdbBaselineEntry {
	e := duckdbBaselineEntry{Source: "duckdb", RowCount: len(rows), Compare: "rows", Ordered: c.ordered}
	if c.countOnly {
		e.Compare = "count"
		e.Tolerance, e.Limit, e.Why = c.tolerance, c.limit, c.why
		return e
	}
	scales := decimalScales(c.name)
	typed := make([]map[string]any, len(rows))
	for i, r := range rows {
		m := make(map[string]any, len(cols))
		for _, col := range cols {
			if _, ok := scales[col]; ok {
				// A decimal column stays TEXT on this side too: reading it
				// as a float64 is precisely the quantization this gate
				// exists to remove.
				if r[col] == duckdbNull {
					m[col] = nil
				} else {
					m[col] = r[col]
				}
				continue
			}
			m[col] = oracle.TextCell(r[col], duckdbNull)
		}
		typed[i] = m
	}
	res := &oracle.Result{Columns: cols, Rows: typed}
	quantizeDecimalColumns(res, scales)
	e.Columns = append([]string(nil), cols...)
	fp := oracle.FingerprintOf(res, c.ordered)
	e.Fine, e.Coarse = fp.Fine, fp.Coarse
	return e
}

// assertStoredMatchesLiveDecimal verifies the committed entry against the
// engine it claims to come from.
func assertStoredMatchesLiveDecimal(t *testing.T, c duckdbCase, want duckdbBaselineEntry, cols []string, rows []map[string]string) {
	t.Helper()
	live := duckdbDecimalEntry(c, cols, rows)
	if live.RowCount != want.RowCount {
		t.Errorf("STORED BASELINE IS NOT DUCKDB'S ANSWER: %s row count %d stored, live %d — regenerate with %s=1",
			c.name, want.RowCount, live.RowCount, regenDecimalBaselineEnv)
		return
	}
	if c.countOnly {
		return
	}
	if live.Fine != want.Fine || live.Coarse != want.Coarse {
		t.Errorf("STORED BASELINE IS NOT DUCKDB'S ANSWER: %s digest %s/%s stored, live %s/%s — regenerate with %s=1",
			c.name, want.Fine, want.Coarse, live.Fine, live.Coarse, regenDecimalBaselineEnv)
	}
}

// ---------------------------------------------------------------------------
// The declared-type gate
// ---------------------------------------------------------------------------

// TestTPCHDecimalDeclaredTypes asserts wadjet declares what decimalOutputTypes
// says for every DECIMAL output column of every query, and that the table
// leaves none of them out. A value can be right under a wrong declaration —
// a JDBC client sizing a column, or a pgwire RowDescription, reads the
// declaration and not the digits — so this is gated separately from the
// answer.
func TestTPCHDecimalDeclaredTypes(t *testing.T) {
	ctx := context.Background()
	data := decimalFixtureRows(t)
	db := ingestDecimalFixture(t, ctx, data)

	want := make(map[string]decimalCol, len(decimalOutputTypes))
	for _, d := range decimalOutputTypes {
		want[d.query+"."+d.column] = d
	}
	seen := make(map[string]bool, len(want))

	for n := 1; n <= 22; n++ {
		name := fmt.Sprintf("Q%02d", n)
		res, err := db.Query(ctx, GetQuery(n, SF001).SQL)
		if err != nil {
			if why, _, pinned := decimalPin(n); pinned {
				t.Logf("%s PINNED, no declaration to check: %v\n  %s", name, err, why)
				continue
			}
			t.Errorf("%s: %v", name, err)
			continue
		}
		for _, m := range res.ColumnMetas {
			key := name + "." + m.Name
			d, listed := want[key]
			if !listed {
				if strings.HasPrefix(declaredTypeString(m), "DECIMAL") {
					t.Errorf("%s is %s but decimalOutputTypes does not list it — every DECIMAL output "+
						"column belongs in that table, which is what makes it the record of ADR-0024 "+
						"item 3's rule over this corpus", key, declaredTypeString(m))
				}
				continue
			}
			seen[key] = true
			got := declaredTypeString(m) + " typmod " + wireTypmodString(m)
			w := d.wadjet + " typmod " + d.wireTypmod
			switch {
			case d.typePin == "":
				if got != w {
					t.Errorf("%s declares %s, want %s", key, got, w)
				}
			case got == w:
				t.Errorf("%s now declares %s, which is what the table wants, but it is still pinned as "+
					"%q — delete typePin from that row so the column is gated again", key, got, d.typePin)
			default:
				t.Logf("%s PINNED: declares %s, want %s\n  %s", key, got, w, d.typePin)
			}
		}
	}
	for key := range want {
		if !seen[key] {
			q := key[:strings.IndexByte(key, '.')]
			n, _ := strconv.Atoi(strings.TrimPrefix(q, "Q"))
			if _, _, pinned := decimalPin(n); pinned {
				continue
			}
			t.Errorf("decimalOutputTypes lists %s, which the query does not return", key)
		}
	}
}

// wireTypmodString renders what RowDescription would carry for this column:
// the declared (p,s) for a constrained numeric, "-1" for an unconstrained
// one. It is pgTypeMod's decision, spelled the way the table records it.
func wireTypmodString(m wadjet.ColumnMeta) string {
	if m.TypeID.String() != "DECIMAL" || m.Precision <= 0 || m.WireUnconstrained {
		return typmodUnconstrd
	}
	return fmt.Sprintf("(%d,%d)", m.Precision, m.Scale)
}

func declaredTypeString(m wadjet.ColumnMeta) string {
	if m.TypeID.String() == "DECIMAL" {
		return fmt.Sprintf("DECIMAL(%d,%d)", m.Precision, m.Scale)
	}
	return m.TypeID.String()
}

// TestTPCHDecimalFixtureIsSpecConformant pins the shape of the variant: the
// eight columns the specification declares DECIMAL(15,2) carry that type, the
// FLOAT64 schema is untouched, and the two fixtures differ in nothing else.
func TestTPCHDecimalFixtureIsSpecConformant(t *testing.T) {
	if len(AllTablesDecimal) != len(AllTables) {
		t.Fatalf("decimal fixture has %d tables, float fixture %d", len(AllTablesDecimal), len(AllTables))
	}
	found := map[string]bool{}
	for name, ds := range AllTablesDecimal {
		fs := AllTables[name]
		if len(ds.Columns) != len(fs.Columns) {
			t.Fatalf("%s: %d columns decimal, %d float", name, len(ds.Columns), len(fs.Columns))
		}
		for i, dc := range ds.Columns {
			fc := fs.Columns[i]
			if dc.Name != fc.Name || dc.Nullable != fc.Nullable {
				t.Errorf("%s.%s differs beyond its type: %+v vs %+v", name, dc.Name, dc, fc)
			}
			if !MoneyColumns[dc.Name] {
				if dc.Type != fc.Type {
					t.Errorf("%s.%s is %s in the decimal fixture and %s in the float one, but it is not "+
						"a monetary column — the two fixtures must differ ONLY in the specification's eight",
						name, dc.Name, dc.Type, fc.Type)
				}
				continue
			}
			found[dc.Name] = true
			if got := declaredColumn(dc.Type.String(), dc.Precision, dc.Scale); got != "DECIMAL(15,2)" {
				t.Errorf("%s.%s is %s, want DECIMAL(15,2)", name, dc.Name, got)
			}
			if fc.Type.String() != "FLOAT64" {
				t.Errorf("%s.%s is %s in the FLOAT64 fixture — that schema is the published benchmark "+
					"and this variant must not move it", name, dc.Name, fc.Type)
			}
		}
	}
	for col := range MoneyColumns {
		if !found[col] {
			t.Errorf("MoneyColumns names %s, which no table declares", col)
		}
	}
}

func declaredColumn(typ string, p, s int) string {
	if typ == "DECIMAL" {
		return fmt.Sprintf("DECIMAL(%d,%d)", p, s)
	}
	return typ
}

// TestDecimalGateRejectsOneCent is the gate's proof of work. The float gate
// digests at six significant figures because float summation order moves the
// seventh; on a sum like 3027140810.74 that quantum would accept an error of
// about a thousand. This gate must reject a wrong PENNY, and this is the
// assertion that says it does — the exact analogue of
// TestGateCatchesHistoricalBugs for the decimal arm.
func TestDecimalGateRejectsOneCent(t *testing.T) {
	// Q01's sum_base_price over the committed fixture, and the same answer
	// one penny out. Nothing else differs.
	const truth, offByOne = "3027140810.74", "3027140810.75"
	const col, scale = "sum_base_price", 2
	c := duckdbCase{name: "Q01", ordered: true}
	cols := []string{col}

	digest := func(cell any) oracle.Fingerprint {
		res := &oracle.Result{Columns: cols, Rows: []map[string]any{{col: cell}}}
		quantizeDecimalColumns(res, map[string]int{col: scale})
		return oracle.FingerprintOf(res, true)
	}
	want := duckdbBaselineEntry{
		Source: "duckdb", RowCount: 1, Compare: "rows", Ordered: true, Columns: cols,
	}
	fp := digest(truth)
	want.Fine, want.Coarse = fp.Fine, fp.Coarse

	if ok, detail := compareToDuckDBDecimal(c, want, []map[string]any{{col: truth}}, cols); !ok {
		t.Fatalf("the gate rejects the RIGHT answer: %s", detail)
	}
	if ok, _ := compareToDuckDBDecimal(c, want, []map[string]any{{col: offByOne}}, cols); ok {
		t.Errorf("an answer one penny out of %s matched — the decimal gate is not exact", truth)
	}

	// And the contrast that motivates the whole file: read as float64, the
	// two are the same answer at both of the float gate's quanta. A penny on
	// a nine-figure sum is nine significant digits down; the fine digest
	// keeps six. This is not a defect in the float gate — a float sum
	// genuinely does not carry that digit — it is why an EXACT type needs
	// its own comparison.
	fl := func(s string) oracle.Fingerprint {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			t.Fatalf("%q: %v", s, err)
		}
		return oracle.FingerprintOf(&oracle.Result{Columns: cols, Rows: []map[string]any{{col: f}}}, true)
	}
	a, b := fl(truth), fl(offByOne)
	if a.Fine != b.Fine && a.Coarse != b.Coarse {
		t.Errorf("the float digest now separates %s from %s at both quanta; if that is deliberate, "+
			"this contrast needs rewriting, and the decimal gate's assertion above still stands",
			truth, offByOne)
	}
}

func TestRoundDecimalText(t *testing.T) {
	cases := []struct {
		in    string
		scale int
		want  string
		ok    bool
	}{
		{"1.005", 2, "1.01", true},   // half AWAY from zero, not to even
		{"-1.005", 2, "-1.01", true}, // and away on the negative side too
		{"1.004", 2, "1.00", true},
		{"1.5", 0, "2", true},
		{"-1.5", 0, "-2", true},
		{"12.5", 2, "12.50", true}, // widening pads
		{"0.001", 2, "0.00", true}, // a rounded-away sign is not kept
		{"-0.001", 2, "0.00", true},
		{"3027140810.74", 2, "3027140810.74", true},
		{"50452.346845549556", 6, "50452.346846", true}, // the AVG min(scale) case
		{"493827160549382.7160549350", 10, "493827160549382.7160549350", true},
		{"1996-01-02", 2, "", false}, // a date is not a decimal
		{"REG AIR", 2, "", false},
		{"1.2e5", 2, "", false}, // no exponent form reaches a DECIMAL rendering
		{"", 2, "", false},
	}
	for _, c := range cases {
		got, ok := roundDecimalText(c.in, c.scale)
		if ok != c.ok {
			t.Errorf("roundDecimalText(%q, %d) ok=%v, want %v", c.in, c.scale, ok, c.ok)
			continue
		}
		if ok && got != c.want {
			t.Errorf("roundDecimalText(%q, %d) = %q, want %q", c.in, c.scale, got, c.want)
		}
	}
}

// TestTPCHDecimalDatagenWritesExactCents proves the generator's decimal arm
// carries the digits it means. Every monetary cell is exact text with two
// fraction digits, and the FLOAT64 arm of the same draw is that text's value
// — so the two fixtures hold the same numbers and the decimal one never went
// through a float64.
func TestTPCHDecimalDatagenWritesExactCents(t *testing.T) {
	fl := GenerateFor(SF001, FloatFixture)
	dec := GenerateFor(SF001, DecimalFixture)
	for table, rows := range dec {
		fr := fl[table]
		if len(fr) != len(rows) {
			t.Fatalf("%s: %d decimal rows, %d float rows", table, len(rows), len(fr))
		}
		for i, row := range rows {
			for col, v := range row {
				if !MoneyColumns[col] {
					continue
				}
				s, ok := v.(string)
				if !ok {
					t.Fatalf("%s.%s row %d is %T, want exact decimal text", table, col, i, v)
				}
				if dot := strings.IndexByte(s, '.'); dot < 0 || len(s)-dot-1 != 2 {
					t.Fatalf("%s.%s row %d is %q, want exactly two fraction digits", table, col, i, s)
				}
				f, err := strconv.ParseFloat(s, 64)
				if err != nil {
					t.Fatalf("%s.%s row %d is %q: %v", table, col, i, s, err)
				}
				if got := fr[i][col].(float64); got != f {
					t.Fatalf("%s.%s row %d: decimal %q, float %v — the two fixtures must hold the same value",
						table, col, i, s, got)
				}
			}
		}
	}
}
