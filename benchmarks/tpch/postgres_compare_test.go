package tpch

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/multikey"
)

// TestPostgresOracle is the PostgreSQL differential gate. It has two arms and
// the second is the one DuckDB cannot provide:
//
//	EngineSemantics — the corpus through the embedded Wadjet API and through a
//	                  real PostgreSQL, compared with oracle.Fingerprint. This
//	                  answers "does the engine MEAN what PostgreSQL means".
//	WireProtocol    — the same SQL through the SAME PostgreSQL client library
//	                  (pgx) against Wadjet's pgwire endpoint and against real
//	                  PostgreSQL, comparing what the WIRE carries: type OIDs,
//	                  format codes, RowDescription metadata, NULL
//	                  representation, SQLSTATE and the command tag. This
//	                  answers "is this what a PostgreSQL CLIENT expects", which
//	                  is a question no non-pg oracle can be asked at all. It
//	                  lives in postgres_wire_test.go.
//
// Both arms share one server and one load — see postgres_oracle_test.go for
// the harness, the scale parameter, and why the string collation is configured
// rather than exempted.
//
// The suite SKIPS with no PostgreSQL reachable. There is no stored baseline: a
// wire comparison has nothing a file could stand in for, and a stored file is
// the artefact that twice made a wrong answer look like ground truth here.
func TestPostgresOracle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Minute)
	t.Cleanup(cancel)

	o := newPostgresOracle(t, ctx)

	t.Run("EngineSemantics", func(t *testing.T) { runPostgresSemanticsArm(t, ctx, o) })
	t.Run("WireProtocol", func(t *testing.T) { runPostgresWireArm(t, ctx, o) })
}

// pgCase is one corpus entry. The comparison modes mirror the DuckDB arm's,
// for the same reasons: see duckdbCase.
type pgCase struct {
	name string
	sql  string
	// ordered: a top-level ORDER BY makes the row SEQUENCE part of the answer.
	ordered bool
	// countOnly: the row CONTENT is not determined by the query; why says which
	// of the two reasons applies.
	countOnly bool
	why       string
	limit     int
	tolerance int
	// exactNumeric compares NUMERIC/DECIMAL cells as their exact decimal
	// digits on both sides instead of through the fingerprint's float
	// rendering. Set it on an entry whose answer IS the digits — a DECIMAL
	// aggregate — and leave it off where the two engines legitimately keep a
	// different number of them (AVG, whose scale rule differs by contract:
	// PostgreSQL picks a scale giving at least 16 significant digits, wadjet
	// widens the input's scale by 4 — see batch.AvgScaleIncrement).
	//
	// Without it these entries prove almost nothing: both sides render to a
	// float64 and agree about the first six significant digits, which is
	// exactly the agreement #455 had while MAX(numeric(38,10)) was returning
	// 9.777777778877776e+14 for 977777777887777.7577887713.
	exactNumeric bool
	// pgSQL is the PostgreSQL-dialect spelling of the same question, for the
	// handful of shapes the two engines write differently. It is a SPELLING
	// escape hatch and nothing more: it must ask PostgreSQL the identical
	// question, and the entry is worthless the moment it does not.
	//
	// The one use today is ROW field access. PostgreSQL requires the
	// parentheses — `(rw).b`, because bare `rw.b` is read as table.column and
	// errors with "missing FROM-clause entry for table rw" — while wadjet
	// spells it `rw.b`. Nothing about the MEANING differs, which is exactly
	// why the comparison is worth making (#568).
	pgSQL string
	// knownBug pins a divergence that is not gated today. The comparison still
	// runs and the subtest FAILS when the divergence disappears, so deleting
	// this field is the whole of "the fix landed". Classified by kind:
	//
	//	pgBugWadjet      — a Wadjet defect, with an open issue in `issue`.
	//	pgBugUnsupported — Wadjet cannot answer the query at all. An error is an
	//	                   acceptable answer; the pin says so out loud.
	//
	// There is deliberately no third kind for "a difference Wadjet CHOSE". A
	// deliberate difference of VALUE would mean the two engines answer
	// different questions, and the fix for that is to configure the oracle
	// (postgresCollation) or to rewrite the entry — not to exempt it. The
	// deliberate differences this project does have are differences of TYPE,
	// and those are reported by the wire arm, where a client meets them.
	knownBug string
	// issue is the tracker reference for a knownBug of kind pgBugWadjet.
	issue string
}

// runPostgresSemanticsArm runs every corpus entry on both engines and compares
// the answers cell by cell.
func runPostgresSemanticsArm(t *testing.T, ctx context.Context, o *postgresOracle) {
	corpus := postgresCorpus()

	gated, pinned := 0, 0
	for _, c := range corpus {
		t.Run(c.name, func(t *testing.T) {
			run := o.runPostgres
			if c.exactNumeric {
				run = o.runPostgresExact
			}
			oracleSQL := c.sql
			if c.pgSQL != "" {
				oracleSQL = c.pgSQL
			}
			pgRows, pgCols, pgErr := run(ctx, oracleSQL)
			if pgErr != nil {
				// PostgreSQL refusing the query means the entry is not a
				// question about Wadjet. It is always the corpus's fault, so
				// it fails loudly rather than being skipped.
				t.Fatalf("the ORACLE refused this query, so it cannot be ground truth for anything: %v\n  SQL: %s",
					pgErr, oracleSQL)
			}

			wRows, wCols, wErr := runWadjet(ctx, o.db, c.sql)
			if wErr != nil {
				if c.knownBug != "" {
					t.Logf("known divergence, NOT gated: Wadjet cannot answer this query: %v\n  %s%s",
						wErr, c.knownBug, issueSuffix(c.issue))
					return
				}
				t.Errorf("Wadjet failed a query PostgreSQL answered with %d rows: %v\n  SQL: %s",
					len(pgRows), wErr, c.sql)
				return
			}

			ok, detail := comparePostgres(c, pgRows, pgCols, wRows, wCols)
			if c.knownBug != "" {
				if ok {
					t.Errorf("Wadjet now agrees with PostgreSQL, so this known divergence is FIXED:\n  %s%s\n"+
						"Delete the knownBug field on %s in postgresCorpus so the entry is gated again.",
						c.knownBug, issueSuffix(c.issue), c.name)
					return
				}
				t.Logf("known divergence, NOT gated: %s\n  %s%s", detail, c.knownBug, issueSuffix(c.issue))
				return
			}
			if !ok {
				mode := "unordered"
				switch {
				case c.countOnly:
					mode = "row count only: " + c.why
				case c.ordered:
					mode = "ordered — a top-level ORDER BY makes the row sequence part of the answer"
				}
				t.Errorf("Wadjet diverges from PostgreSQL [%s]: %s\n  SQL: %s\n%s",
					mode, detail, c.sql, postgresCellDiff(pgRows, pgCols, wRows, wCols))
			}
		})
		if c.knownBug != "" {
			pinned++
		} else {
			gated++
		}
	}
	t.Logf("Summary: %d corpus entries against live PostgreSQL over %s; %d gated, %d pinned as known divergences",
		len(corpus), o.tier, gated, pinned)
}

func issueSuffix(issue string) string {
	if issue == "" {
		return ""
	}
	return " (" + issue + ")"
}

// comparePostgres holds Wadjet's answer against PostgreSQL's.
//
// Cells are compared POSITIONALLY, not by column name. PostgreSQL names an
// unaliased expression "?column?" and lowercases every unquoted identifier, so
// a name comparison here would manufacture divergences that say nothing about
// the answer. Column NAMES are not unchecked — they are what the wire arm
// compares, in the RowDescription, which is where a client actually reads them.
func comparePostgres(c pgCase, pgRows []map[string]any, pgCols []string, wRows []map[string]any, wCols []string) (bool, string) {
	if c.countOnly {
		if diff := len(wRows) - len(pgRows); diff < -c.tolerance || diff > c.tolerance {
			return false, fmt.Sprintf("row count %d, PostgreSQL %d (±%d allowed)", len(wRows), len(pgRows), c.tolerance)
		}
		if c.limit > 0 && len(wRows) > c.limit {
			return false, fmt.Sprintf("%d rows returned for LIMIT %d", len(wRows), c.limit)
		}
		return true, ""
	}
	if len(wCols) != len(pgCols) {
		return false, fmt.Sprintf("%d columns %v, PostgreSQL %d %v", len(wCols), wCols, len(pgCols), pgCols)
	}
	pgPos, wPos := positional(pgRows, pgCols), positional(wRows, wCols)
	reconcileNumericBoxing(c, pgPos, wPos)
	want := oracle.FingerprintOf(pgPos, c.ordered)
	got := oracle.FingerprintOf(wPos, c.ordered)
	return want.Match(got)
}

// reconcileNumericBoxing puts the two engines' numbers in ONE rendering
// before they are digested. It is a rendering rule, not an exemption: it
// never makes two different NUMBERS compare equal, only the same number
// handed over as text by one engine and as a float by the other.
//
// Two directions, chosen per entry:
//
//   - exactNumeric: both sides carry the digits (the PostgreSQL side through
//     runPostgresExact, the Wadjet side because a DECIMAL cell IS its text),
//     so both are canonicalised — trailing fraction zeros trimmed, so
//     PostgreSQL's numeric(9,2) "12.50" and Wadjet's "12.5" are the same
//     value written twice — and compared digit for digit.
//
//   - otherwise: PostgreSQL flattens a NUMERIC to float64 while Wadjet hands
//     a DECIMAL over as text, and the fingerprint would compare "16303690.96"
//     against "1.63037e+07" and call it a divergence about nothing. Where the
//     PostgreSQL side of a column is a float and the Wadjet side is a numeric
//     STRING, the string is read as a float so both render through the same
//     quantum. Keyed on the pair, so a genuine text column — where
//     PostgreSQL's side is also a string — is untouched.
func reconcileNumericBoxing(c pgCase, pg, w *oracle.Result) {
	if len(pg.Rows) == 0 || len(w.Rows) == 0 {
		return
	}
	for _, key := range pg.Columns {
		if c.exactNumeric {
			canonicalizeNumericStrings(pg, key)
			canonicalizeNumericStrings(w, key)
			continue
		}
		if !columnIsFloatBoxed(pg, key) {
			continue
		}
		for _, row := range w.Rows {
			s, ok := row[key].(string)
			if !ok {
				continue
			}
			if f, ok := parseFloat(strings.TrimSpace(s)); ok {
				row[key] = f
			}
		}
	}
}

// columnIsFloatBoxed reports whether every non-NULL cell of this column on the
// PostgreSQL side arrived as a float — i.e. the column is a number there.
func columnIsFloatBoxed(res *oracle.Result, key string) bool {
	sawFloat := false
	for _, row := range res.Rows {
		switch row[key].(type) {
		case nil:
		case float64, float32:
			sawFloat = true
		default:
			return false
		}
	}
	return sawFloat
}

// canonicalizeNumericStrings rewrites every plain-decimal STRING cell of one
// column into one spelling: no trailing fraction zeros, no bare trailing
// point, no "-0". Applied to both engines, so it cannot hide a difference of
// value — only a difference of how many zeros each side chose to print.
func canonicalizeNumericStrings(res *oracle.Result, key string) {
	for _, row := range res.Rows {
		s, ok := row[key].(string)
		if !ok {
			continue
		}
		if canon, ok := canonicalDecimalString(s); ok {
			row[key] = canon
		}
	}
}

// canonicalDecimalString parses a plain decimal literal (sign, digits,
// optional fraction — no exponent, which no DECIMAL rendering produces) and
// re-renders it canonically. ok=false leaves the cell alone, which is what
// keeps a date, a name or an IP address out of this.
func canonicalDecimalString(s string) (string, bool) {
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
	if dot := strings.IndexByte(t, '.'); dot >= 0 {
		intPart, fracPart = t[:dot], t[dot+1:]
	}
	if intPart == "" && fracPart == "" {
		return "", false
	}
	for _, part := range []string{intPart, fracPart} {
		for i := 0; i < len(part); i++ {
			if part[i] < '0' || part[i] > '9' {
				return "", false
			}
		}
	}
	unscaled, ok := new(big.Int).SetString(intPart+fracPart, 10)
	if !ok {
		return "", false
	}
	if neg {
		unscaled.Neg(unscaled)
	}
	return canonicalDecimalText(unscaled, len(fracPart)), true
}

// canonicalDecimalText renders an unscaled integer at a scale in the one
// spelling both engines are compared in: the digits split at the scale with
// trailing fraction zeros removed, and no decimal point at all when nothing
// is left after it. Shared with the PostgreSQL side's exactNumericText.
func canonicalDecimalText(unscaled *big.Int, scale int) string {
	neg := unscaled.Sign() < 0
	digits := new(big.Int).Abs(unscaled).String()
	out := digits
	if scale > 0 {
		if len(digits) <= scale {
			digits = strings.Repeat("0", scale-len(digits)+1) + digits
		}
		intPart, frac := digits[:len(digits)-scale], digits[len(digits)-scale:]
		frac = strings.TrimRight(frac, "0")
		if frac == "" {
			out = intPart
		} else {
			out = intPart + "." + frac
		}
	}
	out = strings.TrimLeft(out, "0")
	if out == "" || out[0] == '.' {
		out = "0" + out
	}
	if neg && strings.Trim(out, "0.") != "" {
		out = "-" + out
	}
	return out
}

// positional re-keys rows onto "c0".."cN" in the engine's own column order, so
// the fingerprint digests values and never column spelling.
func positional(rows []map[string]any, cols []string) *oracle.Result {
	keys := make([]string, len(cols))
	for i := range cols {
		keys[i] = fmt.Sprintf("c%d", i)
	}
	out := make([]map[string]any, len(rows))
	for i, row := range rows {
		m := make(map[string]any, len(cols))
		for j, col := range cols {
			v, ok := lookupCell(row, col)
			if !ok {
				v = row[col]
			}
			m[keys[j]] = v
		}
		out[i] = m
	}
	return &oracle.Result{Columns: keys, Rows: out}
}

// postgresCellDiff renders the first few differing cells, which is what turns a
// digest mismatch into a defect report.
func postgresCellDiff(pgRows []map[string]any, pgCols []string, wRows []map[string]any, wCols []string) string {
	var sb strings.Builder
	if len(pgRows) != len(wRows) {
		fmt.Fprintf(&sb, "  rows: wadjet=%d postgres=%d\n", len(wRows), len(pgRows))
	}
	fmt.Fprintf(&sb, "  columns: wadjet=%v postgres=%v\n", wCols, pgCols)
	n := len(pgRows)
	if len(wRows) < n {
		n = len(wRows)
	}
	shown := 0
	for i := 0; i < n && shown < 5; i++ {
		for j := 0; j < len(pgCols) && j < len(wCols); j++ {
			pv, _ := lookupCell(pgRows[i], pgCols[j])
			wv, _ := lookupCell(wRows[i], wCols[j])
			if cellEqual(wv, canonicalString(pv)) {
				continue
			}
			fmt.Fprintf(&sb, "  row %d col %d (%s/%s): wadjet=%#v postgres=%#v\n",
				i, j, wCols[j], pgCols[j], wv, pv)
			shown++
			if shown >= 5 {
				break
			}
		}
	}
	return sb.String()
}

// Kinds of pin, spelled out once so a knownBug string cannot drift into a
// shrug. Every pin in postgresCorpus opens with one of these.
const (
	pgBugWadjet      = "WADJET BUG:"
	pgBugUnsupported = "UNIMPLEMENTED IN WADJET:"
)

// windowCountNullPin is #670, found while fixing #586 and filed rather than
// fixed there: `COUNT(col) OVER (...)` answers the frame's ROW COUNT instead
// of the column's non-NULL count, so a NULL in the frame inflates it. The
// grouped `COUNT(col)` is right, which makes this the same
// windowed-versus-grouped disagreement #586 closed for SUM/AVG, at the one
// function #586 deliberately left alone (COUNT(*) must keep counting rows).
//
// It is pinned on an entry whose OTHER columns — the exact SUM beside it —
// stay gated, so the pin cannot hide a regression in what this entry is
// actually here for.
const windowCountNullPin = pgBugWadjet + " COUNT(col) OVER counts the frame's rows rather than " +
	"the column's non-NULL values, so a NULL in the frame inflates it; the grouped COUNT(col) is right"

// postgresCorpus is the 22 TPC-H queries plus the PostgreSQL-semantics shapes
// they leave dark.
//
// The TPC-H half is the shared load-bearing corpus and is derived exactly as
// the DuckDB arm derives it (top-level ORDER BY ⇒ ordered; a trailing LIMIT
// splits into a stripped row-compare plus a verbatim count-compare; Q02/Q22
// relax to counts because their row membership turns on a float threshold).
//
// The second half is what this oracle exists for. Every entry is a rule
// PostgreSQL DEFINES and a client depends on, and none of them is decided by
// the TPC-H queries: TPC-H contains no NULLs, no integer division, no rounding
// at a tie, no set operation, no mixed-case string ordering, and no error at
// all.
func postgresCorpus() []pgCase {
	nums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		nums = append(nums, n)
	}
	sort.Ints(nums)

	out := make([]pgCase, 0, len(nums)+80)
	for _, n := range nums {
		name := fmt.Sprintf("Q%02d", n)
		sql := TPCHQueries[n].SQL
		c := pgCase{name: name, sql: sql, ordered: hasTopLevelOrderBy(sql)}
		if n == 2 || n == 22 {
			c.countOnly, c.tolerance = true, 4
			c.why = "row membership turns on a float threshold; borderline rows shift with accumulation order"
		}
		// Q08 and Q14 were pinned here under TPCH_DECIMAL=1 for #695 — a CASE
		// over a DECIMAL column and a numeric literal declared the LITERAL's
		// type, so the decimal evaluator wrote its text into an integer vector
		// and the #361 store guard raised. Both agree with PostgreSQL now and
		// are gated again at every tier.
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

	out = append(out, postgresRowFieldCases()...)
	out = append(out, postgresSemanticsCases()...)
	out = append(out, postgresConstArgAggCases()...)

	for i := range out {
		if !out[i].countOnly {
			out[i].ordered = hasTopLevelOrderBy(out[i].sql)
		}
	}
	return out
}

// postgresConstArgAggCases asks PostgreSQL what an aggregate over a
// COMPILE-TIME LITERAL argument means — MIN(1), BOOL_AND(TRUE), SUM(0) (#621).
//
// Wadjet handed such an aggregate to the executor with its argument as a
// column NAME ("1"), which no scan produces: the whole-table form errored and
// the grouped form silently dropped every group, so `HAVING MIN(1) > 0` — TRUE
// for every non-empty group — returned nothing. PostgreSQL evaluates the
// constant per group and keeps them all, which is the ground truth here
// (ADR-0012). The single-process pre-aggregate projection now materializes the
// literal into a column, matching the DAG spec path that already did.
//
// The entries are grouped over nation.n_regionkey (five groups) and cover both
// the value in the SELECT list and the aggregate used as a HAVING predicate,
// for each aggregate kind.
func postgresConstArgAggCases() []pgCase {
	var out []pgCase
	add := func(name, sql string) {
		out = append(out, pgCase{name: name, sql: sql})
	}

	// The value carried in the SELECT list.
	add("ConstArgSelectValues",
		`SELECT n_regionkey, MIN(1) AS mn, MAX(1) AS mx, SUM(0) AS s, COUNT(1) AS c,
			BOOL_AND(TRUE) AS ba, BOOL_OR(FALSE) AS bo
		 FROM nation GROUP BY n_regionkey ORDER BY n_regionkey`)
	add("ConstArgSelectMinMaxNonTrivial",
		`SELECT n_regionkey, MIN(7) AS mn, MAX(7) AS mx, SUM(3) AS s
		 FROM nation GROUP BY n_regionkey ORDER BY n_regionkey`)

	// The aggregate used directly as the HAVING predicate. Each keeps every
	// group (the constant makes the test TRUE for all of them), which is the
	// exact shape the soak reduced to.
	for _, c := range []struct{ name, having string }{
		{"ConstArgHavingMin", "MIN(1) > 0"},
		{"ConstArgHavingMax", "MAX(1) > 0"},
		{"ConstArgHavingSum", "SUM(0) = 0"},
		{"ConstArgHavingCount", "COUNT(1) > 0"},
		{"ConstArgHavingBoolAnd", "BOOL_AND(TRUE)"},
		{"ConstArgHavingBoolOr", "BOOL_OR(TRUE)"},
		{"ConstArgHavingMinExcludesAll", "MIN(1) > 1"},
		{"ConstArgHavingBoolAndFalse", "BOOL_AND(FALSE)"},
	} {
		add(c.name,
			`SELECT n_regionkey FROM nation GROUP BY n_regionkey HAVING `+c.having+
				` ORDER BY n_regionkey`)
	}

	// The whole-table form (no GROUP BY), which used to ERROR outright.
	add("ConstArgWholeTable",
		`SELECT MIN(1) AS mn, MAX(1) AS mx, SUM(0) AS s, COUNT(1) AS c FROM nation`)
	return out
}

// postgresRowFieldCases asks PostgreSQL what a ROW FIELD PATH means (#568).
//
// This is the question the DuckDB arm cannot carry: its fixture is the eight
// committed TPC-H parquet files, none of which has a STRUCT column, and
// adding one would mean regenerating all eight and rippling through every
// suite that reads AllTables. The PostgreSQL arm has the clean seam —
// oracleTables plus a Go row builder — so the composite fixture lives here
// and the field-path semantics are gated against the engine that DEFINES
// them (ADR-0012).
//
// Each entry differs from its PostgreSQL twin only in the parentheses the
// composite access requires; the values, the ordering and the NULL rules
// compared are PostgreSQL's own.
func postgresRowFieldCases() []pgCase {
	var out []pgCase
	add := func(name, w, p string) {
		out = append(out, pgCase{name: name, sql: w, pgSQL: p})
	}
	addExact := func(name, w, p string) {
		out = append(out, pgCase{name: name, sql: w, pgSQL: p, exactNumeric: true})
	}
	const tbl = pgRowTable

	// The projection. A field read at the wrong declared type comes back as
	// its TEXT, which the fingerprint sees as a different cell.
	add("RowFieldProjectText",
		`SELECT k, rw.a AS v FROM `+tbl+` ORDER BY k`,
		`SELECT k, (rw).a AS v FROM `+tbl+` ORDER BY k`)
	add("RowFieldProjectInt",
		`SELECT k, rw.b AS v FROM `+tbl+` ORDER BY k`,
		`SELECT k, (rw).b AS v FROM `+tbl+` ORDER BY k`)
	add("RowFieldProjectFloat",
		`SELECT k, rw.f AS v FROM `+tbl+` ORDER BY k`,
		`SELECT k, (rw).f AS v FROM `+tbl+` ORDER BY k`)
	addExact("RowFieldProjectDecimal",
		`SELECT k, rw.n AS v FROM `+tbl+` ORDER BY k`,
		`SELECT k, (rw).n AS v FROM `+tbl+` ORDER BY k`)

	// ORDER BY, both directions. This is the shape the issue was filed on:
	// an INT64 field sorted as TEXT puts 9 after 10 and 192. PostgreSQL's
	// NULL placement — LAST for ASC, FIRST for DESC — is part of the answer
	// and is compared with it.
	add("RowFieldOrderIntAsc",
		`SELECT k, rw.b AS v FROM `+tbl+` ORDER BY rw.b, k`,
		`SELECT k, (rw).b AS v FROM `+tbl+` ORDER BY (rw).b, k`)
	add("RowFieldOrderIntDesc",
		`SELECT k, rw.b AS v FROM `+tbl+` ORDER BY rw.b DESC, k`,
		`SELECT k, (rw).b AS v FROM `+tbl+` ORDER BY (rw).b DESC, k`)
	add("RowFieldOrderText",
		`SELECT k, rw.a AS v FROM `+tbl+` ORDER BY rw.a, k`,
		`SELECT k, (rw).a AS v FROM `+tbl+` ORDER BY (rw).a, k`)

	// GROUP BY and the aggregates, which could not run at all before the
	// fix: the aggregate resolved the field name against its input's columns
	// and missed.
	add("RowFieldGroupInt",
		`SELECT rw.b AS g, COUNT(*) AS n FROM `+tbl+` GROUP BY rw.b ORDER BY g`,
		`SELECT (rw).b AS g, COUNT(*) AS n FROM `+tbl+` GROUP BY (rw).b ORDER BY (rw).b`)
	add("RowFieldMinMaxInt",
		`SELECT MIN(rw.b) AS lo, MAX(rw.b) AS hi, COUNT(rw.b) AS n FROM `+tbl,
		`SELECT MIN((rw).b) AS lo, MAX((rw).b) AS hi, COUNT((rw).b) AS n FROM `+tbl)
	add("RowFieldMinMaxText",
		`SELECT MIN(rw.a) AS lo, MAX(rw.a) AS hi FROM `+tbl,
		`SELECT MIN((rw).a) AS lo, MAX((rw).a) AS hi FROM `+tbl)
	addExact("RowFieldSumDecimal",
		`SELECT SUM(rw.n) AS s FROM `+tbl,
		`SELECT SUM((rw).n) AS s FROM `+tbl)

	// Predicates: the comparison rule follows the FIELD's declaration, so an
	// integer field meets an integer literal and a text field meets a text
	// one.
	add("RowFieldPredicateInt",
		`SELECT k FROM `+tbl+` WHERE rw.b > 100 ORDER BY k`,
		`SELECT k FROM `+tbl+` WHERE (rw).b > 100 ORDER BY k`)
	add("RowFieldPredicateText",
		`SELECT k FROM `+tbl+` WHERE rw.a > 's-100' ORDER BY k`,
		`SELECT k FROM `+tbl+` WHERE (rw).a > 's-100' ORDER BY k`)

	// The two NULL shapes a field path has and a column does not: a NULL
	// FIELD inside a present composite, and a field of a NULL composite.
	// PostgreSQL answers NULL for both, and they must not be conflated.
	add("RowFieldIsNull",
		`SELECT COUNT(*) AS n FROM `+tbl+` WHERE rw.b IS NULL`,
		`SELECT COUNT(*) AS n FROM `+tbl+` WHERE (rw).b IS NULL`)
	add("RowFieldNullContainerVsNullField",
		`SELECT COUNT(*) AS both_null, SUM(CASE WHEN rw IS NULL THEN 1 ELSE 0 END) AS row_null `+
			`FROM `+tbl+` WHERE rw.a IS NULL`,
		`SELECT COUNT(*) AS both_null, SUM(CASE WHEN rw IS NULL THEN 1 ELSE 0 END) AS row_null `+
			`FROM `+tbl+` WHERE (rw).a IS NULL`)

	// The container reached through a DERIVED TABLE and a CTE. A rename-only
	// projection forwards the column, so the field path above it must type
	// exactly as it does at the base — the walk used to stop at any Project
	// and answer nil, so the path fell back to STRING and an aggregate over
	// it could not resolve its input at all.
	add("RowFieldDerivedTable",
		`SELECT k, rw.b AS v FROM (SELECT k, rw FROM `+tbl+`) s ORDER BY k`,
		`SELECT k, (rw).b AS v FROM (SELECT k, rw FROM `+tbl+`) s ORDER BY k`)
	add("RowFieldCTE",
		`WITH s AS (SELECT k, rw FROM `+tbl+`) SELECT k, rw.b AS v FROM s ORDER BY k`,
		`WITH s AS (SELECT k, rw FROM `+tbl+`) SELECT k, (rw).b AS v FROM s ORDER BY k`)
	add("RowFieldDerivedMinMax",
		`SELECT MIN(rw.b) AS lo, MAX(rw.b) AS hi FROM (SELECT rw FROM `+tbl+`) s`,
		`SELECT MIN((rw).b) AS lo, MAX((rw).b) AS hi FROM (SELECT rw FROM `+tbl+`) s`)
	// A field as a FUNCTION ARGUMENT, where the box has to be undone: the
	// engine hands a field over the way a column of its type is handed over,
	// and every family that renders one has to accept a field path too.
	out = append(out, pgCase{
		name:  "RowFieldTextFunctions",
		sql:   `SELECT k, UPPER(rw.a) AS u, LENGTH(rw.a) AS l, CONCAT('x=', rw.b) AS c FROM ` + tbl + ` ORDER BY k`,
		pgSQL: `SELECT k, UPPER((rw).a) AS u, LENGTH((rw).a) AS l, CONCAT('x=', (rw).b) AS c FROM ` + tbl + ` ORDER BY k`,
		// The UPPER and LENGTH columns are GATED and pass; the pin is the
		// CONCAT one, and it is not about field paths at all. PostgreSQL's
		// CONCAT IGNORES a NULL argument — that is what distinguishes it from
		// `||` — and wadjet's propagates, so `CONCAT('x=', <null>)` is NULL
		// here and 'x=' there. A flat column and a bare NULL literal diverge
		// identically; this entry is only where it happened to be caught.
		knownBug: pgBugWadjet + " CONCAT propagates NULL where PostgreSQL ignores it, so " +
			"CONCAT('x=', <null field>) is NULL here and 'x=' there. Not field-path specific: " +
			"CONCAT('a', NULL, 'b') answers NULL over a flat column and a bare literal too",
		issue: "#609",
	})
	// Arithmetic over an integer field stays INTEGER, the way it does over an
	// integer column (#297's rule).
	add("RowFieldIntArithmetic",
		`SELECT k, rw.b + 1 AS v FROM `+tbl+` WHERE rw.b IS NOT NULL ORDER BY k`,
		`SELECT k, (rw).b + 1 AS v FROM `+tbl+` WHERE (rw).b IS NOT NULL ORDER BY k`)
	// BETWEEN and IN over a field path, which failed outright with
	// `filter column "rw.b" does not exist in the input schema`.
	add("RowFieldBetween",
		`SELECT k FROM `+tbl+` WHERE rw.b BETWEEN 20 AND 120 ORDER BY k`,
		`SELECT k FROM `+tbl+` WHERE (rw).b BETWEEN 20 AND 120 ORDER BY k`)
	add("RowFieldIn",
		`SELECT k FROM `+tbl+` WHERE rw.b IN (0, 37, 74) ORDER BY k`,
		`SELECT k FROM `+tbl+` WHERE (rw).b IN (0, 37, 74) ORDER BY k`)

	// A CAST off a field path, which reads the value through the typed route
	// rather than the boxed one.
	add("RowFieldCastIntToText",
		`SELECT k, CAST(rw.b AS VARCHAR) AS v FROM `+tbl+` WHERE rw.b IS NOT NULL ORDER BY k`,
		`SELECT k, CAST((rw).b AS VARCHAR) AS v FROM `+tbl+` WHERE (rw).b IS NOT NULL ORDER BY k`)

	return out
}

// postgresSemanticsCases is the half of the corpus DuckDB cannot answer for.
// Grouped by the rule each family pins.
// polymorphicOverDecimalCase is PolymorphicOverFloatColumns asked of the
// column the DECIMAL variant retypes. Under the FLOAT64 fixture it is the
// same question over a float and gated normally; under TPCH_DECIMAL=1 it is
// the CHOICE family over a DECIMAL column beside an integer — #695's shape —
// and pinned, so the pin flips to a failure the day that lands.
func polymorphicOverDecimalCase() pgCase {
	c := pgCase{name: "PolymorphicOverSupplycost", sql: `SELECT ps_partkey, ps_suppkey,
			COALESCE(ps_supplycost, 0) AS coalesce_num,
			GREATEST(ps_supplycost, ps_availqty) AS greatest_mixed,
			LEAST(ps_supplycost, ps_availqty) AS least_mixed
			FROM partsupp WHERE ps_partkey <= 20 ORDER BY ps_partkey, ps_suppkey`}
	// Pinned to #695 under TPCH_DECIMAL=1 until the choice constructs folded to
	// their branches' common DECIMAL type: COALESCE/GREATEST/LEAST over
	// ps_supplycost declared the OTHER branch's type and either raised the #361
	// store guard or answered under the wrong one. Gated on both fixtures now.
	return c
}

func postgresSemanticsCases() []pgCase {
	var out []pgCase

	// --- NULL placement in ORDER BY -------------------------------------
	//
	// PostgreSQL's rule — NULLS LAST for ASC, NULLS FIRST for DESC — is the
	// one Wadjet claims to implement, and the DuckDB arm has to be CONFIGURED
	// into it (default_null_order). Here it is simply asked of the engine that
	// defines it, which is the difference between a convention and a citation.
	out = append(out,
		// #593 / #594 — a comma FROM item mixed with an explicit JOIN ... ON
		// whose equi-predicate is in the WHERE. PostgreSQL is the authority
		// here (ADR-0012) and the shape is not ambiguous: comma-separated
		// FROM items are cross-joined, an explicit JOIN belongs to the item
		// it follows, and an inner join's WHERE equality is a join
		// predicate. Wadjet planned `FROM a JOIN b ON …, c` as `(a x c) ⋈ b`,
		// stranded the WHERE equality on a subtree that no longer held the
		// relation it names, and answered ZERO ROWS — with SUM answering
		// NULL, which is what an empty input legitimately produces, so
		// nothing about the result said the join was wrong.
		pgCase{name: "CommaJoinMixedWhereEquality",
			sql: `SELECT COUNT(*) AS n FROM supplier t0 JOIN partsupp t1 ON t0.s_suppkey = t1.ps_suppkey,
				part t2 WHERE t1.ps_partkey = t2.p_partkey`},
		pgCase{name: "CommaJoinMixedWhereEqualitySum",
			sql: `SELECT SUM(t1.ps_availqty) AS s FROM supplier t0 JOIN partsupp t1 ON t0.s_suppkey = t1.ps_suppkey,
				part t2 WHERE t1.ps_partkey = t2.p_partkey`},
		pgCase{name: "CommaJoinMixedOrderedRows",
			sql: `SELECT t1.ps_partkey, t1.ps_suppkey, t2.p_size
				FROM supplier t0 JOIN partsupp t1 ON t0.s_suppkey = t1.ps_suppkey, part t2
				WHERE t1.ps_partkey = t2.p_partkey AND t1.ps_partkey <= 5
				ORDER BY t1.ps_partkey, t1.ps_suppkey`},
		pgCase{name: "CommaJoinBeforeOnJoinWhereEquality",
			sql: `SELECT COUNT(*) AS n FROM part t2, supplier t0 JOIN partsupp t1 ON t0.s_suppkey = t1.ps_suppkey
				WHERE t1.ps_partkey = t2.p_partkey`},
		pgCase{name: "CommaJoinTwoJoinedItems",
			sql: `SELECT COUNT(*) AS n FROM supplier t0 JOIN partsupp t1 ON t0.s_suppkey = t1.ps_suppkey,
				nation n JOIN region r ON n.n_regionkey = r.r_regionkey
				WHERE t0.s_nationkey = n.n_nationkey`},
		pgCase{name: "CommaJoinMixedDerivedTable",
			sql: `SELECT COUNT(*) AS n FROM supplier t0 JOIN partsupp t1 ON t0.s_suppkey = t1.ps_suppkey,
				(SELECT p_partkey AS pk FROM part) d WHERE t1.ps_partkey = d.pk`},
		// The same relation twice under two aliases (TPC-H Q7/Q8's FROM
		// shape) and a CYCLE in the join graph (Q5's), both in comma form.
		pgCase{name: "CommaJoinSelfAliasedRelation",
			sql: `SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, COUNT(*) AS c
				FROM supplier, lineitem, orders, customer, nation n1, nation n2
				WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey AND c_custkey = o_custkey
					AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey
					AND l_shipdate >= '1995-01-01' AND l_shipdate <= '1995-03-31'
				GROUP BY n1.n_name, n2.n_name ORDER BY supp_nation, cust_nation`},
		pgCase{name: "CommaJoinGraphCycle",
			sql: `SELECT n_name, COUNT(*) AS c
				FROM customer, orders, lineitem, supplier, nation, region
				WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey AND l_suppkey = s_suppkey
					AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
					AND c_nationkey = s_nationkey AND r_name = 'ASIA'
				GROUP BY n_name ORDER BY n_name`},
		// An OR is not an equi-conjunct and must not become a join key; a
		// non-equi conjunct alongside a lifted one stays a filter.
		pgCase{name: "CommaJoinMixedOrIsNotLifted",
			sql: `SELECT COUNT(*) AS n FROM supplier t0 JOIN partsupp t1 ON t0.s_suppkey = t1.ps_suppkey,
				part t2 WHERE t1.ps_partkey = t2.p_partkey OR t2.p_partkey = 1`},
		pgCase{name: "CommaJoinMixedNonEquiResidual",
			sql: `SELECT COUNT(*) AS n FROM supplier t0 JOIN partsupp t1 ON t0.s_suppkey = t1.ps_suppkey,
				part t2 WHERE t1.ps_partkey = t2.p_partkey AND t2.p_size > 20`},
		// Genuine cross products: small ones must answer, and one mixed with
		// an equi-join must not be dropped (#281) or invented into a join.
		pgCase{name: "CommaJoinPureCrossProduct",
			sql: `SELECT COUNT(*) AS n FROM nation a, region b`},
		pgCase{name: "CommaJoinGenuineCrossAlongsideEqui",
			sql: `SELECT COUNT(*) AS n FROM supplier t0, partsupp t1, region t2 WHERE t0.s_suppkey = t1.ps_suppkey`},
		pgCase{name: "CommaJoinCrossBetweenEquiJoinedPair",
			sql: `SELECT COUNT(*) AS n FROM nation a, region x, supplier b WHERE a.n_nationkey = b.s_nationkey`},
		pgCase{name: "CommaJoinAfterLeftJoinItem",
			sql: `SELECT COUNT(*) AS n FROM supplier t0 LEFT JOIN partsupp t1 ON t0.s_suppkey = t1.ps_suppkey,
				region t2 WHERE t2.r_regionkey = t0.s_nationkey`},
		// F1 LEFT-JOIN control: an outer join beside a comma, ON confined to
		// its own two sides. Valid in PostgreSQL (unlike a cross-item ON,
		// which PG rejects as an invalid FROM-clause reference — so the
		// bare-cross-item-ON shapes themselves are gated only against DuckDB
		// and the two-path oracle, not here). The fix must not fold the outer
		// join's left; PG preserves the unmatched customers.
		pgCase{name: "CommaBesideLeftJoinControl",
			sql: `SELECT COUNT(*) AS n FROM nation, customer LEFT JOIN orders ON c_custkey = o_custkey
				WHERE c_nationkey = n_nationkey`},

		pgCase{name: "NullOrderAscDefault", sql: `SELECT NULLIF(n_regionkey, 1) AS k, n_name FROM nation ORDER BY k, n_name`},
		pgCase{name: "NullOrderDescDefault", sql: `SELECT NULLIF(n_regionkey, 1) AS k, n_name FROM nation ORDER BY k DESC, n_name`},
		pgCase{name: "NullOrderAscNullsFirst", sql: `SELECT NULLIF(n_regionkey, 1) AS k, n_name FROM nation ORDER BY k ASC NULLS FIRST, n_name`},
		pgCase{name: "NullOrderAscNullsLast", sql: `SELECT NULLIF(n_regionkey, 1) AS k, n_name FROM nation ORDER BY k ASC NULLS LAST, n_name`},
		pgCase{name: "NullOrderDescNullsFirst", sql: `SELECT NULLIF(n_regionkey, 1) AS k, n_name FROM nation ORDER BY k DESC NULLS FIRST, n_name`},
		pgCase{name: "NullOrderDescNullsLast", sql: `SELECT NULLIF(n_regionkey, 1) AS k, n_name FROM nation ORDER BY k DESC NULLS LAST, n_name`},
		// Placement resolved per KEY, not per query: the two keys disagree on
		// direction and on placement.
		pgCase{name: "NullOrderSecondKey", sql: `SELECT n_regionkey AS r, NULLIF(n_nationkey % 5, 1) AS k, n_name
			FROM nation ORDER BY r DESC, k DESC NULLS LAST, n_name`},
	)

	// --- DECIMAL predicates ---------------------------------------------
	//
	// The gap #438 fell through. TPC-H models every monetary column as
	// FLOAT64, so no arm of this oracle had ever put a predicate on a DECIMAL
	// column, and `WHERE v = 0.25` on a DECIMAL(9,2) holding 0.25 returned no
	// rows — the row-group prune compared the literal against the UNSCALED
	// bound (#442). PostgreSQL's `numeric` is what a client means by these,
	// so it is the one that answers.
	//
	// dec_probe is monotonic in both columns over small row groups, and the
	// literals are chosen from the LAST row group, whose unscaled bounds
	// (1250..2475 for d_2) sit far above the literal's scaled value — which is
	// what makes a raw comparison prune the row group holding the answer. A
	// literal from a row group whose bounds straddle zero is absorbed and
	// proves nothing. The entries project
	// d_key rather than the decimal itself on purpose: PostgreSQL hands a
	// numeric to the driver and Wadjet renders it as text, which is a
	// difference of BOXING that the wire arm reports as a type OID — asking it
	// here would report a divergence about neither engine's arithmetic.
	out = append(out,
		pgCase{name: "DecimalEqScale2", sql: `SELECT d_key FROM dec_probe WHERE d_2 = 12.75 ORDER BY d_key`},
		pgCase{name: "DecimalEqScale2Negative", sql: `SELECT d_key FROM dec_probe WHERE d_2 = -20.00 ORDER BY d_key`},
		pgCase{name: "DecimalEqScale2Zero", sql: `SELECT d_key FROM dec_probe WHERE d_2 = 0 ORDER BY d_key`},
		// The same value written with more and with fewer decimals than the
		// column carries. 12.750 IS 12.75; 12.751 is a value the column cannot
		// hold, and PostgreSQL matches nothing rather than rounding.
		pgCase{name: "DecimalEqTrailingZeros", sql: `SELECT d_key FROM dec_probe WHERE d_2 = 12.7500 ORDER BY d_key`},
		pgCase{name: "DecimalEqPastScale", sql: `SELECT d_key FROM dec_probe WHERE d_2 = 12.751 ORDER BY d_key`},
		pgCase{name: "DecimalEqIntegerLiteral", sql: `SELECT d_key FROM dec_probe WHERE d_2 = 20 ORDER BY d_key`},
		pgCase{name: "DecimalNe", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 <> 12.75`},
		pgCase{name: "DecimalLt", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 < 12.75`},
		pgCase{name: "DecimalLe", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 <= 12.75`},
		pgCase{name: "DecimalGt", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 > 12.75`},
		pgCase{name: "DecimalGe", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 >= 12.75`},
		pgCase{name: "DecimalIn", sql: `SELECT d_key FROM dec_probe WHERE d_2 IN (12.75, -20.00, 0.25) ORDER BY d_key`},
		pgCase{name: "DecimalBetween", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 BETWEEN 12.75 AND 20.00`},
		pgCase{name: "DecimalCase", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE WHEN d_2 = 12.75 THEN 1 ELSE 0 END = 1`},
		pgCase{name: "DecimalIsNull", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 IS NULL`},
		// Scale 4, so the conversion is not pinned to one scale.
		pgCase{name: "DecimalEqScale4", sql: `SELECT d_key FROM dec_probe WHERE d_4 = 3.1875 ORDER BY d_key`},
		pgCase{name: "DecimalGeScale4", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_4 >= 3.1875`},
		pgCase{name: "DecimalTwoColumns", sql: `SELECT d_key FROM dec_probe WHERE d_2 = 12.75 AND d_4 >= 0 ORDER BY d_key`},

		// FLOAT32 (real) IN-list — #549. PostgreSQL's multi-element
		// `real IN (...)` narrows the literals to real[] (EXPLAIN VERBOSE:
		// `= ANY('{...}'::real[])`), so a literal not exactly representable in
		// float32 still matches its row. r_val holds real(i)+0.1, so these
		// literals are the non-representable case the bug turned up on.
		pgCase{name: "RealInNonRepresentable", sql: `SELECT r_key FROM real_probe WHERE r_val IN (0.1, 3.1, 7.1) ORDER BY r_key`},
		pgCase{name: "RealInRepresentable", sql: `SELECT r_key FROM real_probe WHERE r_val IN (0.5, 1.5) ORDER BY r_key`},
		pgCase{name: "RealInMixed", sql: `SELECT r_key FROM real_probe WHERE r_val IN (0.1, 0.5, 9.1) ORDER BY r_key`},
		pgCase{name: "RealNotIn", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE r_val NOT IN (0.1, 3.1)`},
		// Representable, so `=` agrees on both sides (1.5 is exact in float32).
		pgCase{name: "RealEqRepresentable", sql: `SELECT r_key FROM real_probe WHERE r_val = 1.5 ORDER BY r_key`},

		// SCALAR comparison against a numeric literal WIDENS the column to
		// double — the other half of the arity rule, and the opposite width
		// from the multi-element IN entries above (#631). PostgreSQL has no
		// `real <op> numeric` operator to resolve to, so the comparison goes
		// through float8 and the COLUMN is what moves:
		//
		//	real = 3.1  ->  Filter: (r_val = '3.1'::double precision)
		//
		// All six operators, because this is not only an equality question:
		// float32(3)+0.1 widens to 3.0999999046325684, which is BELOW 3.1, so
		// row 3 leaves `=`, joins `<`/`<=`, and leaves `>=`. Under the
		// narrowing this replaced, four of the six answered differently.
		pgCase{name: "RealEqNonRepresentable", sql: `SELECT r_key FROM real_probe WHERE r_val = 3.1 ORDER BY r_key`},
		pgCase{name: "RealNeNonRepresentable", sql: `SELECT r_key FROM real_probe WHERE r_val <> 3.1 ORDER BY r_key`},
		pgCase{name: "RealLtNonRepresentable", sql: `SELECT r_key FROM real_probe WHERE r_val < 3.1 ORDER BY r_key`},
		pgCase{name: "RealLeNonRepresentable", sql: `SELECT r_key FROM real_probe WHERE r_val <= 3.1 ORDER BY r_key`},
		pgCase{name: "RealGtNonRepresentable", sql: `SELECT r_key FROM real_probe WHERE r_val > 3.1 ORDER BY r_key`},
		pgCase{name: "RealGeNonRepresentable", sql: `SELECT r_key FROM real_probe WHERE r_val >= 3.1 ORDER BY r_key`},
		// The same number with a trailing zero is the same literal: both plan
		// as '3.1'::double precision.
		pgCase{name: "RealEqTrailingZero", sql: `SELECT r_key FROM real_probe WHERE r_val = 3.10 ORDER BY r_key`},
		pgCase{name: "RealBetweenNonRepresentable", sql: `SELECT r_key FROM real_probe WHERE r_val BETWEEN 3.1 AND 4.1 ORDER BY r_key`},
		// An INTEGER literal is widened too — `r_val = 3` plans as
		// '3'::double precision. 16777217 is exact in double and not in real,
		// so it matches nothing; narrowing would round it onto row 20 (2^24).
		// The exact companion proves that row is reachable at all.
		pgCase{name: "RealEqIntegerPastMantissa", sql: `SELECT r_key FROM real_probe WHERE r_val = 16777217 ORDER BY r_key`},
		pgCase{name: "RealEqIntegerExact", sql: `SELECT r_key FROM real_probe WHERE r_val = 16777216 ORDER BY r_key`},
		// The same literal through a MULTI-element IN, which narrows instead
		// and DOES match row 20. Scalar `=` and multi-element IN disagreeing
		// on one literal is the property that forbids lowering either to the
		// other, on any path.
		pgCase{name: "RealInIntegerPastMantissa", sql: `SELECT r_key FROM real_probe WHERE r_val IN (16777217, 99) ORDER BY r_key`},
		// A finite literal past real's range is an ordinary double for a
		// scalar comparison: no row equals it, everything is below it, and
		// PostgreSQL raises NO error (the 22003 belongs to the multi-element
		// IN, which casts the array to real[] — RealInOverflow*, #549).
		pgCase{name: "RealEqOverRange", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE r_val = 1e40`},
		pgCase{name: "RealLtOverRange", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE r_val < 1e40`},
		// COLUMN against COLUMN: no literal, so nothing decides a width and
		// #631 must leave both exactly where they were. PostgreSQL compares
		// real to real directly, and real to double by widening the real — so
		// only the rows whose value survives the round trip match the second.
		pgCase{name: "RealEqRealColumn", sql: `SELECT r_key FROM real_probe WHERE r_val = r_other ORDER BY r_key`},
		pgCase{name: "RealEqDoubleColumn", sql: `SELECT r_key FROM real_probe WHERE r_val = d_val ORDER BY r_key`},
		pgCase{name: "RealLtDoubleColumn", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE r_val < d_val`},
		// Keyed operations over the same column, for the same reason: a group
		// key, a DISTINCT and an ORDER BY compare stored value to stored value
		// at real width, with no literal in sight.
		pgCase{name: "RealGroupBy", sql: `SELECT COUNT(*) AS n FROM (SELECT r_val FROM real_probe GROUP BY r_val) s`},
		pgCase{name: "RealDistinct", sql: `SELECT COUNT(*) AS n FROM (SELECT DISTINCT r_val FROM real_probe) s`},
		pgCase{name: "RealOrderBy", sql: `SELECT r_key FROM real_probe ORDER BY r_val, r_key`},

		// An explicit CAST TO REAL narrows, where the bare literal widens: the
		// same number, two answers, decided by whether the cast is written
		// (EXPLAIN VERBOSE). `CAST(x AS REAL)` used to be a NO-OP in wadjet —
		// the arm answered ToFloat64 beside "float" and "double" — so all
		// three of these read as if the cast were absent.
		//
		//	r_val = CAST(3.1 AS REAL)  ->  Filter: (r_val = '3.1'::real)
		//	r_val IN (CAST(3.1 AS REAL), 7.1)
		//	                           ->  Filter: (r_val = ANY ('{3.1,7.1}'::real[]))
		//	d_val = CAST(3.1 AS REAL)  ->  Filter: (d_val = '3.1'::real)
		//
		// The DOUBLE entry is the proof the narrowing really happened: that
		// column holds 3.1 exactly and stops matching once the literal has
		// been through float4.
		pgCase{name: "RealEqCastReal", sql: `SELECT r_key FROM real_probe WHERE r_val = CAST(3.1 AS REAL) ORDER BY r_key`},
		pgCase{name: "RealInCastReal", sql: `SELECT r_key FROM real_probe WHERE r_val IN (CAST(3.1 AS REAL), 7.1) ORDER BY r_key`},
		pgCase{name: "DoubleEqCastReal", sql: `SELECT r_key FROM real_probe WHERE d_val = CAST(3.1 AS REAL) ORDER BY r_key`},

		// The array cast follows the OPERAND'S TYPE, not "is it a column".
		// PostgreSQL resolves the list's element type over the members AND the
		// probed expression, so any real-typed left operand pulls the array to
		// real[] (EXPLAIN VERBOSE):
		//
		//	-r_val IN (-3.1, -7.1)          -> ((- r_val) = ANY ('{-3.1,-7.1}'::real[]))
		//	CAST(d_val AS REAL) IN (3.1,…)  -> ((d_val)::real = ANY ('{3.1,7.1}'::real[]))
		//	(r_val + 0) IN (3.1, 7.1)       -> (… = ANY ('{3.1,7.1}'::double precision[]))
		//
		// The last is the control: an integer literal added to a real gives
		// DOUBLE PRECISION there (pg_typeof), so that shape STAYS widened and
		// matches nothing — which is why the rule cannot be "walk down to a
		// column". The `=` companions are the scalar halves, which widen
		// whatever the operand is.
		pgCase{name: "RealInNegatedOperand", sql: `SELECT r_key FROM real_probe WHERE -r_val IN (-3.1, -7.1) ORDER BY r_key`},
		pgCase{name: "RealEqNegatedOperand", sql: `SELECT r_key FROM real_probe WHERE -r_val = -3.1 ORDER BY r_key`},
		pgCase{name: "RealInCastToRealOperand", sql: `SELECT r_key FROM real_probe WHERE CAST(d_val AS REAL) IN (3.1, 7.1) ORDER BY r_key`},
		pgCase{name: "RealEqCastToRealOperand", sql: `SELECT r_key FROM real_probe WHERE CAST(d_val AS REAL) = 3.1 ORDER BY r_key`},
		pgCase{name: "RealInPlusZeroOperand", sql: `SELECT r_key FROM real_probe WHERE (r_val + 0) IN (3.1, 7.1) ORDER BY r_key`},
		// A NEGATIVE member is still a constant: the sign is a unary operator
		// over the literal in the AST, and reading that as "not a constant"
		// took the narrowing — and the 22003 refusal — away from the whole
		// list. PostgreSQL narrows exactly as it does for a positive member.
		pgCase{name: "RealInNegativeMember", sql: `SELECT r_key FROM real_probe WHERE r_val IN (-1.0, 3.1) ORDER BY r_key`},
		// real's smallest denormal is about 1.4e-45, so 1e-45 is representable
		// and the list narrows around it. Its neighbour 1e-46 is NOT, and
		// PostgreSQL refuses that whole predicate with 22003 — an error the
		// oracle cannot express as a row set, gated instead by
		// coordinator.TestRealInOverRangeLiteralRaisesOnBothPaths.
		pgCase{name: "RealInDenormalBoundary", sql: `SELECT r_key FROM real_probe WHERE r_val IN (1e-45, 3.1) ORDER BY r_key`},

		// A set operation reconciles its arms' types, which is where a
		// newly-narrowing CAST TO REAL could go wrong in either direction — by
		// widening straight back, or by dragging the other arm down. Live on
		// postgres:17 (pg_typeof over the union's output plus the values):
		//
		//	real UNION ALL double precision  ->  double precision
		//	real UNION ALL bigint            ->  real
		//	real UNION ALL real              ->  real
		//
		// so the real arm's values WIDEN in the first (3.1 as a real prints
		// 3.0999999046325684 once it is a double) and the integer arm NARROWS
		// in the second. The wire arm reads the OID as well as the value,
		// which is the half a value oracle cannot see.
		pgCase{name: "RealUnionDouble", sql: `SELECT v FROM (SELECT CAST(r_val AS REAL) AS v FROM real_probe WHERE r_key IN (3,7) UNION ALL SELECT d_val FROM real_probe WHERE r_key = 3) s ORDER BY v`},
		pgCase{name: "RealUnionInteger", sql: `SELECT v FROM (SELECT CAST(d_val AS REAL) AS v FROM real_probe WHERE r_key IN (3,7) UNION ALL SELECT r_key FROM real_probe WHERE r_key = 5) s ORDER BY v`},
		pgCase{name: "RealUnionReal", sql: `SELECT v FROM (SELECT CAST(3.1 AS REAL) AS v FROM real_probe WHERE r_key = 0 UNION ALL SELECT CAST(7.1 AS REAL) FROM real_probe WHERE r_key = 0) s ORDER BY v`},
		// A filter ABOVE a real-typed union: the literal widens against the
		// union's real output exactly as it does against a real column, so the
		// non-representable 3.1 matches nothing.
		pgCase{name: "RealFilterAboveUnion", sql: `SELECT COUNT(*) AS n FROM (SELECT CAST(d_val AS REAL) AS v FROM real_probe UNION ALL SELECT CAST(d_val AS REAL) FROM real_probe) s WHERE s.v = 3.1`},

		// An INTEGER literal against a float column keeps PostgreSQL's float
		// TOTAL order — NaN greatest and equal to itself (ADR-0012 item 8).
		// The row-at-a-time comparison used Go's IEEE operators for a mixed
		// int/float pair while the both-float64 path beside it did not, so
		// `r_val > 1` dropped the NaN row and `r_val > 1.0` kept it: one
		// predicate, two answers, decided by the literal's spelling. Both
		// spellings are here so the pair cannot drift apart again, and the
		// FLOAT64 column is here because it lost the same order the same way.
		pgCase{name: "RealGtIntegerLiteral", sql: `SELECT r_key FROM real_probe WHERE r_val > 1 ORDER BY r_key`},
		pgCase{name: "RealGtFloatLiteral", sql: `SELECT r_key FROM real_probe WHERE r_val > 1.0 ORDER BY r_key`},
		pgCase{name: "RealGeIntegerLiteral", sql: `SELECT r_key FROM real_probe WHERE r_val >= 1 ORDER BY r_key`},
		pgCase{name: "RealLtIntegerLiteral", sql: `SELECT r_key FROM real_probe WHERE r_val < 1 ORDER BY r_key`},
		pgCase{name: "RealNeIntegerLiteral", sql: `SELECT r_key FROM real_probe WHERE r_val <> 1 ORDER BY r_key`},
		pgCase{name: "DoubleGtIntegerLiteral", sql: `SELECT r_key FROM real_probe WHERE d_val > 1 ORDER BY r_key`},
		pgCase{name: "DoubleNeIntegerLiteral", sql: `SELECT r_key FROM real_probe WHERE d_val <> 1 ORDER BY r_key`},

		// A QUOTED constant is the other half of the width rule, and it goes
		// the OTHER way (#646). PostgreSQL types an unknown-typed literal
		// FROM the other operand and coerces it with THAT type's own input
		// function, so a quoted constant against a real column NARROWS where
		// the numeric literal above widens:
		//
		//	r_val = 3.1    ->  Filter: (r_val = '3.1'::double precision)
		//	r_val = '3.1'  ->  Filter: (r_val = '3.1'::real)
		//
		// Both spellings are here, side by side, because they are two
		// predicates over one number: an engine that widens both, or narrows
		// both, fails one of them. Wadjet did NEITHER — kernel.toFloat64 has
		// no string arm, so every quoted constant read as 0.0 and these
		// selected the row holding 0.0. These three were the pins; they are
		// deleted, which is the fix's proof.
		pgCase{name: "RealEqQuotedNumeric", sql: `SELECT r_key FROM real_probe WHERE r_val = '3.1' ORDER BY r_key`},
		pgCase{name: "RealInQuotedNumeric", sql: `SELECT r_key FROM real_probe WHERE r_val IN ('3.1', '7.1') ORDER BY r_key`},
		pgCase{name: "DoubleEqQuotedNumeric", sql: `SELECT r_key FROM real_probe WHERE d_val = '3.1' ORDER BY r_key`},
		// The rest of the shape table, over the same fixture. The SINGLE
		// element IN narrows for a quoted member and widens for a numeric one
		// (`r IN ('3.1')` is `r = '3.1'::real`, `r IN (3.1)` is `r =
		// '3.1'::double precision`), and 16777217 is exact in double and not
		// in real — so the quoted spelling rounds onto the 2^24 row and the
		// unquoted one matches nothing.
		pgCase{name: "RealInSingleQuotedNumeric", sql: `SELECT r_key FROM real_probe WHERE r_val IN ('3.1') ORDER BY r_key`},
		pgCase{name: "RealLtQuotedNumeric", sql: `SELECT r_key FROM real_probe WHERE r_val < '3.1' ORDER BY r_key`},
		pgCase{name: "RealGeQuotedNumeric", sql: `SELECT r_key FROM real_probe WHERE r_val >= '3.1' ORDER BY r_key`},
		pgCase{name: "RealBetweenQuotedNumeric", sql: `SELECT r_key FROM real_probe WHERE r_val BETWEEN '3.1' AND '4.1' ORDER BY r_key`},
		pgCase{name: "RealEqQuotedIntPastMantissa", sql: `SELECT r_key FROM real_probe WHERE r_val = '16777217' ORDER BY r_key`},
		pgCase{name: "RealEqQuotedZero", sql: `SELECT r_key FROM real_probe WHERE r_val = '0' ORDER BY r_key`},
		// PostgreSQL's float input is strtod, so a C99 HEX float is a value
		// there. Refusing it would be a PG-superset regression.
		pgCase{name: "RealLtQuotedHexFloat", sql: `SELECT r_key FROM real_probe WHERE r_val < '0x1p3' ORDER BY r_key`},
		// C whitespace is trimmed at both ends.
		pgCase{name: "RealLtQuotedSpaced", sql: `SELECT r_key FROM real_probe WHERE r_val < ' 3.1 ' ORDER BY r_key`},
		// The BOXED sites, where the column arrives as a Go box and the
		// literal as its text: every one of them answered every row.
		pgCase{name: "RealSimpleCaseQuoted", sql: `SELECT r_key FROM real_probe WHERE CASE r_val WHEN '3.1' THEN 1 ELSE 0 END = 1 ORDER BY r_key`},
		pgCase{name: "RealIsDistinctQuoted", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE r_val IS DISTINCT FROM '3.1'`},
		pgCase{name: "RealGreatestQuoted", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE GREATEST(r_val, '3.1') = '3.1'`},
		pgCase{name: "RealNullifQuoted", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE NULLIF(r_val, '3.1') IS NULL`},
		pgCase{name: "DoubleCaseLtQuoted", sql: `SELECT r_key FROM real_probe WHERE CASE WHEN d_val < '3.1' THEN 1 ELSE 0 END = 1 ORDER BY r_key`},
		// The INTEGER column, including #634's radix and underscore forms —
		// PostgreSQL reads '0x0A' and '1_0' as ten, and refusing them was a
		// PG-superset regression of its own.
		pgCase{name: "BigintEqQuoted", sql: `SELECT r_key FROM real_probe WHERE r_key = '3' ORDER BY r_key`},
		pgCase{name: "BigintInQuoted", sql: `SELECT r_key FROM real_probe WHERE r_key IN ('3', '7') ORDER BY r_key`},
		pgCase{name: "BigintCaseLtQuoted", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE CASE WHEN r_key < '3' THEN 1 ELSE 0 END = 1`},
		pgCase{name: "BigintGreatestQuoted", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE GREATEST(r_key, '3') = '3'`},
		pgCase{name: "BigintNullifQuoted", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE NULLIF(r_key, '3') IS NULL`},
		pgCase{name: "BigintEqQuotedUnderscore", sql: `SELECT r_key FROM real_probe WHERE r_key = '1_0' ORDER BY r_key`},
		pgCase{name: "BigintEqQuotedHex", sql: `SELECT r_key FROM real_probe WHERE r_key = '0x0A' ORDER BY r_key`},

		// A COMPOSITE resolves ONE type over every argument
		// (select_common_type) and coerces the unknown-typed literal to THAT
		// — not to whichever argument the comparison happens to pair it with.
		// `GREATEST(bigint, '3.1', double)` folds to double precision and
		// ANSWERS; asking bigint's input function for '3.1' is 22P02, which is
		// a PG-SUPERSET regression, and reading the width off the row's BOX
		// makes the answer depend on the DATA (the NULL row of
		// `COALESCE(real, 0)` boxes an int64 and every other row a float64).
		// All three shapes are here, plus a permutation, because the fold is
		// over the SET of arguments and argument order changes nothing.
		pgCase{name: "GreatestIntQuotedFracDouble", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE GREATEST(r_key, '3.1', d_val) > 0`},
		pgCase{name: "LeastIntQuotedFracDouble", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE LEAST(r_key, '3.1', d_val) > 0`},
		pgCase{name: "GreatestDoubleQuotedFracInt", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE GREATEST(d_val, '3.1', r_key) > 0`},
		// The literal is out of REAL's range and inside DOUBLE's: a value
		// under the call's double fold, a 22003 under the real column's own.
		pgCase{name: "GreatestRealQuotedOverReal", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE GREATEST(r_val, '1e39', d_val) > 0`},
		pgCase{name: "LeastRealQuotedOverReal", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE LEAST(r_val, '1e39', d_val) > 0`},
		// No literal inside the composite: its own folded type is what the
		// OUTER quoted literal is coerced to. real ∪ bigint is real, so '3.1'
		// narrows and finds the row.
		pgCase{name: "GreatestIntRealEqQuoted", sql: `SELECT r_key FROM real_probe WHERE GREATEST(r_key, r_val) = '3.1' ORDER BY r_key`},
		pgCase{name: "CaseIntRealEqQuoted", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE CASE WHEN r_key > 0 THEN r_key ELSE r_val END = '3.1'`},
		pgCase{name: "CoalesceRealIntEqQuoted", sql: `SELECT r_key FROM real_probe WHERE COALESCE(r_val, 0) = '3.1' ORDER BY r_key`},
		pgCase{name: "CoalesceRealIntEqQuotedBig", sql: `SELECT r_key FROM real_probe WHERE COALESCE(r_val, 0) = '16777217' ORDER BY r_key`},
		pgCase{name: "NullifGreatestIntRealQuoted", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE NULLIF(GREATEST(r_key, r_val), '3.1') IS NULL`},
		// WIDTH in both directions over ONE literal: the three-argument form
		// folds to double, where 16777217 is exact and beats the bound; the
		// two-argument form folds to real, where it rounds onto 2^24.
		pgCase{name: "GreatestRealQuotedIntDouble", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE GREATEST(r_val, '16777217', d_val) <= 16777216.5`},
		pgCase{name: "GreatestRealOnlyQuotedInt", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE GREATEST(r_val, '16777217') <= 16777216.5`},
		// A NUMERIC-typed CONSTANT arm. PostgreSQL types an unsuffixed
		// constant `numeric` as soon as it carries a decimal point or an
		// exponent, so `COALESCE(real, 0.0)` is real ∪ numeric — REAL — and
		// `COALESCE(bigint, 0.0)` is numeric. DECIMAL had no rung on the
		// engine's fold ladder, so both folds failed and the comparison went
		// BYTEWISE against the literal.
		pgCase{name: "CoalesceRealNumericConstGt", sql: `SELECT r_key FROM real_probe WHERE COALESCE(r_val, 0.0) > '9' ORDER BY r_key`},
		pgCase{name: "CoalesceRealNumericConstExp", sql: `SELECT r_key FROM real_probe WHERE COALESCE(r_val, 1e0) > '9' ORDER BY r_key`},
		pgCase{name: "GreatestRealNumericConst", sql: `SELECT r_key FROM real_probe WHERE GREATEST(r_val, 0.0) > '9' ORDER BY r_key`},
		pgCase{name: "CaseRealNumericConst", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE CASE WHEN r_key > 0 THEN r_val ELSE 0.0 END > '9'`},
		pgCase{name: "CoalesceIntNumericConst", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE COALESCE(r_key, 0.0) > '9'`},
		pgCase{name: "CoalesceRealNumericConstEq", sql: `SELECT r_key FROM real_probe WHERE COALESCE(r_val, 0.0) = '3.1' ORDER BY r_key`},
		// A DECIMAL COLUMN inside a composite — the same rung, reached from
		// the other side. No corpus entry mixed one into a composite before.
		pgCase{name: "GreatestIntQuotedDecimal", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE GREATEST(d_key, '3.1', d_2) > 0`},
		pgCase{name: "LeastIntQuotedDecimal", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE LEAST(d_key, '3.1', d_2) > 0`},
		pgCase{name: "GreatestDecimalQuotedInt", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE GREATEST(d_2, '3.1', d_key) > 0`},
		// A literal past every FLOAT range, and a NaN, are VALUES against a
		// numeric fold: the carrier saturates them into their place in the
		// order (ADR-0024 item 6) rather than refusing them.
		pgCase{name: "GreatestIntQuotedOverCarrier", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE GREATEST(d_key, '1e39', d_2) > 0`},
		pgCase{name: "GreatestIntQuotedNaNDecimal", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE GREATEST(d_key, 'NaN', d_2) > 0`},
		pgCase{name: "GreatestIntDecimalEqQuoted", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE GREATEST(d_key, d_2) = '12.75'`},
		pgCase{name: "CoalesceDecimalNumericConst", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE COALESCE(d_2, 0.0) > '9'`},
		// A DECIMAL column and a FLOAT one inside ONE composite. The fold is
		// the FLOAT rung — `numeric ∪ double precision` is double precision —
		// while the VALUE on the rows where the decimal arm wins is that
		// decimal's rendered TEXT. Reading the two questions as one left that
		// text with no reading and ordered it by BYTES. l_quantity is the
		// float column here because it is FLOAT64 in BOTH fixtures, where
		// l_discount becomes DECIMAL(15,2) under TPCH_DECIMAL=1 and the
		// question would quietly stop being asked.
		pgCase{name: "CoalesceDecimalFloatGtQuoted", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE COALESCE(d_2, 9.5) > '9'`},
		pgCase{name: "GreatestDecimalFloatGtQuoted", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE GREATEST(d_2, CAST(d_key AS DOUBLE PRECISION)) > '9'`},
		pgCase{name: "GreatestDecimalFloatGtUnquoted", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE GREATEST(d_2, CAST(d_key AS DOUBLE PRECISION)) > 9`},
		pgCase{name: "LeastDecimalFloatGtQuoted", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE LEAST(d_2, CAST(d_key AS DOUBLE PRECISION)) > '9'`},
		pgCase{name: "CaseDecimalFloatGtQuoted", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE WHEN d_key > 4 THEN d_2 ELSE CAST(d_key AS DOUBLE PRECISION) END > '9'`},
		pgCase{name: "GreatestDecimalRealGtQuoted", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE GREATEST(d_2, CAST(d_key AS REAL)) > '9'`},
		// TWO quoted literals in one call: neither operand of that pair is
		// typed, so neither was retyped to the fold and the two were ordered
		// by BYTES — '12.75' sorts below '3.1' there and above it as a number.
		pgCase{name: "TwoQuotedLiteralsDecimalFold", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE GREATEST('3.1','12.75',d_2) = '12.75'`},
		pgCase{name: "TwoQuotedLiteralsDecimalFoldLeast", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE LEAST('3.1','12.75',d_4) = '3.1'`},
		pgCase{name: "TwoQuotedLiteralsRealFold", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE GREATEST('9','10',r_val) = '10'`},
		pgCase{name: "TwoQuotedLiteralsRealFoldLeast", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE LEAST('9','10',r_val) = '9'`},
		// The fold decides the READING, not only the literal's grammar. A
		// composite whose kind is DECIMAL and whose fold is float8 must be
		// compared at the FLOAT rung on every row, including the rows where
		// the decimal arm supplied the value — otherwise the literal is read
		// with the DECIMAL grammar at DECIMAL width.
		//
		// '0x1.9p3' is 12.5 to the float input function and 22P02 to the
		// numeric one, so it separates the two grammars with a VALUE; the
		// decimal spelling beside it answers the same under either reading and
		// is the control.
		pgCase{name: "DecimalFloatFoldReadsHexLiteral", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE COALESCE(d_2, CAST(d_key AS DOUBLE PRECISION)) = '0x1.9p3'`},
		pgCase{name: "DecimalFloatFoldReadsHexLiteralGt", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE COALESCE(d_2, CAST(d_key AS DOUBLE PRECISION)) > '0x1.9p3'`},
		pgCase{name: "DecimalFloatFoldHexControl", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE COALESCE(d_2, CAST(d_key AS DOUBLE PRECISION)) = '12.5'`},
		pgCase{name: "DecimalRealFoldReadsHexLiteral", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE COALESCE(d_2, CAST(d_key AS REAL)) = '0x1.9p3'`},
		// WIDTH: float8 rounds a literal past its precision onto the value,
		// and an exact decimal comparison does not. The all-DECIMAL control
		// keeps its exactness and answers none.
		pgCase{name: "DecimalFloatFoldRoundsLiteral", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE COALESCE(d_2, CAST(d_key AS DOUBLE PRECISION)) = '12.750000000000000001'`},
		pgCase{name: "DecimalDecimalFoldKeepsExactness", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE COALESCE(d_2, d_4) = '12.750000000000000001'`},

		// SINGLE-element real IN — the arity split #549's re-review turned up.
		// PostgreSQL folds `real IN (x)` to `= 'x'::double precision` (WIDEN),
		// not the multi-element `= ANY(real[])` (NARROW), so a single
		// non-representable literal matches nothing and a single finite
		// over-range literal returns 0 rows with NO error. These are gated (not
		// pinned): wadjet keeps the widening path for arity 1 and agrees.
		pgCase{name: "RealInSingleNonRepresentable", sql: `SELECT r_key FROM real_probe WHERE r_val IN (0.1) ORDER BY r_key`},
		pgCase{name: "RealInSingleRepresentable", sql: `SELECT r_key FROM real_probe WHERE r_val IN (1.5) ORDER BY r_key`},
		pgCase{name: "RealInSingleOverflowNoError", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE r_val IN (1e40)`},
		// A NULL member in the list does NOT change the arity PostgreSQL uses
		// for the width decision: `real IN (0.1, NULL)` is syntactically two
		// elements, so it NARROWS to real[] and the 0.1 row matches — even
		// though the NULL is stripped for three-valued logic before the kernel
		// sees the list. This is the canonical #549 value (0.1) inside the most
		// common BI shape (a NULL in an IN list); before the syntactic-arity
		// fix wadjet decided WIDEN from the post-strip count and returned 0 rows.
		pgCase{name: "RealInNonRepresentableWithNull", sql: `SELECT r_key FROM real_probe WHERE r_val IN (0.1, NULL) ORDER BY r_key`},

		// --- CROSS-WIDTH KEYS (#615, #650, #663) ------------------------
		//
		// The comparison entries above ask what `real = double` MEANS. These
		// ask the same question of a KEY: a join, a semi/anti join and a
		// set-operation dedup all match by hashing each side, and the hash
		// was built from each side's OWN storage encoding while the
		// comparator resolved the pair to a common type first. So the WHERE
		// spelling of a predicate was right and the ON spelling of the same
		// predicate matched almost nothing — ADR-0023's "a key and the
		// comparator name one relation", violated across widths.
		//
		// PostgreSQL uses TWO ladders here and both are pinned. A JOIN key is
		// OPERATOR resolution: `real = double` is float8 (float48eq), `int =
		// numeric` is numeric, `numeric = real` is float8. A SET OPERATION is
		// `select_common_type`: `real ∪ double` is double precision but
		// `real ∪ numeric` and `real ∪ int` are REAL. They disagree exactly
		// where float4 meets an exact type, which is why the two rungs sit
		// side by side below.
		pgCase{name: "JoinRealAgainstDoubleKey", sql: `SELECT COUNT(*) AS n FROM real_probe a JOIN real_probe b ON a.r_val = b.d_val`},
		pgCase{name: "JoinDoubleAgainstRealKey", sql: `SELECT COUNT(*) AS n FROM real_probe a JOIN real_probe b ON a.d_val = b.r_val`},
		pgCase{name: "JoinBigintAgainstRealKey", sql: `SELECT COUNT(*) AS n FROM real_probe a JOIN real_probe b ON a.r_key = b.r_val`},
		pgCase{name: "JoinIntAgainstDoubleKey", sql: `SELECT COUNT(*) AS n FROM real_probe a JOIN real_probe b ON a.r_grp = b.d_val`},
		pgCase{name: "LeftJoinRealAgainstDoubleKey", sql: `SELECT COUNT(*) AS n FROM real_probe a LEFT JOIN real_probe b ON a.r_val = b.d_val`},
		pgCase{name: "SemiRealInDouble", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE r_val IN (SELECT d_val FROM real_probe)`},
		pgCase{name: "SemiDoubleInReal", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE d_val IN (SELECT r_val FROM real_probe)`},
		// NOT IN over a NULL-free subquery: with the NULLs left in,
		// PostgreSQL's three-valued rule answers 0 for every pair and the
		// entry cannot tell a right key from a wrong one.
		pgCase{name: "AntiRealNotInDouble", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE r_val NOT IN (SELECT d_val FROM real_probe WHERE d_val IS NOT NULL)`},
		pgCase{name: "ExistsRealEqDouble", sql: `SELECT COUNT(*) AS n FROM real_probe a WHERE EXISTS (SELECT 1 FROM real_probe b WHERE a.r_val = b.d_val)`},
		// The SET-OPERATION rung over the same pair: `real ∪ double` is
		// double precision, so both arms are read at float8 and NULL is a
		// value equal to itself.
		pgCase{name: "IntersectRealDoubleKey", sql: `SELECT COUNT(*) AS n FROM (SELECT r_val AS v FROM real_probe INTERSECT SELECT d_val FROM real_probe) s`},
		pgCase{name: "ExceptRealDoubleKey", sql: `SELECT COUNT(*) AS n FROM (SELECT r_val AS v FROM real_probe EXCEPT SELECT d_val FROM real_probe) s`},

		// The EXACT half of the same question, over dec_probe. `numeric IN
		// (SELECT bigint)` is the shape that PANICKED — the bloom and the
		// semi/anti probe took the integer fast path over a DECIMAL column
		// with no integer storage — and `bigint IN (SELECT numeric)` is the
		// one that answered 0.
		pgCase{name: "JoinBigintAgainstNumericKey", sql: `SELECT COUNT(*) AS n FROM dec_probe a JOIN dec_probe b ON a.d_key = b.d_2`},
		pgCase{name: "JoinNumericAgainstBigintKey", sql: `SELECT COUNT(*) AS n FROM dec_probe a JOIN dec_probe b ON a.d_2 = b.d_key`},
		pgCase{name: "JoinIntAgainstNumericKey", sql: `SELECT COUNT(*) AS n FROM dec_probe a JOIN dec_probe b ON a.d_grp = b.d_2`},
		// Two DECIMALs at different SCALES. The key has normalized scale
		// since #474, so this one was already right — it is here as the
		// control that says so.
		pgCase{name: "JoinNumericScalesKey", sql: `SELECT COUNT(*) AS n FROM dec_probe a JOIN dec_probe b ON a.d_2 = b.d_4`},
		pgCase{name: "LeftJoinNumericAgainstBigintKey", sql: `SELECT COUNT(*) AS n FROM dec_probe a LEFT JOIN dec_probe b ON a.d_2 = b.d_key`},
		pgCase{name: "SemiNumericInBigint", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 IN (SELECT d_key FROM dec_probe)`},
		pgCase{name: "SemiBigintInNumeric", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key IN (SELECT d_2 FROM dec_probe)`},
		pgCase{name: "AntiNumericNotInBigint", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 NOT IN (SELECT d_key FROM dec_probe WHERE d_key IS NOT NULL)`},
		pgCase{name: "ExistsNumericEqBigint", sql: `SELECT COUNT(*) AS n FROM dec_probe a WHERE EXISTS (SELECT 1 FROM dec_probe b WHERE a.d_2 = b.d_key)`},
		pgCase{name: "IntersectNumericBigintKey", sql: `SELECT COUNT(*) AS n FROM (SELECT d_2 AS v FROM dec_probe INTERSECT SELECT d_key FROM dec_probe) s`},
		pgCase{name: "ExceptNumericBigintKey", sql: `SELECT COUNT(*) AS n FROM (SELECT d_2 AS v FROM dec_probe EXCEPT SELECT d_key FROM dec_probe) s`},
		pgCase{name: "IntersectNumericScalesKey", sql: `SELECT COUNT(*) AS n FROM (SELECT d_2 AS v FROM dec_probe INTERSECT SELECT d_4 FROM dec_probe) s`},
		// A three-relation chain whose two links resolve to DIFFERENT common
		// types — numeric for the first, float8 for the second. It is the
		// shape #615 was filed with (the panic came out of inlineIntProbe,
		// which only a chain reaches) and it is the reason the resolved type
		// is per key PAIR rather than per query.
		pgCase{name: "ChainMixedWidthKeys", sql: `SELECT COUNT(*) AS n FROM dec_probe a JOIN dec_probe b ON a.d_key = b.d_2 JOIN dec_probe c ON b.d_grp = c.d_key`},

		// The same key pair over a side the plan-time type walk has to look
		// THROUGH. The first #615 commit resolved a BARE column on each side
		// and answered nothing for a side rooted at an aggregate, a window or
		// a set operation, and dropped every computed projection — so the
		// pair fell back to the build column's storage, which is the gate the
		// fix replaces. These are the shapes that says so.
		pgCase{name: "JoinNumericAgainstGroupedBigint", sql: `SELECT COUNT(*) AS n FROM dec_probe a JOIN (SELECT d_key AS k FROM dec_probe GROUP BY d_key) b ON a.d_2 = b.k`},
		pgCase{name: "JoinNumericAgainstDistinctBigint", sql: `SELECT COUNT(*) AS n FROM dec_probe a JOIN (SELECT DISTINCT d_key AS k FROM dec_probe) b ON a.d_2 = b.k`},
		pgCase{name: "JoinNumericAgainstCastBigint", sql: `SELECT COUNT(*) AS n FROM dec_probe a JOIN (SELECT CAST(d_grp AS BIGINT) AS k FROM dec_probe) b ON a.d_2 = b.k`},
		pgCase{name: "JoinNumericAgainstUnionBigint", sql: `SELECT COUNT(*) AS n FROM dec_probe a JOIN (SELECT d_key AS k FROM dec_probe UNION ALL SELECT d_grp FROM dec_probe) b ON a.d_2 = b.k`},
		pgCase{name: "JoinNumericAgainstWindowedBigint", sql: `SELECT COUNT(*) AS n FROM dec_probe a JOIN (SELECT d_key AS k, ROW_NUMBER() OVER (ORDER BY d_key) AS rn FROM dec_probe) b ON a.d_2 = b.k`},
		pgCase{name: "GroupedBigintProbeAgainstNumeric", sql: `SELECT COUNT(*) AS n FROM (SELECT d_key AS k FROM dec_probe GROUP BY d_key) a JOIN dec_probe b ON a.k = b.d_2`},
		// The row-at-a-time twin: a COMPUTED inner select item does not
		// decorrelate into a semi join, so the predicate is answered by
		// expr.InSubquery over a value SET — where a DECIMAL probe boxes as
		// its text and an integer set as int64, and missed every member.
		pgCase{name: "SemiNumericInCastBigint", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 IN (SELECT CAST(d_grp AS BIGINT) FROM dec_probe)`},
		pgCase{name: "SemiNumericInGroupedBigint", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 IN (SELECT d_key FROM dec_probe GROUP BY d_key)`},
		// The row-at-a-time LADDER, one entry per rung (#615 F2). A COMPUTED
		// inner select item does not decorrelate, so the predicate is answered
		// either by expr.InSubquery over a value SET or by a materialised
		// literal list — and both had rules of their own instead of
		// PostgreSQL's operator resolution. Every cross-rung pair missed every
		// member, and the NOT IN spelling INVENTED rows rather than dropping
		// them.
		pgCase{name: "SemiNumericInCastDouble", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 IN (SELECT CAST(d_key AS DOUBLE PRECISION) FROM dec_probe)`},
		pgCase{name: "SemiDoubleInCastNumeric", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE d_val IN (SELECT CAST(r_grp AS DECIMAL(9,2)) FROM real_probe)`},
		pgCase{name: "SemiBigintInCastDouble", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key IN (SELECT CAST(d_2 AS DOUBLE PRECISION) FROM dec_probe)`},
		pgCase{name: "AntiNumericNotInCastDouble", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 NOT IN (SELECT CAST(d_key AS DOUBLE PRECISION) FROM dec_probe WHERE d_key IS NOT NULL)`},
		pgCase{name: "AntiNumericNotInCastDoubleWithNulls", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 NOT IN (SELECT CAST(d_2 AS DOUBLE PRECISION) FROM dec_probe)`},
		pgCase{name: "SemiRealInCastBigint", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE r_val IN (SELECT CAST(r_grp AS BIGINT) FROM real_probe)`},
		pgCase{name: "SemiRealInCastDouble", sql: `SELECT COUNT(*) AS n FROM real_probe WHERE r_val IN (SELECT CAST(d_val AS DOUBLE PRECISION) FROM real_probe)`},
	)

	// A literal wider than the 128-bit carrier at the column's scale (#462).
	// PostgreSQL's numeric is unbounded, so it answers these by ordinary
	// comparison and every row is on one side of the literal. Wadjet narrowed
	// the literal by two's-complement WRAPAROUND, which landed it back inside
	// the ordinary range — sometimes with the opposite sign — so `d_2 < 1e39`
	// returned nothing at all. Written out in full as well as in exponent
	// form: the two spellings must agree.
	out = append(out,
		pgCase{name: "DecimalLtPastCarrier", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 < 1000000000000000000000000000000000000000`},
		pgCase{name: "DecimalGtPastCarrierNegative", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 > -1000000000000000000000000000000000000000`},
		pgCase{name: "DecimalGtPastCarrier", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 > 1000000000000000000000000000000000000000`},
		pgCase{name: "DecimalEqPastCarrier", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 = 1000000000000000000000000000000000000000`},
		pgCase{name: "DecimalNePastCarrier", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 <> 1000000000000000000000000000000000000000`},
		pgCase{name: "DecimalInPastCarrier", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 IN (1000000000000000000000000000000000000000, 12.75)`},
		pgCase{name: "DecimalBetweenPastCarrier", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 BETWEEN -1000000000000000000000000000000000000000 AND 1000000000000000000000000000000000000000`},
		// In range as an integer, out of range once the column's scale of 10
		// is applied: saturation is a property of the SCALED value.
		pgCase{name: "WideDecimalLtPastCarrierAtScale", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_wide < 100000000000000000000000000000`},
		pgCase{name: "WideDecimalGtPastCarrierAtScale", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_wide > 100000000000000000000000000000`},
	)

	// Exponent-form literals (#463). These used to be expanded through
	// strconv.ParseFloat before a digit was scaled: 1e400 made ParseFloat
	// report ErrRange, the expansion gave up, and the parser that received the
	// untouched text answered with the value ZERO — so `d_2 = 1e400` matched
	// the row holding 0.00. PostgreSQL's numeric is unbounded and reads every
	// spelling below as the number it names.
	out = append(out,
		pgCase{name: "DecimalEqOutOfFloatRange", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 = 1e400`},
		pgCase{name: "DecimalLtOutOfFloatRange", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 < 1e400`},
		pgCase{name: "DecimalGtOutOfFloatRangeNegative", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 > -1e400`},
		pgCase{name: "DecimalBetweenOutOfFloatRange", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 BETWEEN -1e400 AND 1e400`},
		// Underflow is the same defect mirrored: a positive value below the
		// column's last place equals nothing and splits the rows at zero.
		pgCase{name: "DecimalEqUnderFloatRange", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 = 1e-400`},
		pgCase{name: "DecimalLtUnderFloatRange", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 < 1e-400`},
		pgCase{name: "DecimalGeUnderFloatRangeNegative", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 >= -1e-400`},
		// Inside float64's range but past its significant digits: the exponent
		// is folded into the scaling, so all 25 digits survive.
		pgCase{name: "WideDecimalEqExponentForm", sql: `SELECT d_key FROM dec_probe WHERE d_wide = 4.938271605493827160549350e14 ORDER BY d_key`},
		pgCase{name: "WideDecimalEqExponentFormNegativeExp", sql: `SELECT d_key FROM dec_probe WHERE d_wide = 4938271605493827160549350e-10 ORDER BY d_key`},
		pgCase{name: "WideDecimalGtExponentForm", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_wide > 4.938271605493827160549350e14`},
		// A quoted numeric constant against a DECIMAL column is a NUMERIC
		// comparison in PostgreSQL, not a string one.
		pgCase{name: "DecimalEqQuotedNumeric", sql: `SELECT d_key FROM dec_probe WHERE d_2 = '12.75' ORDER BY d_key`},
	)

	// COLUMN against COLUMN, one of them a DECIMAL (#476). d_2 is
	// (d_key-100)*0.25, so `d_key >= d_2` is true of every non-NULL row and
	// `d_key < d_2` of none — numbers a reader can check by hand, and numbers
	// wadjet got wrong (129 and 59) because the DECIMAL side boxes as its
	// rendered TEXT and the pair fell through to a lexicographic comparison.
	// `=` and `<>` were right throughout, which is why only an oracle sweeping
	// every operator could see it.
	out = append(out,
		pgCase{name: "DecimalColColIntGe", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key >= d_2`},
		pgCase{name: "DecimalColColIntGt", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key > d_2`},
		pgCase{name: "DecimalColColIntLt", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key < d_2`},
		pgCase{name: "DecimalColColIntLe", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key <= d_2`},
		pgCase{name: "DecimalColColIntEq", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key = d_2`},
		pgCase{name: "DecimalColColIntNe", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key <> d_2`},
		pgCase{name: "DecimalColColIntFlipped", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 <= d_key`},
		// Scale 4 and the 25-digit wide column, so the reading is not pinned
		// to one scale or to values an int64 could hold.
		pgCase{name: "DecimalColColIntGeScale4", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key >= d_4`},
		pgCase{name: "DecimalColColIntGeWide", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key >= d_wide`},
		pgCase{name: "DecimalColColIntLtWide", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key < d_wide`},
		// The rows themselves, not only their count: a count can agree while
		// the row SET does not.
		pgCase{name: "DecimalColColIntGeRows", sql: `SELECT d_key FROM dec_probe WHERE d_key >= d_2 ORDER BY d_key`},
		// Through the row-at-a-time evaluator, which a CASE forces.
		pgCase{name: "DecimalColColIntGeInCase", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE WHEN d_key >= d_2 THEN 1 ELSE 0 END = 1`},
	)

	// DECIMAL against DECIMAL (#477). The two columns share a TypeID, so
	// ColColFilter skipped the mixed-type row fallback and went looking for a
	// kernel that did not exist — every operator FAILED the query. Three
	// different scales here (2, 4 and 10), so equality is decided ACROSS
	// scales, where "1.50" and "1.5000" are the same number and different
	// text.
	out = append(out,
		pgCase{name: "DecimalColColEq", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 = d_4`},
		pgCase{name: "DecimalColColNe", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 <> d_4`},
		pgCase{name: "DecimalColColLt", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 < d_4`},
		pgCase{name: "DecimalColColLe", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 <= d_4`},
		pgCase{name: "DecimalColColGt", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 > d_4`},
		pgCase{name: "DecimalColColGe", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 >= d_4`},
		pgCase{name: "DecimalColColWideLt", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_4 < d_wide`},
		pgCase{name: "DecimalColColWideGe", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 >= d_wide`},
		pgCase{name: "DecimalColColSelf", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 = d_2`},
		pgCase{name: "DecimalColColEqRows", sql: `SELECT d_key FROM dec_probe WHERE d_2 = d_4 ORDER BY d_key`},
		pgCase{name: "DecimalColColLtInCase", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE WHEN d_2 < d_4 THEN 1 ELSE 0 END = 1`},
	)

	// The same two DECIMAL columns at the BOXED sites (#506). #477's binding
	// lived in NewCmp alone, so a simple CASE's WHEN, IS [NOT] DISTINCT FROM
	// and GREATEST/LEAST still saw two rendered decimals as two strings and
	// ordered them LEXICOGRAPHICALLY, where "10.00" sorts below "2.5000".
	// Every entry here answers over the same pair the family above compares
	// directly, so a fix that reaches one site and not another shows up as a
	// disagreement between the two families rather than as silence.
	out = append(out,
		pgCase{name: "DecimalColPairSimpleCase", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE d_2 WHEN d_4 THEN 1 ELSE 0 END = 1`},
		pgCase{name: "DecimalColPairSimpleCaseWide", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE d_4 WHEN d_wide THEN 1 ELSE 0 END = 1`},
		pgCase{name: "DecimalColPairIsDistinctFrom", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 IS DISTINCT FROM d_4`},
		pgCase{name: "DecimalColPairIsNotDistinctFrom", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 IS NOT DISTINCT FROM d_4`},
		pgCase{name: "DecimalColPairIsDistinctFromWide", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_4 IS DISTINCT FROM d_wide`},
		pgCase{name: "DecimalColPairGreatest", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE GREATEST(d_2, d_4) = d_4`},
		pgCase{name: "DecimalColPairLeast", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE LEAST(d_2, d_4) = d_2`},
		pgCase{name: "DecimalColPairGreatestWide", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE GREATEST(d_2, d_wide) = d_wide`},
		// The rows themselves: the counts above can agree while the row SET
		// does not, because two wrong picks can cancel.
		pgCase{name: "DecimalColPairGreatestRows", sql: `SELECT d_key FROM dec_probe WHERE GREATEST(d_2, d_4) = d_4 ORDER BY d_key`},
		// GREATEST/LEAST's own VALUE, at full precision. A wrong pick that
		// happens to satisfy the predicates above is visible here, and until
		// ADR-0024 the query could not run at all: the registry declared the
		// return type FLOAT64, the value arrived as the column's rendered
		// text, and the #361 silent-write guard refused it. The declaration
		// is now the common DECIMAL type of the arguments and the value
		// materializes into a DECIMAL vector at that scale, so this entry
		// carries no pin — which is #529's proof.
		pgCase{name: "DecimalColPairGreatestValue", exactNumeric: true,
			sql: `SELECT d_key, GREATEST(d_2, d_4) AS g, LEAST(d_2, d_4) AS l FROM dec_probe ORDER BY d_key`},
		// The same for the other constructs that CHOOSE BETWEEN their
		// DECIMAL operands (ADR-0024 item 2): COALESCE and CASE were #555's
		// half of the same defect, and NULLIF mirrors argument 0 alone, so
		// its output keeps the NARROWER column's scale.
		pgCase{name: "DecimalColPairCoalesceValue", exactNumeric: true,
			sql: `SELECT d_key, COALESCE(d_2, d_4) AS c FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalColPairCaseValue", exactNumeric: true,
			sql: `SELECT d_key, CASE WHEN d_key < 5 THEN d_2 ELSE d_4 END AS c FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalColPairNullifValue", exactNumeric: true,
			sql: `SELECT d_key, NULLIF(d_2, d_4) AS c FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalColTripleGreatestValue", exactNumeric: true,
			sql: `SELECT d_key, GREATEST(d_2, d_4, d_wide) AS g, LEAST(d_2, d_4, d_wide) AS l ` +
				`FROM dec_probe ORDER BY d_key`},
		// --- A DECIMAL branch beside an INTEGER one (ADR-0024 item 2, #695) --
		//
		// PostgreSQL resolves every one of these to numeric, verified live on
		// 17.11. Wadjet declared the INTEGER, so the query's fate depended on
		// the DATA: it answered wherever the integer won every row (as an
		// INT64 column, TPC-H Q08's silent face) and failed at the #361 store
		// guard on the first row the decimal won (Q14's loud one). d_2 runs
		// from -25.00 to 24.75, so every entry below has rows on BOTH sides of
		// its literal — a fixture where one branch always won would pass under
		// either rule.
		//
		// The rendering the two engines do NOT share is the finite carrier's:
		// PostgreSQL's numeric carries a per-VALUE scale and prints the
		// integer branch as `0`, a DECIMAL column has one scale and prints
		// `0.00`. canonicalizeNumericStrings trims both to the same spelling,
		// so what is compared is the NUMBER.
		pgCase{name: "DecimalChoiceIntegerLiteralValue", exactNumeric: true,
			sql: `SELECT d_key, GREATEST(d_2, 0) AS g, LEAST(d_2, 0) AS l, COALESCE(d_2, 0) AS c ` +
				`FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalChoiceCaseIntegerElseValue", exactNumeric: true,
			sql: `SELECT d_key, CASE WHEN d_grp < 3 THEN d_2 ELSE 0 END AS c ` +
				`FROM dec_probe ORDER BY d_key`},
		// A FRACTIONAL literal, whose own declaration is FLOAT64: only its
		// carried (p,s) tells it from a float COLUMN, which PostgreSQL types
		// double precision. 0.125 is finer than d_2's scale, so the fold
		// WIDENS to scale 3 and the column's values gain a digit.
		pgCase{name: "DecimalChoiceFractionalLiteralValue", exactNumeric: true,
			sql: `SELECT d_key, CASE WHEN d_grp < 3 THEN d_2 ELSE 0.125 END AS c, ` +
				`GREATEST(d_4, 1.5) AS g FROM dec_probe ORDER BY d_key`},
		// An integer COLUMN rather than a literal: it contributes its whole
		// RANGE at scale 0 (10 digits for an INT32, 19 for an INT64), so the
		// fold's precision is wider while the values are the same.
		pgCase{name: "DecimalChoiceIntegerColumnValue", exactNumeric: true,
			sql: `SELECT d_key, LEAST(d_2, d_grp) AS l, GREATEST(d_4, d_key) AS g ` +
				`FROM dec_probe ORDER BY d_key`},
		// NULLIF mirrors argument 0 alone, so its fold is over the column and
		// the output keeps the column's own scale — the TYPE half of the same
		// rule the typmod table records.
		pgCase{name: "DecimalChoiceNullifIntegerValue", exactNumeric: true,
			sql: `SELECT d_key, NULLIF(d_2, 0) AS n FROM dec_probe ORDER BY d_key`},
		// TPC-H Q14's expression and Q08's, which is where the defect was
		// found: the choice is UNDER an aggregate, so its value travels
		// through the pre-aggregate projection's own DECIMAL vector.
		pgCase{name: "DecimalChoiceQ14Shape", exactNumeric: true,
			sql: `SELECT SUM(CASE WHEN d_grp < 3 THEN d_2 * (1 - d_4) ELSE 0 END) AS s FROM dec_probe`},
		pgCase{name: "DecimalChoiceQ08Shape", exactNumeric: true,
			sql: `SELECT SUM(CASE WHEN d_grp = 1 THEN d_2 ELSE 0 END) AS s FROM dec_probe`},
		// Q08 AS IT IS WRITTEN: the CASE branch is a bare reference to a
		// column a DERIVED TABLE computes. The aggregate's pre-projection
		// resolved its input types with a walk that STOPS at a subquery's
		// Project, so `volume` decided nothing and the CASE declared its
		// integer ELSE — the SELECT list had crossed a derived table since
		// #529 and the aggregate input had not.
		pgCase{name: "DecimalChoiceQ08ShapeOverADerivedTable", exactNumeric: true,
			sql: `SELECT SUM(CASE WHEN grp = 1 THEN volume ELSE 0 END) AS num, SUM(volume) AS den ` +
				`FROM (SELECT d_grp AS grp, d_2 * (1 - d_4) AS volume FROM dec_probe) x`},
		// GROUP BY the choice, which makes it a KEY: the key is encoded from
		// the materialized DECIMAL vector, so a branch that arrived as an
		// integer carrier would group under a different number.
		pgCase{name: "DecimalChoiceGroupKey", exactNumeric: true, ordered: true,
			sql: `SELECT CASE WHEN d_grp < 3 THEN d_2 ELSE 0 END AS k, COUNT(*) AS n ` +
				`FROM dec_probe GROUP BY 1 ORDER BY 1`},
		// An INTEGER ARM THAT IS AN EXPRESSION, not a literal or a bare
		// column (#695's review). The DECLARED fold takes any arm whose type
		// is INT32/INT64; the COMPILED fold classified by node kind and knew
		// neither a CAST nor a nested choice, so it declined and the integer
		// box met the DECIMAL vector — 22003 on both paths for values
		// PostgreSQL answers. A fixture of literals and bare columns cannot
		// tell the two folds apart, which is why these are separate entries.
		pgCase{name: "DecimalChoiceIntegerCastArm", exactNumeric: true,
			sql: `SELECT d_key, CASE WHEN d_grp < 3 THEN d_2 ELSE CAST(d_grp AS BIGINT) END AS c, ` +
				`GREATEST(d_2, CAST(d_grp AS BIGINT)) AS g FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalChoiceNestedIntegerChoiceArm", exactNumeric: true,
			sql: `SELECT d_key, COALESCE(d_2, CASE WHEN d_grp = 0 THEN 1 ELSE 2 END) AS c ` +
				`FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalChoiceIntegerExpressionGroupKey", exactNumeric: true, ordered: true,
			sql: `SELECT CASE WHEN d_grp < 3 THEN d_2 ELSE CAST(d_grp AS BIGINT) END AS k, ` +
				`COUNT(*) AS n FROM dec_probe GROUP BY 1 ORDER BY 1`},
		// The choice ABOVE an aggregate over an integer EXPRESSION: the DAG's
		// gather runs the same fold to decide whether to build a DECIMAL
		// column, and answered NULL when it declined.
		pgCase{name: "DecimalChoiceOverAnAggregateAndACast", exactNumeric: true,
			sql: `SELECT GREATEST(SUM(d_2), CAST(0 AS BIGINT)) AS g, ` +
				`COALESCE(SUM(d_2), CASE WHEN 1=1 THEN 0 ELSE 1 END) AS c FROM dec_probe`},
		// NULLIF types over BOTH arguments in PostgreSQL while its typmod
		// comes from argument 0 — `NULLIF(0, numeric)` is numeric there and
		// was INT64 here.
		pgCase{name: "DecimalChoiceNullifDecimalSecondArgument", exactNumeric: true,
			sql: `SELECT d_key, NULLIF(0, d_2) AS n FROM dec_probe ORDER BY d_key`},
		// The control that must NOT move: int ⊕ int stays integer on both
		// engines, so nothing here is reading "any number" as a decimal.
		pgCase{name: "DecimalChoiceIntegerOnlyStaysInteger",
			sql: `SELECT d_key, COALESCE(d_grp, 0) AS c, GREATEST(d_grp, 3) AS g ` +
				`FROM dec_probe ORDER BY d_key`},
		// --- Exact DECIMAL ARITHMETIC (ADR-0024 item 3, #555) --------------
		//
		// One entry per rule in item 3's table, over each pair of the
		// fixture's three widths, compared DIGIT FOR DIGIT. Before this the
		// declaration was FLOAT64 and the execution float64: `d_2 - d_4`
		// answered -9.999999999976694e-05 for a difference that is exactly 0,
		// and both sides of a float-rendered comparison agreed about the
		// first six significant digits — the agreement #455 had while
		// MAX(numeric(38,10)) was returning 9.777777778877776e+14.
		//
		// DIVISION is compared as a value only where the quotient
		// TERMINATES. PostgreSQL picks a result scale from the magnitude
		// (at least 16 significant digits) and wadjet from the input types,
		// so a repeating quotient keeps a different number of digits on each
		// side — item 3's recorded divergence, and one no exact comparison
		// can express. The fixture's d_2 values are multiples of 0.25, so
		// dividing by a power of two terminates in both engines.
		pgCase{name: "DecimalAddAcrossScales", exactNumeric: true,
			sql: `SELECT d_key, d_2 + d_4 AS s FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalSubAcrossScales", exactNumeric: true,
			sql: `SELECT d_key, d_2 - d_4 AS s FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalMulAcrossScales", exactNumeric: true,
			sql: `SELECT d_key, d_2 * d_4 AS p FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalModAcrossScales", exactNumeric: true,
			sql: `SELECT d_key, d_4 % d_2 AS m FROM dec_probe WHERE d_2 <> 0 ORDER BY d_key`},
		pgCase{name: "DecimalDivTerminating", exactNumeric: true,
			sql: `SELECT d_key, d_2 / 4 AS q FROM dec_probe ORDER BY d_key`},
		// The WIDE arm, whose values need more than 64 bits: a truncation
		// cannot hide inside a right answer here.
		//
		// Arithmetic over a DECIMAL(38,10) ALWAYS meets item 3's adjustment —
		// any rule over it wants more than 38 digits — so wadjet keeps fewer
		// fraction digits than PostgreSQL's unbounded numeric: `d_wide + d_4`
		// is DECIMAL(38,9) here and scale 10 there. That is the documented
		// divergence in the NUMBER of digits kept, so both sides are rounded
		// to a scale they BOTH keep before comparing. What is being asserted
		// is then the whole point: the digits they do keep are identical, and
		// the 128-bit carrier did the arithmetic exactly.
		pgCase{name: "WideDecimalAdd", exactNumeric: true,
			sql: `SELECT d_key, round(d_wide + d_4, 9) AS s, round(d_wide - d_2, 9) AS d ` +
				`FROM dec_probe ORDER BY d_key`},
		pgCase{name: "WideDecimalMulNarrow", exactNumeric: true,
			sql: `SELECT d_key, round(d_wide * d_2, 6) AS p FROM dec_probe ORDER BY d_key`},
		// (38,10) x (38,10) is item 3's ADJUSTMENT case: p = 77 wants 57
		// integer digits, so the scale falls to its floor min(20,6) = 6 and
		// the precision to 38. PostgreSQL's numeric is unbounded and keeps
		// all 20 fraction digits, so this is compared as a ROW COUNT — the
		// documented divergence in the number of digits kept.
		pgCase{name: "WideDecimalSquaredRowCount",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_wide * d_wide >= 0`},
		// An INTEGER operand joins as its whole range at scale 0, and a
		// numeric LITERAL as its spelling — so `d_2 * 2` keeps scale 2 and
		// `d_2 + 0.005` gains one.
		pgCase{name: "DecimalTimesIntegerColumn", exactNumeric: true,
			sql: `SELECT d_key, d_2 * d_key AS p, d_4 + d_grp AS s FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalTimesLiteral", exactNumeric: true,
			sql: `SELECT d_key, d_2 * 2 AS p, d_2 + 0.005 AS s, 100 - d_2 AS r FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalUnaryMinus", exactNumeric: true,
			sql: `SELECT d_key, -d_2 AS n, -d_4 + d_2 AS m FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalNestedArithmetic", exactNumeric: true,
			sql: `SELECT d_key, (d_2 + d_4) * d_2 AS v FROM dec_probe ORDER BY d_key`},
		// Arithmetic INSIDE an aggregate: the AggSpec's input (p,s) is the
		// EXPRESSION's, not a bare column's, which is what makes SUM exact
		// here (ADR-0012 item 9).
		pgCase{name: "DecimalArithmeticInsideAggregate", exactNumeric: true,
			sql: `SELECT SUM(d_2 * d_4) AS s, MIN(d_2 - d_4) AS lo, MAX(d_2 + d_4) AS hi FROM dec_probe`},
		pgCase{name: "DecimalArithmeticGrouped", exactNumeric: true,
			sql: `SELECT d_grp, SUM(d_2 * 2) AS s FROM dec_probe GROUP BY d_grp ORDER BY d_grp`},
		// Arithmetic in a GROUP BY key, an ORDER BY and a WHERE: the sites a
		// computed DECIMAL has to survive a stage boundary as a KEY.
		pgCase{name: "DecimalArithmeticGroupKey",
			sql: `SELECT COUNT(*) AS n FROM (SELECT d_2 * 2 AS g FROM dec_probe GROUP BY d_2 * 2) t`},
		pgCase{name: "DecimalArithmeticOrderBy",
			sql: `SELECT d_key FROM dec_probe ORDER BY d_2 * d_4, d_key`},
		pgCase{name: "DecimalArithmeticFilter",
			sql: `SELECT d_key FROM dec_probe WHERE d_2 * 2 > 10 ORDER BY d_key`},
		pgCase{name: "DecimalArithmeticInChoice", exactNumeric: true,
			sql: `SELECT d_key, COALESCE(d_2 * 2, d_4) AS c, ` +
				`CASE WHEN d_key < 5 THEN d_2 + d_4 ELSE d_4 END AS w FROM dec_probe ORDER BY d_key`},

		// A numeric LITERAL is an EXACT operand, and the values here are the
		// ones a float64 cannot represent — which is the whole point. Every
		// entry above uses the fixture's own values, and those are all
		// float-exact (12.75, 25.50, 0.0001), so none of them could see the
		// literal defect: `d_2 * 1.05` answered 13.387500000000001 and
		// `SELECT 0.1 + 0.2` refused itself with 22003 (#555 review, R2).
		pgCase{name: "DecimalTimesNonRepresentableLiteral", exactNumeric: true,
			sql: `SELECT d_key, d_2 * 1.05 AS a, d_4 * 1.1 AS b FROM dec_probe ORDER BY d_key`},
		// TRAILING ZEROS are part of the spelling and so part of the type:
		// PostgreSQL renders this with THREE fraction digits because the
		// literal contributed one.
		pgCase{name: "DecimalTimesTrailingZeroLiteral", exactNumeric: true,
			sql: `SELECT d_key, d_2 * 100.0 AS p FROM dec_probe ORDER BY d_key`},
		// A literal below a double's last place: the sum needs 22 digits.
		pgCase{name: "DecimalPlusSubUlpLiteral", exactNumeric: true,
			sql: `SELECT d_key, d_2 + 0.00000000000000000001 AS s FROM dec_probe ORDER BY d_key`},
		// Literal OP literal, with no column in the query at all — numeric on
		// both engines, and the canonical float trap as a predicate.
		pgCase{name: "LiteralArithmeticIsExact", exactNumeric: true,
			sql: `SELECT 0.1 + 0.2 AS a, 1.1 + 2.2 AS b, 10.0 / 4 AS c, 1.05 * 3 AS d`},
		pgCase{name: "LiteralFloatTrapPredicate", sql: `SELECT (0.1 + 0.2 = 0.3) AS v`},
		// MODULO over a DECIMAL and a fraction. `d % 1.5` truncated both
		// operands to integers and answered 0, and `d % 0.5` divided by the
		// zero that truncation created and CRASHED the query (#555 review,
		// N1/N2).
		pgCase{name: "DecimalModuloFractionalLiteral", exactNumeric: true,
			sql: `SELECT d_key, d_2 % 0.5 AS a, d_2 % 1.5 AS b, 0.5 % d_2 AS c ` +
				`FROM dec_probe WHERE d_2 <> 0 ORDER BY d_key`},

		// A COMPUTED INTEGER arm meeting a COMPUTED DECIMAL one, in both set
		// operations. Neither arm is a bare column, so neither is a
		// DirectCopy the worker types from its input — the integer arm's
		// DECLARED spec is what builds its vector, and declaring the
		// reconciled DECIMAL there made the checked writer refuse the int box
		// before the coercion could convert it. PostgreSQL resolves both to
		// numeric and moves no value.
		pgCase{name: "SetOpComputedIntegerArmAgainstComputedDecimal", exactNumeric: true,
			sql: `SELECT d_key + 1 AS v FROM dec_probe UNION ALL ` +
				`SELECT d_2 + 0.5 FROM dec_probe ORDER BY v`},
		pgCase{name: "SetOpComputedIntegerArmIntersectComputedDecimal",
			sql: `SELECT d_grp + 100 AS v FROM dec_probe INTERSECT ` +
				`SELECT d_grp + 100.0 FROM dec_probe ORDER BY v`},

		// --- CAST to and from DECIMAL (ADR-0024 item 3, #555) --------------
		//
		// A parameterized destination used to match no case label in the
		// evaluator's switch and fell to `default: return v`, so the value
		// passed through with the (p,s) IGNORED — no rounding, no rescale.
		pgCase{name: "DecimalCastNarrowing", exactNumeric: true,
			sql: `SELECT d_key, CAST(d_4 AS numeric(9,2)) AS c FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalCastWidening", exactNumeric: true,
			sql: `SELECT d_key, CAST(d_2 AS numeric(18,6)) AS c FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalCastFromInteger", exactNumeric: true,
			sql: `SELECT d_key, CAST(d_key AS numeric(10,2)) AS c, CAST(d_grp AS numeric(6,3)) AS g ` +
				`FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalCastBare", exactNumeric: true,
			sql: `SELECT d_key, CAST(d_2 AS numeric) AS c, CAST(d_key AS numeric) AS k FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalCastWide", exactNumeric: true,
			sql: `SELECT d_key, CAST(d_wide AS numeric(38,4)) AS c FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalCastInArithmetic", exactNumeric: true,
			sql: `SELECT d_key, CAST(d_4 AS numeric(9,2)) * 2 AS c FROM dec_probe ORDER BY d_key`},
		// numeric -> integer ROUNDS half away from zero (#373), and the wide
		// column is past a double entirely, so a float-mediated conversion is
		// visible as a wrong integer.
		pgCase{name: "DecimalCastToInteger",
			sql: `SELECT d_key, CAST(d_2 AS bigint) AS b FROM dec_probe ORDER BY d_key`},
		pgCase{name: "WideDecimalCastToInteger",
			sql: `SELECT d_key, CAST(d_wide AS bigint) AS b FROM dec_probe WHERE d_wide IS NOT NULL ` +
				`AND abs(d_wide) < 9000000000000000 ORDER BY d_key`},
		pgCase{name: "DecimalCastToText",
			sql: `SELECT d_key, CAST(d_2 AS text) AS t FROM dec_probe ORDER BY d_key`},
		// SMALLINT was a declared-STRING pass-through: the destination matched
		// no arm of the evaluator's switch at all (#555 review, N4).
		pgCase{name: "DecimalCastToSmallint",
			sql: `SELECT d_key, CAST(d_2 AS smallint) AS s FROM dec_probe ORDER BY d_key`},
		// A ROUND past the carrier's width: the result type capped the
		// precision while KEEPING the scale — DECIMAL(38,38), whose bound is
		// |v| < 10^0, so no value with an integer digit could be declared and
		// the query failed where PostgreSQL answers (#555 review, S1).
		pgCase{name: "DecimalRoundPastTheCarrier", exactNumeric: true,
			sql: `SELECT d_key, round(d_2, 40) AS r FROM dec_probe ORDER BY d_key`},

		// --- Scalar math over DECIMAL (ADR-0024 items 2 and 3, #668) -------
		//
		// PostgreSQL answers all of these in numeric. Wadjet declared every
		// one RetFloat64 and computed through ToFloat64 of the column's
		// rendered TEXT, so the value made a round trip through a double
		// before any rounding happened.
		pgCase{name: "DecimalRound", exactNumeric: true,
			sql: `SELECT d_key, round(d_2, 1) AS r1, round(d_4, 2) AS r2, round(d_2) AS r0 ` +
				`FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalRoundNegativeDigits", exactNumeric: true,
			sql: `SELECT d_key, round(d_2, -1) AS r FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalTrunc", exactNumeric: true,
			sql: `SELECT d_key, trunc(d_2, 1) AS t1, trunc(d_4) AS t0 FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalAbsCeilFloorSign", exactNumeric: true,
			sql: `SELECT d_key, abs(d_2) AS a, ceil(d_2) AS c, floor(d_2) AS f, sign(d_2) AS s ` +
				`FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalMod", exactNumeric: true,
			sql: `SELECT d_key, mod(d_2, 3) AS m FROM dec_probe ORDER BY d_key`},
		pgCase{name: "WideDecimalRoundAndAbs", exactNumeric: true,
			sql: `SELECT d_key, round(d_wide, 4) AS r, abs(d_wide) AS a, trunc(d_wide, 2) AS t ` +
				`FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalScalarFnInArithmetic", exactNumeric: true,
			sql: `SELECT d_key, round(d_2, 1) * 2 AS v, abs(d_4) + d_2 AS w FROM dec_probe ORDER BY d_key`},
		pgCase{name: "DecimalScalarFnInAggregate", exactNumeric: true,
			sql: `SELECT SUM(round(d_2, 1)) AS s, MAX(abs(d_4)) AS m FROM dec_probe`},

		// --- Integer division TRUNCATES, function operands included (#636) --
		//
		// compileBinOp chose the arithmetic node from the operands'
		// COMPILE-TIME shape, and a function call had none, so
		// `length(s) / 2` compiled to float division and answered 2.5 where
		// PostgreSQL answers 2.
		pgCase{name: "IntegerDivisionOverFunctionResult",
			sql: `SELECT n_nationkey, length(n_name) / 2 AS h, octet_length(n_name) / 3 AS t, ` +
				`length(n_name) % 2 AS m FROM nation ORDER BY n_nationkey`},
		pgCase{name: "IntegerDivisionNested",
			sql: `SELECT n_nationkey, (length(n_name) + 1) / 2 AS h FROM nation ORDER BY n_nationkey`},
		pgCase{name: "IntegerColumnDivision",
			sql: `SELECT n_nationkey, n_nationkey / 2 AS h, n_nationkey % 3 AS m FROM nation ORDER BY n_nationkey`},

		// One side a DECIMAL and the other an INT64 column, at the same three
		// sites: the pair #476 fixed for the direct comparison, which these
		// sites answered by SNIFFING the box until #504 bound it from the
		// declarations.
		pgCase{name: "DecimalColPairMixedGreatest", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE GREATEST(d_key, d_2) = d_key`},
		pgCase{name: "DecimalColPairMixedLeast", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE LEAST(d_key, d_2) = d_2`},
		pgCase{name: "DecimalColPairMixedIsDistinctFrom", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key IS DISTINCT FROM d_2`},
		pgCase{name: "DecimalColPairMixedSimpleCase", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE d_key WHEN d_2 THEN 1 ELSE 0 END = 1`},
	)

	// A DECIMAL column against an operand whose DECLARATION the boxed
	// comparison cannot read — a scalar subquery, arithmetic, a CAST, a
	// COALESCE with a NULL or an untyped literal alternative. The binding
	// used to require the OTHER side to be a number it could name, which
	// left every one of these comparing the DECIMAL as its RENDERED TEXT
	// (#504 review, B2). A proven DECIMAL operand now applies against
	// anything, and the numeric reading simply declines when the other box
	// is not a number.
	out = append(out,
		pgCase{name: "DecimalVsScalarSubqueryGt",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 > (SELECT MIN(d_key) FROM dec_probe)`},
		pgCase{name: "DecimalVsScalarSubqueryLt",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 < (SELECT MAX(d_key) FROM dec_probe)`},
		pgCase{name: "DecimalVsScalarSubqueryRows",
			sql: `SELECT d_key FROM dec_probe WHERE d_2 > (SELECT MIN(d_key) FROM dec_probe) ORDER BY d_key`},
		// Arithmetic and a CAST, beside the bare column they must agree with:
		// `d_2 > d_key` and `d_2 > d_key + 0` are the same question.
		pgCase{name: "DecimalVsBareIntColumn",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 > d_key`},
		pgCase{name: "DecimalVsIntArithmetic",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 > d_key + 0`},
		pgCase{name: "DecimalVsIntCast",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 > CAST(d_key AS BIGINT)`},
		pgCase{name: "DecimalVsIntArithmeticRows",
			sql: `SELECT d_key FROM dec_probe WHERE d_2 > d_key + 0 ORDER BY d_key`},
		// COALESCE: a NULL alternative used to poison the operand's kind, and
		// an untyped literal one still has to take the DECIMAL's type the way
		// PostgreSQL resolves it.
		pgCase{name: "DecimalInCoalesceWithNull",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE COALESCE(d_2, NULL) = 12.75`},
		pgCase{name: "DecimalInCoalesceWithNullOrdering",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE COALESCE(d_2, NULL) > 12.75`},
		pgCase{name: "DecimalInCoalesceTwoDecimals",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE COALESCE(d_2, d_4) > 12.75`},
		pgCase{name: "DecimalInCaseResult",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE (CASE WHEN d_key > 100 THEN d_2 ELSE d_4 END) > 12.75`},
		// A quoted numeric literal against a DECIMAL column, at the boxed
		// sites: PostgreSQL types the unknown literal from the column, so
		// these are exact numeric comparisons and not text ones.
		pgCase{name: "DecimalVsQuotedNumericSimpleCase",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE d_4 WHEN '3.1875' THEN 1 ELSE 0 END = 1`},
		pgCase{name: "DecimalVsQuotedNumericIsDistinctFrom",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_4 IS DISTINCT FROM '3.1875'`},
		pgCase{name: "DecimalVsQuotedNumericGreatest",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE GREATEST(d_4, '3.1875') = '3.1875'`},
	)

	// A NUMBER column against a QUOTED numeric literal. PostgreSQL types the
	// unknown-typed literal from the column — `d_key > '2'` is `d_key > 2`
	// there, not a text comparison and not a comparison against zero.
	//
	// The CASE-wrapped forms are GATED: they take the row-at-a-time path,
	// which follows that rule. The bare scalar comparisons on d_key (an
	// integer column) are GATED too: #536 gave the vectorized filter's integer
	// arms PostgreSQL's integer input grammar, so `d_key > '2'` asks "> 2".
	// The last two arms — the IN-LIST set builder (which still read its
	// elements through toInt64) and the FLOAT comparison (whose toFloat64 has
	// no string arm at all) — were pinned here and closed by #646, which gave
	// every numeric column type ONE literal rule keyed on its TypeID. Nothing
	// in this family is pinned any more.
	out = append(out,
		pgCase{name: "IntColumnVsQuotedNumericGtInCase",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE WHEN d_key > '2' THEN 1 ELSE 0 END = 1`},
		pgCase{name: "IntColumnVsQuotedNumericGeInCase",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE WHEN d_key >= '2' THEN 1 ELSE 0 END = 1`},
		pgCase{name: "IntColumnVsQuotedNumericLtInCase",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE WHEN d_key < '2' THEN 1 ELSE 0 END = 1`},
		pgCase{name: "IntColumnVsQuotedNumericEqInCase",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE WHEN d_key = '2' THEN 1 ELSE 0 END = 1`},
		// The zero row is the one the old temporal-epoch guard matched for
		// ANY unparseable string once the numeric side was zero.
		pgCase{name: "IntColumnVsQuotedNumericZeroInCase",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE WHEN d_key = '0' THEN 1 ELSE 0 END = 1`},
		pgCase{name: "IntColumnVsQuotedNumericInListInCase",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE WHEN d_key IN ('2','3') THEN 1 ELSE 0 END = 1`},
		pgCase{name: "IntColumnVsQuotedNumericBetweenInCase",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE WHEN d_key BETWEEN '2' AND '4' THEN 1 ELSE 0 END = 1`},
		pgCase{name: "IntColumnVsQuotedNumericGreatest",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE GREATEST(d_key, '2') = d_key`},
		pgCase{name: "IntColumnVsQuotedNumericSimpleCase",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE d_key WHEN '2' THEN 1 ELSE 0 END = 1`},
		pgCase{name: "IntColumnVsQuotedNumericIsDistinctFrom",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key IS DISTINCT FROM '2'`},
		// A FLOAT column, whose rule is the float comparison rather than the
		// exact one.
		pgCase{name: "FloatColumnVsQuotedNumericInCase",
			sql: `SELECT COUNT(*) AS n FROM lineitem WHERE CASE WHEN l_discount > '0.05' THEN 1 ELSE 0 END = 1`},
		pgCase{name: "FloatColumnVsQuotedNumericEqInCase",
			sql: `SELECT COUNT(*) AS n FROM lineitem WHERE CASE WHEN l_discount = '0.05' THEN 1 ELSE 0 END = 1`},
		// The scan path. The scalar integer comparisons now GATE #536's fix
		// (the vectorized filter parses the quoted integer): they must agree.
		// #536 review: an explicit valid-integer control, so the fix cannot
		// over-fire into refusing a genuine quoted integer.
		pgCase{name: "IntColumnVsQuotedIntegerEq",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key = '3'`},
		pgCase{name: "IntColumnVsQuotedNumericGt",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key > '2'`},
		pgCase{name: "IntColumnVsQuotedNumericLt",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key < '2'`},
		pgCase{name: "IntColumnVsQuotedNumericBetween",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key BETWEEN '2' AND '4'`},
		// The two arms #536 did not reach — the IN-list set builder and the
		// FLOAT comparison — closed by #646, which gave every numeric column
		// type the same literal rule. Their pins are deleted, which is that
		// fix's proof.
		pgCase{name: "IntColumnVsQuotedNumericInList",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key IN ('2','3')`},
		pgCase{name: "IntColumnVsQuotedNumericNotInList",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key NOT IN ('2','3')`},
		// l_quantity, not l_discount: this entry's subject is a FLOAT column,
		// and l_discount is DECIMAL(15,2) under TPCH_DECIMAL=1 (ADR-0024),
		// where the question would silently become the decimal one the
		// dec_probe entries already ask. l_quantity is FLOAT64 in BOTH
		// fixtures, so these stay gated in both.
		pgCase{name: "FloatColumnVsQuotedNumericGt",
			sql: `SELECT COUNT(*) AS n FROM lineitem WHERE l_quantity > '25'`},
		pgCase{name: "FloatColumnVsQuotedNumericEq",
			sql: `SELECT COUNT(*) AS n FROM lineitem WHERE l_quantity = '25'`},
		pgCase{name: "FloatColumnVsQuotedNumericInList",
			sql: `SELECT COUNT(*) AS n FROM lineitem WHERE l_quantity IN ('25','26')`},
	)

	// NaN and the infinities against a DECIMAL column (#534, ADR-0024 item 6).
	//
	// PostgreSQL's `numeric` HAS all three — NaN above every non-NaN and equal
	// only to itself, ±Infinity since PostgreSQL 14 — so every shape below is
	// a question it answers over a column holding none of them: 0 rows for
	// `=`, every non-NULL row for `<`. Wadjet's Int128-at-a-fixed-scale
	// carrier has no bit pattern for any of them and REFUSED the query with
	// 22P02 instead, which is PostgreSQL's answer for 'abc' and not for these.
	// They are comparison BOUNDS now, resolved through the same
	// ScaledDecimal.Sat a finite literal past the carrier already uses (#462).
	//
	// dec_probe's d_2/d_4 are NULL on no row, so the counts are the table's
	// own size; the shapes rather than the numbers are what these gate, over
	// three scales (2, 4 and the 38-digit wide column) and through both the
	// vectorized filter and the boxed sites.
	out = append(out,
		pgCase{name: "DecimalEqNaN", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 = 'NaN'`},
		pgCase{name: "DecimalNeNaN", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 <> 'NaN'`},
		pgCase{name: "DecimalLtNaN", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 < 'NaN'`},
		pgCase{name: "DecimalLeNaN", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 <= 'NaN'`},
		pgCase{name: "DecimalGtNaN", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 > 'NaN'`},
		pgCase{name: "DecimalGeNaN", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 >= 'NaN'`},
		pgCase{name: "DecimalEqInfinity", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 = 'Infinity'`},
		pgCase{name: "DecimalLeInfinity", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 <= 'Infinity'`},
		pgCase{name: "DecimalGtInfinity", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 > 'Infinity'`},
		pgCase{name: "DecimalGtNegInfinity", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 > '-Infinity'`},
		pgCase{name: "DecimalLtNegInfinity", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 < '-Infinity'`},
		pgCase{name: "DecimalBetweenInfinities",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 BETWEEN '-Infinity' AND 'Infinity'`},
		// The row SET, not only its size: a count can agree while the rows do
		// not, and a bound that landed in the MIDDLE of the range (which is
		// what a wrapped or zeroed literal produces) shows up here.
		pgCase{name: "DecimalLtNaNRows",
			sql: `SELECT d_key FROM dec_probe WHERE d_2 < 'NaN' AND d_key < 10 ORDER BY d_key`},
		pgCase{name: "DecimalGtNegInfinityRows",
			sql: `SELECT d_key FROM dec_probe WHERE d_4 > '-Infinity' AND d_key < 10 ORDER BY d_key`},
		// An IN list mixing a special with a value the column HOLDS keeps the
		// value: the special equals nothing, so it contributes no member.
		pgCase{name: "DecimalInNaNAndValue",
			sql: `SELECT d_key FROM dec_probe WHERE d_2 IN ('NaN', 12.75) ORDER BY d_key`},
		pgCase{name: "DecimalInBothInfinities",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 IN ('-Infinity', 'Infinity')`},
		pgCase{name: "DecimalNotInNaN", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 NOT IN ('NaN')`},
		// Every spelling PostgreSQL's numeric input accepts, verified live:
		// case-insensitive, the short `inf` form, an optional sign on the
		// infinities (and NONE on NaN), and surrounding whitespace stripped.
		pgCase{name: "DecimalLtNaNLowercase", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_4 < 'nan'`},
		pgCase{name: "DecimalLtNaNUppercase", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_4 < 'NAN'`},
		pgCase{name: "DecimalLtNaNPadded", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_4 < '  NaN  '`},
		pgCase{name: "DecimalLtInfShort", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_4 < 'inf'`},
		pgCase{name: "DecimalLtInfMixedCase", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_4 < 'Inf'`},
		pgCase{name: "DecimalLtInfinityUppercase",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_4 < 'INFINITY'`},
		pgCase{name: "DecimalLtPlusInfinity", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_4 < '+Infinity'`},
		pgCase{name: "DecimalLtPlusInfShort", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_4 < '+inf'`},
		pgCase{name: "DecimalGtNegInfShort", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_4 > '-inf'`},
		pgCase{name: "DecimalGtNegInfMixedCase", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_4 > '-Inf'`},
		// The 38-digit column, whose values no float64 and no int64 holds:
		// the bound must not depend on the carrier width it is compared at.
		pgCase{name: "WideDecimalLtNaN", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_wide < 'NaN'`},
		pgCase{name: "WideDecimalGtNegInfinity",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_wide > '-Infinity'`},
		pgCase{name: "WideDecimalEqNaN", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_wide = 'NaN'`},
		// The BOXED sites (#465/#506), which reach the column as its rendered
		// TEXT — the path that would otherwise compare "12.75" against "NaN"
		// lexicographically, where "N" sorts below every digit.
		pgCase{name: "DecimalSimpleCaseNaN",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE d_2 WHEN 'NaN' THEN 1 ELSE 0 END = 1`},
		pgCase{name: "DecimalIsDistinctFromNaN",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 IS DISTINCT FROM 'NaN'`},
		pgCase{name: "DecimalIsNotDistinctFromNaN",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 IS NOT DISTINCT FROM 'NaN'`},
		pgCase{name: "DecimalGreatestNaN",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE GREATEST(d_2, 'NaN') = 'NaN'`},
		pgCase{name: "DecimalLeastNegInfinity",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE LEAST(d_2, '-Infinity') = '-Infinity'`},
		// Through the row-at-a-time evaluator, which a CASE forces.
		pgCase{name: "DecimalLtNaNInCase",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE WHEN d_2 < 'NaN' THEN 1 ELSE 0 END = 1`},
		pgCase{name: "DecimalGtNegInfinityInCase",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE WHEN d_2 > '-Infinity' THEN 1 ELSE 0 END = 1`},
		// A FLOAT column against the same literals is the PARITY control, and
		// it is a DIFFERENT rule: float8 and float4 HOLD all three, so the
		// comparison is PostgreSQL's ordinary float order (ADR-0012 item 8),
		// not the DECIMAL bound above. The row-at-a-time path applies it
		// (expr.decimalTextOrderFloat -> kernel.FloatSpecialText ->
		// kernel.CompareFloat64), so these are GATED.
		//
		// They run on real_probe rather than on lineitem BECAUSE OF THE SIGN.
		// `> '-Infinity'` is the only shape that can tell the float rule from
		// the LEXICOGRAPHIC fallthrough it used to take: every finite value
		// renders starting with a digit or '-', and only a NEGATIVE rendering
		// ("-3.5") sorts below "-Infinity" as text, so a non-negative column
		// (l_discount, l_quantity, every TPC-H float) answers these
		// identically under both readings and could never fail. real_probe
		// rows 22-23 are negative for exactly this, and it also carries a NaN
		// row and a NULL, so the order and the three-valued logic are both
		// exercised. `r_val` is float4 and `d_val` float8, so both widths are
		// covered.
		pgCase{name: "RealColumnGtNegInfinityInCase",
			sql: `SELECT r_key FROM real_probe WHERE CASE WHEN r_val > '-Infinity' THEN 1 ELSE 0 END = 1 ORDER BY r_key`},
		pgCase{name: "RealColumnLtNegInfinityInCase",
			sql: `SELECT COUNT(*) AS n FROM real_probe WHERE CASE WHEN r_val < '-Infinity' THEN 1 ELSE 0 END = 1`},
		pgCase{name: "RealColumnLtNaNInCase",
			sql: `SELECT r_key FROM real_probe WHERE CASE WHEN r_val < 'NaN' THEN 1 ELSE 0 END = 1 ORDER BY r_key`},
		pgCase{name: "RealColumnEqNaNInCase",
			sql: `SELECT r_key FROM real_probe WHERE CASE WHEN r_val = 'NaN' THEN 1 ELSE 0 END = 1 ORDER BY r_key`},
		pgCase{name: "RealColumnLeInfinityInCase",
			sql: `SELECT COUNT(*) AS n FROM real_probe WHERE CASE WHEN r_val <= 'Infinity' THEN 1 ELSE 0 END = 1`},
		pgCase{name: "RealColumnGtInfinityInCase",
			sql: `SELECT COUNT(*) AS n FROM real_probe WHERE CASE WHEN r_val > 'Infinity' THEN 1 ELSE 0 END = 1`},
		// float8, same shapes.
		pgCase{name: "DoubleColumnGtNegInfinityInCase",
			sql: `SELECT r_key FROM real_probe WHERE CASE WHEN d_val > '-Infinity' THEN 1 ELSE 0 END = 1 ORDER BY r_key`},
		pgCase{name: "DoubleColumnLtNaNInCase",
			sql: `SELECT r_key FROM real_probe WHERE CASE WHEN d_val < 'NaN' THEN 1 ELSE 0 END = 1 ORDER BY r_key`},
		// A SIGNED NaN is where the float grammar and the numeric one part:
		// float8 reads '+NaN' and '-NaN' as NaN, numeric refuses both with
		// 22P02 (verified live). So this shape must ANSWER on the float
		// column while the DECIMAL entries above keep refusing it.
		pgCase{name: "RealColumnLtSignedNaNInCase",
			sql: `SELECT r_key FROM real_probe WHERE CASE WHEN r_val < '+NaN' THEN 1 ELSE 0 END = 1 ORDER BY r_key`},
		pgCase{name: "RealColumnLtNegatedNaNInCase",
			sql: `SELECT r_key FROM real_probe WHERE CASE WHEN r_val < '-NaN' THEN 1 ELSE 0 END = 1 ORDER BY r_key`},
		// The BARE forms take the VECTORIZED float kernel, and they answer the
		// same row sets as the CASE-wrapped entries above now that it reads
		// the float input grammar instead of kernel.toFloat64's silent 0.0
		// (#646). These two were the pins; they are deleted, which is the
		// fix's proof. `> '-Infinity'` is the shape that can tell the float
		// rule from the lexicographic fallthrough, and it needs real_probe's
		// NEGATIVE rows to do it.
		pgCase{name: "RealColumnGtNegInfinity",
			sql: `SELECT r_key FROM real_probe WHERE r_val > '-Infinity' ORDER BY r_key`},
		pgCase{name: "RealColumnLtNaN",
			sql: `SELECT r_key FROM real_probe WHERE r_val < 'NaN' ORDER BY r_key`},
		pgCase{name: "RealColumnEqNaN",
			sql: `SELECT r_key FROM real_probe WHERE r_val = 'NaN' ORDER BY r_key`},
		pgCase{name: "DoubleColumnGtNegInfinity",
			sql: `SELECT r_key FROM real_probe WHERE d_val > '-Infinity' ORDER BY r_key`},
		// l_quantity for the same reason as FloatColumnVsQuotedNumericGt: the
		// subject must stay FLOAT in both fixtures, or under TPCH_DECIMAL=1
		// this stops asking the float question at all.
		pgCase{name: "FloatColumnLtNaN",
			sql: `SELECT COUNT(*) AS n FROM lineitem WHERE l_quantity < 'NaN'`},
	)

	// The other side of #534's boundary — a signed NaN, a partial spelling of
	// an infinity, and ordinary garbage — is 22P02 in PostgreSQL too, and this
	// arm fatals on an oracle error by design (see the boolean-input note
	// above). So the refusals are gated where an error is the assertion: the
	// SQLSTATE in the WIRE arm's DecimalNonNumericConstant family, and the
	// spellings in wadjet.TestNonNumericDecimalLiteralIsStillRefused and
	// coordinator.TestNonNumericDecimalLiteralIsRefusedOnBothPaths, all
	// against the same live transcript.

	// The comparison sites #452's binding did not reach (#465). A simple
	// CASE's WHEN, IS [NOT] DISTINCT FROM and GREATEST/LEAST all compared
	// through the boxed path, where the column is rendered TEXT and the
	// literal is the float64 the compiler built for arithmetic — so a literal
	// naming a stored value exactly did not match it.
	//
	// Each shape appears TWICE: once with the literal that names a stored
	// value exactly and once one unit of the last place away from it. A
	// float64 renders those two identically, so the pair is what tells an
	// exact comparison from a rounded one.
	out = append(out,
		pgCase{name: "WideDecimalSimpleCase", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE d_wide WHEN 493827160549382.7160549350 THEN 1 ELSE 0 END = 1`},
		pgCase{name: "WideDecimalSimpleCaseOffByUlp", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE d_wide WHEN 493827160549382.7160549351 THEN 1 ELSE 0 END = 1`},
		pgCase{name: "WideDecimalIsDistinctFrom", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_wide IS DISTINCT FROM 493827160549382.7160549350`},
		pgCase{name: "WideDecimalIsDistinctFromOffByUlp", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_wide IS DISTINCT FROM 493827160549382.7160549351`},
		pgCase{name: "WideDecimalIsNotDistinctFrom", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_wide IS NOT DISTINCT FROM 493827160549382.7160549350`},
		pgCase{name: "WideDecimalGreatest", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE GREATEST(d_wide, 493827160549382.7160549350) = 493827160549382.7160549350`},
		pgCase{name: "WideDecimalLeast", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE LEAST(d_wide, 493827160549382.7160549350) = 493827160549382.7160549350`},
		pgCase{name: "WideDecimalLeastOffByUlp", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE LEAST(d_wide, 493827160549382.7160549351) = 493827160549382.7160549351`},
		// The narrow column, where every value IS a float64: these entries
		// hold the sites to what they already answered.
		pgCase{name: "DecimalSimpleCase", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE CASE d_2 WHEN 12.75 THEN 1 ELSE 0 END = 1`},
		pgCase{name: "DecimalIsDistinctFrom", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 IS DISTINCT FROM 12.75`},
		pgCase{name: "DecimalGreatest", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE GREATEST(d_2, 12.75) = 12.75`},
		pgCase{name: "DecimalLeast", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE LEAST(d_2, 12.75) = 12.75`},
		// A literal past the 128-bit carrier reaches these sites too, and
		// saturates there as it does everywhere else (#462).
		pgCase{name: "DecimalGreatestPastCarrier", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE GREATEST(d_2, 1e39) = 1e39`},
	)

	// --- Wide DECIMAL (precision 38) --------------------------------------
	//
	// Everything above runs on ONE physical encoding. A DECIMAL's leaf type is
	// a function of its PRECISION (ADR-0018 §4) — INT32 to 9 digits, INT64 to
	// 18, FIXED_LEN_BYTE_ARRAY beyond — so d_2 and d_4 are both INT64 leaves
	// and the fixture had no column at all on the third arm. That is the arm
	// #429 got wrong (a wide DECIMAL annotated over an INT64 leaf, which the
	// Apache implementation refuses to open) and the one #437's old reader
	// silently truncates to 64 bits.
	//
	// d_wide's unscaled values run 77 to 84 bits, so every non-zero one is
	// past int64 entirely — a truncation cannot hide inside a right answer
	// here. It also has no footer statistics (a wide DECIMAL carries none), so
	// these entries exercise the prune's OTHER branch: withholding a bound
	// rather than converting one, which must never change a row.
	//
	// Same projection discipline as above — d_key, not the decimal, because
	// the two engines box it differently and the wire arm is where that is
	// reported. The AGGREGATES are the exception, and since #455 they are
	// compared as VALUES, digit for digit (exactNumeric): MIN/MAX/SUM over a
	// numeric are exact on both engines, so anything less than exact equality
	// is a defect. They used to be compared through the fingerprint's float
	// rendering "for the same reason AVG(int) is", and that is precisely how
	// MAX(d_wide) could answer 9.777777778877776e+14 for
	// 977777777887777.7577887713 with the gate green.
	//
	// AVG keeps the float comparison, and that is a CONTRACT difference
	// rather than an oversight: both engines divide exactly, but PostgreSQL
	// picks a result scale giving at least 16 significant digits while wadjet
	// widens the input scale by a fixed 4 (batch.AvgScaleIncrement, ADR-0012
	// item 9). The two agree to min(both scales) and differ in how many digits
	// past that they keep, which no exact comparison can express.
	out = append(out,
		pgCase{name: "WideDecimalEq", sql: `SELECT d_key FROM dec_probe WHERE d_wide = 493827160549382.7160549350 ORDER BY d_key`},
		pgCase{name: "WideDecimalEqNegative", sql: `SELECT d_key FROM dec_probe WHERE d_wide = -888888888988888.8888988830 ORDER BY d_key`},
		pgCase{name: "WideDecimalEqZero", sql: `SELECT d_key FROM dec_probe WHERE d_wide = 0 ORDER BY d_key`},
		// One unit of the last decimal place apart from a value that IS in the
		// column. A float64 cannot tell these two literals apart at all.
		pgCase{name: "WideDecimalEqOffByUlp", sql: `SELECT d_key FROM dec_probe WHERE d_wide = 493827160549382.7160549351 ORDER BY d_key`},
		pgCase{name: "WideDecimalNe", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_wide <> 493827160549382.7160549350`},
		pgCase{name: "WideDecimalLt", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_wide < 493827160549382.7160549350`},
		pgCase{name: "WideDecimalGt", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_wide > 493827160549382.7160549350`},
		pgCase{name: "WideDecimalGe", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_wide >= 493827160549382.7160549350`},
		pgCase{name: "WideDecimalBetween", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_wide BETWEEN 493827160549382.7160549350 AND 740740740824074.0740824025`},
		pgCase{name: "WideDecimalIn", sql: `SELECT d_key FROM dec_probe WHERE d_wide IN (493827160549382.7160549350, -888888888988888.8888988830, 0) ORDER BY d_key`},
		pgCase{name: "WideDecimalIsNull", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_wide IS NULL`},
		pgCase{name: "WideDecimalIsNotNull", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_wide IS NOT NULL`},
		// ORDER BY on the wide column: the SEQUENCE of d_key is the answer, so
		// a comparator that truncates to 64 bits reorders visibly. NULLs
		// included, since their placement is PostgreSQL's rule (ADR-0012).
		pgCase{name: "WideDecimalOrderBy", sql: `SELECT d_key FROM dec_probe ORDER BY d_wide, d_key`},
		pgCase{name: "WideDecimalOrderByDesc", sql: `SELECT d_key FROM dec_probe ORDER BY d_wide DESC, d_key`},
		pgCase{name: "WideDecimalSum", sql: `SELECT SUM(d_wide) AS s FROM dec_probe WHERE d_key < 60`,
			exactNumeric: true},
		pgCase{name: "WideDecimalAvg", sql: `SELECT AVG(d_wide) AS a FROM dec_probe WHERE d_key < 60`},
		pgCase{name: "WideDecimalMinMax", sql: `SELECT MIN(d_wide) AS lo, MAX(d_wide) AS hi FROM dec_probe`,
			exactNumeric: true},
		// The grouped accumulator is a different one from the whole-input
		// kernels above (the flat SoA arrays, agg_scatter.go), and it is the
		// half #417 found answering NULL for DECIMAL while the scalar form
		// was right.
		pgCase{name: "WideDecimalMinMaxSumGrouped", exactNumeric: true,
			sql: `SELECT d_key % 4 AS g, MIN(d_wide) AS lo, MAX(d_wide) AS hi, SUM(d_wide) AS s
				FROM dec_probe GROUP BY g`},
		// The narrow encodings, which the issue's second example is over: a
		// DECIMAL(9,2) MIN answered -4.27876713e+06 for -4278767.13.
		pgCase{name: "NarrowDecimalMinMaxSum", exactNumeric: true,
			sql: `SELECT MIN(d_2) AS lo2, MAX(d_2) AS hi2, SUM(d_2) AS s2,
				MIN(d_4) AS lo4, MAX(d_4) AS hi4, SUM(d_4) AS s4 FROM dec_probe`},
		pgCase{name: "NarrowDecimalMinMaxSumGrouped", exactNumeric: true,
			sql: `SELECT d_key % 3 AS g, MIN(d_2) AS lo, MAX(d_2) AS hi, SUM(d_2) AS s,
				SUM(d_4) AS s4 FROM dec_probe GROUP BY g`},
		// Both encodings in one predicate.
		pgCase{name: "WideDecimalWithNarrow", sql: `SELECT d_key FROM dec_probe WHERE d_wide > 0 AND d_2 < 0 ORDER BY d_key`},
		// An UNGROUPED aggregate under a SELECTIVE filter (#685). On the stage
		// DAG the selectivity is what empties whole partial tasks, and an
		// ungrouped aggregate that consumed nothing still owes an identity
		// row — which is where its DECIMAL scale used to go missing and every
		// value came back 10^scale too large. The entries here hold this arm
		// to PostgreSQL's answer over the same rows; the DAG half of the same
		// question is coordinator.TestFilteredDecimalAggregateTwoPath, since
		// this arm runs through the embedded API and never fans out.
		//
		// Both selectivities, all three encodings, and the empty result: what
		// the fix has to keep right is the whole range, not the one bound the
		// failing query used.
		pgCase{name: "FilteredDecimalAggSelective", exactNumeric: true,
			sql: `SELECT SUM(d_2) AS s2, MIN(d_2) AS lo2, MAX(d_2) AS hi2,
				SUM(d_4) AS s4, MIN(d_4) AS lo4, MAX(d_4) AS hi4,
				SUM(d_wide) AS sw, MIN(d_wide) AS low, MAX(d_wide) AS hiw,
				COUNT(d_2) AS n FROM dec_probe WHERE d_key < 5`},
		pgCase{name: "FilteredDecimalAggVerySelective", exactNumeric: true,
			sql: `SELECT SUM(d_2) AS s2, MIN(d_2) AS lo2, MAX(d_2) AS hi2,
				SUM(d_wide) AS sw, COUNT(d_2) AS n FROM dec_probe WHERE d_key < 2`},
		pgCase{name: "FilteredDecimalAggEmpty", exactNumeric: true,
			sql: `SELECT SUM(d_2) AS s2, MIN(d_2) AS lo2, MAX(d_2) AS hi2,
				SUM(d_wide) AS sw, COUNT(d_2) AS n FROM dec_probe WHERE d_key < 0`},
		pgCase{name: "FilteredDecimalAvgSelective",
			sql: `SELECT AVG(d_2) AS a2, AVG(d_4) AS a4, AVG(d_wide) AS aw
				FROM dec_probe WHERE d_key < 5`},
		// The same filter with the aggregate WRAPPED, so the carrier reaches
		// an operator rather than the client: a scale that travelled apart
		// from its integer shows up multiplied here too.
		//
		// NOT exactNumeric, deliberately and for a reason that is not this
		// entry's: `numeric * 2` still resolves FLOAT64 here (ADR-0024 item
		// 1's residual — the declared-type layer has no (p,s) for arithmetic
		// yet), so the digits past float64's are a difference this entry does
		// not own and must not pin. What it does own is the FACTOR: a scale
		// read apart from its integer moves the value by 10^scale, which is
		// 100x at the narrowest and survives any rendering either side picks.
		pgCase{name: "FilteredDecimalAggWrapped",
			sql: `SELECT SUM(d_2) * 2 AS s2, MIN(d_4) * 2 AS lo4, MAX(d_wide) * 2 AS hiw
				FROM dec_probe WHERE d_key < 5`},
		// And GROUPED under the same filter — the arm that was correct before
		// the fix, kept as the control that says the entries above are about
		// selectivity and not about the filter.
		pgCase{name: "FilteredDecimalAggGrouped", exactNumeric: true,
			sql: `SELECT d_key, SUM(d_2) AS s2, MIN(d_4) AS lo4, MAX(d_wide) AS hiw
				FROM dec_probe WHERE d_key < 5 GROUP BY d_key ORDER BY d_key`},
		// A DECIMAL GROUP-BY KEY under the same filter: the value IS the key
		// there, and a key column that lost its (p,s) truncates rather than
		// rescaling (ADR-0024 item 2).
		pgCase{name: "FilteredDecimalGroupKey",
			sql: `SELECT d_2 AS k, COUNT(*) AS n FROM dec_probe WHERE d_key < 5 GROUP BY d_2 ORDER BY d_2`},
		// And a WINDOW aggregate under it, the third exact path.
		pgCase{name: "FilteredDecimalWindow", exactNumeric: true,
			sql: `SELECT d_key, SUM(d_2) OVER () AS ws, MIN(d_4) OVER () AS wlo,
				MAX(d_wide) OVER () AS whi FROM dec_probe WHERE d_key < 5 ORDER BY d_key`},
	)

	// --- String ordering ------------------------------------------------
	//
	// Wadjet compares strings by bytes; PostgreSQL by the collation. The
	// oracle is configured to a byte-ordering collation (postgresCollation)
	// rather than exempting these, because exempting them would blind the arm
	// to every real ordering bug in a string ORDER BY — the whole family
	// #313/#316/#320 came out of.
	//
	// The values are built from the fixture so that byte order and locale
	// order DISAGREE: by bytes every uppercase letter sorts before every
	// lowercase one, and the space and the underscore sort on opposite sides
	// of the letters. Under an en_US database these entries fail, which is the
	// intended alarm and not a Wadjet defect — reportCollation says so before
	// a single query runs.
	//
	// They are spelled over `nation` rather than over a VALUES list because,
	// when these were written, Wadjet's parser had no VALUES-in-FROM (see
	// ValuesListInFrom below) — a pinned entry is not a comparison. VALUES
	// works now (#374); left over `nation` rather than rewritten, since real
	// fixture data is the stronger test and nothing here depended on the
	// workaround.

	// The aggregate-over-CASE defect, shared by the two entries below.
	const mixedCase = `CASE WHEN n_nationkey % 4 = 0 THEN LOWER(n_name)
			WHEN n_nationkey % 4 = 1 THEN n_name
			WHEN n_nationkey % 4 = 2 THEN '_' || n_name
			ELSE ' ' || LOWER(n_name) END`
	out = append(out,
		pgCase{name: "StringOrderMixedCase", sql: `SELECT n_nationkey, ` + mixedCase + ` AS v FROM nation ORDER BY v, n_nationkey`},
		pgCase{name: "StringOrderMixedCaseDesc", sql: `SELECT n_nationkey, ` + mixedCase + ` AS v FROM nation ORDER BY v DESC, n_nationkey`},
		pgCase{name: "StringMinMaxCollation", sql: `SELECT MIN(` + mixedCase + `) AS mn, MAX(` + mixedCase + `) AS mx FROM nation`},
		// The control that names the trigger exactly (#372's localization):
		// bare, fn and cat were always correct; only the CASE arm dropped
		// its value, because a CASE was the one string-valued expression
		// whose type the aggregate did not resolve (nodeDeclaredType had no
		// CaseNode arm).
		pgCase{name: "MinMaxOverStringExpression", sql: `SELECT MIN(n_name) AS bare, MIN(LOWER(n_name)) AS fn,
			MIN(n_name || 'x') AS cat, MIN(CASE WHEN n_regionkey = 0 THEN n_name ELSE n_name END) AS case_expr
			FROM nation`},
		pgCase{name: "StringComparisonCollation", sql: `SELECT ('B' < 'a') AS upper_first, ('a' < 'b') AS same_case, ('' < 'a') AS empty_first`},
		// An ordering over real fixture data, where the values differ only
		// after several characters.
		pgCase{name: "StringOrderFixtureNames", sql: `SELECT n_name FROM nation ORDER BY n_name`},
		// Trailing spaces: TEXT is not blank-padded, so 'a' < 'a ' < 'a  ' and
		// 'a ' < 'ab' — the space sorts before every letter by bytes.
		pgCase{name: "StringOrderTrailingSpace", sql: `SELECT r_name AS v FROM region
			UNION ALL SELECT r_name || ' ' FROM region
			UNION ALL SELECT r_name || '  ' FROM region ORDER BY 1`},
		// VALUES as a table source, which is how a client writes a literal set
		// and how the entries above would rather be spelled.
		pgCase{name: "ValuesListInFrom", sql: `SELECT v FROM (VALUES ('Zebra'), ('apple'), ('Apple')) AS t(v) ORDER BY v`},
	)

	// --- Integer arithmetic ---------------------------------------------
	//
	// PostgreSQL's `/` between two integers is INTEGER division, truncating
	// toward zero, and `%` takes the sign of the dividend. An engine that
	// promotes to float here answers 2.5 where every PostgreSQL client reads
	// 2, and no row count or NULL check sees it. (#369 was exactly that
	// engine; the fix spans the literal, column, and generic arithmetic
	// kernels.)
	out = append(out,
		pgCase{name: "IntegerDivision", sql: `SELECT 7/2 AS a, (-7)/2 AS b, 7/(-2) AS c, (-7)/(-2) AS d`},
		pgCase{name: "IntegerModulo", sql: `SELECT 7%3 AS a, (-7)%3 AS b, 7%(-3) AS c, (-7)%(-3) AS d`},
		pgCase{name: "IntegerDivisionOverColumn", sql: `SELECT n_nationkey, n_nationkey/4 AS q, n_nationkey%4 AS r FROM nation ORDER BY n_nationkey`},
		// A float operand makes it float division in both engines; the pair is
		// what localizes a divergence to the INTEGER rule.
		pgCase{name: "FloatDivisionControl", sql: `SELECT 7.0/2 AS a, 7/2.0 AS b`},
	)

	// --- Rounding at a tie ----------------------------------------------
	//
	// PostgreSQL rounds NUMERIC half away from zero and DOUBLE PRECISION
	// half-to-even, and it is the numeric rule a client sees for a literal.
	// Both spellings, because getting one right by accident is easy.
	out = append(out,
		pgCase{name: "RoundHalfNumeric", sql: `SELECT ROUND(0.5) AS a, ROUND(1.5) AS b, ROUND(2.5) AS c, ROUND(-0.5) AS d, ROUND(-1.5) AS e`},
		// The CAST(x AS double precision) spelling parses since #374.
		// compileFuncCallNode now routes ROUND(CAST(x AS double precision))
		// to a half-to-even kernel (round_half_even in internal/engine/expr)
		// so this agrees with PostgreSQL's DOUBLE PRECISION rule; ROUND on a
		// bare literal or a NUMERIC/DECIMAL cast keeps the NUMERIC rule
		// (RoundHalfNumeric above, math.Round) (#381, fixed).
		pgCase{name: "RoundHalfDouble", sql: `SELECT ROUND(CAST(0.5 AS double precision)) AS a, ROUND(CAST(1.5 AS double precision)) AS b, ROUND(CAST(2.5 AS double precision)) AS c`},
		pgCase{name: "TruncAndFloorNegative", sql: `SELECT FLOOR(-1.5) AS f, CEIL(-1.5) AS c, ABS(-1.5) AS a`},
	)

	// --- NULL semantics --------------------------------------------------
	//
	// TPC-H has no NULLs, so every rule here is untested by the 22 queries,
	// and each is one an engine can get wrong while returning a plausible
	// answer.
	// SQL's logic is THREE-valued, and the third value is the one an engine
	// built on Go booleans does not have. A comparison against NULL is
	// UNKNOWN: it does not match, and it does not anti-match either. (#370
	// added the UNKNOWN — expr.BoolNullExpr — after every operator here
	// collapsed it to false, and `1 NOT IN (2, NULL)` to true.)
	out = append(out,
		// Three-valued logic. `1 IN (2, NULL)` is UNKNOWN, not false, and
		// `NOT IN` with a NULL in the list is UNKNOWN for every probe — the
		// trap that silently empties a result.
		pgCase{name: "NullInList", sql: `SELECT (1 IN (2, NULL)) AS in_null, (1 IN (1, NULL)) AS in_match,
			(1 NOT IN (2, NULL)) AS notin_null, (1 NOT IN (2, 3)) AS notin_plain`},
		pgCase{name: "NullThreeValuedLogic", sql: `SELECT (NULL = NULL) AS eq, (NULL <> NULL) AS ne,
			(NULL IS NULL) AS isnull, (NULL IS NOT NULL) AS isnotnull,
			(TRUE OR NULL) AS or_true, (FALSE OR NULL) AS or_false,
			(TRUE AND NULL) AS and_true, (FALSE AND NULL) AS and_false`},
		pgCase{name: "NullIsDistinctFrom", sql: `SELECT (NULL IS DISTINCT FROM NULL) AS a, (1 IS DISTINCT FROM NULL) AS b,
			(1 IS NOT DISTINCT FROM 1) AS c`},
		// NULL propagates through arithmetic and concatenation rather than
		// acting as an identity element.
		pgCase{name: "NullPropagation", sql: `SELECT n_nationkey,
			NULLIF(n_regionkey, 1) + 1 AS plus,
			NULLIF(n_name, 'ALGERIA') || '!' AS cat,
			COALESCE(NULLIF(n_name, 'ALGERIA'), 'fallback') AS coalesced,
			UPPER(NULLIF(n_name, 'ARGENTINA')) AS upper_null
			FROM nation ORDER BY n_nationkey`},
		// Aggregates skip NULLs; COUNT(col) is not COUNT(*); an all-NULL input
		// aggregates to NULL except for COUNT.
		pgCase{name: "NullAggregates", sql: `SELECT COUNT(*) AS all_rows, COUNT(NULLIF(n_regionkey, 1)) AS non_null,
			SUM(NULLIF(n_regionkey, 1)) AS s, AVG(NULLIF(n_regionkey, 1)) AS a,
			MIN(NULLIF(n_regionkey, 1)) AS mn, MAX(NULLIF(n_regionkey, 1)) AS mx FROM nation`},
		pgCase{name: "NullAllNullAggregate", sql: `SELECT SUM(NULLIF(n_regionkey, n_regionkey)) AS s,
			AVG(NULLIF(n_regionkey, n_regionkey)) AS a, MIN(NULLIF(n_regionkey, n_regionkey)) AS mn,
			COUNT(NULLIF(n_regionkey, n_regionkey)) AS c FROM nation`},
		// An aggregate over ZERO rows still returns exactly one row: SQL has no
		// case where an ungrouped aggregate answers "no rows".
		pgCase{name: "AggregateOverEmptyInput", sql: `SELECT COUNT(*) AS c, SUM(n_regionkey) AS s, AVG(n_regionkey) AS a,
			MIN(n_name) AS mn FROM nation WHERE n_nationkey < 0`},
		// GROUP BY and DISTINCT treat NULLs as EQUAL to each other — the one
		// place SQL does.
		pgCase{name: "NullGroupsTogether", sql: `SELECT NULLIF(n_regionkey, 1) AS k, COUNT(*) AS c
			FROM nation GROUP BY NULLIF(n_regionkey, 1) ORDER BY k`},
		pgCase{name: "NullDistinct", sql: `SELECT DISTINCT NULLIF(n_regionkey, 1) AS k FROM nation ORDER BY k`},
		// The empty string is not NULL. A renderer that folds them together
		// calls these two answers the same.
		pgCase{name: "EmptyStringIsNotNull", sql: `SELECT ('' IS NULL) AS empty_is_null, LENGTH('') AS len,
			(('' = '') IS TRUE) AS empty_eq_empty, COALESCE(NULLIF('', ''), 'was_empty') AS folded`},
		pgCase{name: "NullAndEmptyStringColumn", sql: `SELECT n_nationkey,
			CASE WHEN n_nationkey % 3 = 0 THEN NULL WHEN n_nationkey % 3 = 1 THEN '' ELSE n_name END AS mixed
			FROM nation ORDER BY n_nationkey`},
	)

	// --- Comparison against a NULL literal --------------------------------
	//
	// `col = NULL` is UNKNOWN, never TRUE, which is why `IS NULL` exists as a
	// separate operator. Wadjet lowered the shape to a typed kernel and the
	// nil constant was coerced to the column type's ZERO, so `= NULL` matched
	// the rows holding 0 or '' and `<> NULL` matched everything else (#450).
	//
	// PostgreSQL is the authority here and answers all of these with the
	// empty set — except the three that are deliberately NOT empty: a NULL
	// inside an IN list drops out instead of poisoning it, a NULL BETWEEN
	// bound leaves the other bound standing under NOT BETWEEN, and an OR
	// keeps its other arm. A gate made only of empty answers would pass on an
	// engine that returned nothing for everything.
	out = append(out,
		pgCase{name: "NullLiteralEq", sql: `SELECT COUNT(*) AS n FROM nation WHERE n_regionkey = NULL`},
		pgCase{name: "NullLiteralNe", sql: `SELECT COUNT(*) AS n FROM nation WHERE n_regionkey <> NULL`},
		pgCase{name: "NullLiteralLt", sql: `SELECT COUNT(*) AS n FROM nation WHERE n_regionkey < NULL`},
		pgCase{name: "NullLiteralGe", sql: `SELECT COUNT(*) AS n FROM nation WHERE n_regionkey >= NULL`},
		pgCase{name: "NullLiteralFlipped", sql: `SELECT COUNT(*) AS n FROM nation WHERE NULL = n_regionkey`},
		pgCase{name: "NullLiteralString", sql: `SELECT COUNT(*) AS n FROM nation WHERE n_name = NULL`},
		pgCase{name: "NullLiteralStringGt", sql: `SELECT COUNT(*) AS n FROM nation WHERE n_name > NULL`},
		pgCase{name: "NullLiteralLike", sql: `SELECT COUNT(*) AS n FROM nation WHERE n_name LIKE NULL`},
		pgCase{name: "NullLiteralNotLike", sql: `SELECT COUNT(*) AS n FROM nation WHERE n_name NOT LIKE NULL`},
		pgCase{name: "NullLiteralNegated", sql: `SELECT COUNT(*) AS n FROM nation WHERE NOT (n_regionkey = NULL)`},
		pgCase{name: "NullLiteralInAlone", sql: `SELECT COUNT(*) AS n FROM nation WHERE n_regionkey IN (NULL)`},
		pgCase{name: "NullLiteralNotInWithValue", sql: `SELECT COUNT(*) AS n FROM nation WHERE n_regionkey NOT IN (1, NULL)`},
		pgCase{name: "NullLiteralBetween", sql: `SELECT COUNT(*) AS n FROM nation WHERE n_regionkey BETWEEN NULL AND 2`},
		// The three that must NOT come back empty.
		pgCase{name: "NullLiteralInWithValue", sql: `SELECT n_nationkey FROM nation
			WHERE n_regionkey IN (1, NULL) ORDER BY n_nationkey`},
		pgCase{name: "NullLiteralNotBetween", sql: `SELECT n_nationkey FROM nation
			WHERE n_regionkey NOT BETWEEN NULL AND 2 ORDER BY n_nationkey`},
		pgCase{name: "NullLiteralOrKeepsOtherArm", sql: `SELECT n_nationkey FROM nation
			WHERE n_regionkey = NULL OR n_nationkey < 5 ORDER BY n_nationkey`},
		// On a genuinely NULLABLE column, where IS NULL and = NULL are two
		// different questions with two different answers.
		pgCase{name: "NullLiteralEqNullable", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 = NULL`},
		pgCase{name: "NullLiteralNeNullable", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_2 <> NULL`},
		pgCase{name: "NullLiteralIsNullContrast", sql: `SELECT
			(SELECT COUNT(*) FROM dec_probe WHERE d_2 = NULL) AS eq_null,
			(SELECT COUNT(*) FROM dec_probe WHERE d_2 IS NULL) AS is_null,
			(SELECT COUNT(*) FROM dec_probe WHERE d_2 IS NOT NULL) AS is_not_null`},
	)

	// --- Negated predicates ----------------------------------------------
	//
	// `WHERE NOT (<predicate>)` was executed as `WHERE <predicate>` whenever
	// the inner predicate vectorized: the physical planner's filter lowering
	// returned the operand's operators UN-negated, so the single-process
	// engine answered the COMPLEMENT of the row set (#461) — the distributed
	// worker compiles scan filters straight to the row evaluator and never
	// hit this. A complement is a plausible-looking answer, which is how it
	// survived every existing gate — including the two-path one, whose
	// corpus had no negated predicate to send the single-process arm down
	// the buggy path in the first place. That gate compares the two arms to
	// each other, not to SQL; a corpus gap, not a lowering the arms share,
	// is what hid it.
	//
	// Half of these negate a predicate on a NULLABLE column, where the answer
	// is NOT the complement: NOT UNKNOWN is UNKNOWN, so a NULL row belongs to
	// neither the predicate's answer nor its negation's. dec_probe nulls d_2
	// every 17th row, which is what makes that half a different question from
	// the nation half.
	out = append(out,
		pgCase{name: "NotEquality", sql: `SELECT n_nationkey FROM nation WHERE NOT (n_regionkey = 1) ORDER BY n_nationkey`},
		pgCase{name: "NotInequality", sql: `SELECT n_nationkey FROM nation WHERE NOT (n_regionkey <> 1) ORDER BY n_nationkey`},
		pgCase{name: "NotLessThan", sql: `SELECT n_nationkey FROM nation WHERE NOT (n_nationkey < 10) ORDER BY n_nationkey`},
		pgCase{name: "NotGreaterEqual", sql: `SELECT n_nationkey FROM nation WHERE NOT (n_nationkey >= 10) ORDER BY n_nationkey`},
		pgCase{name: "NotInList", sql: `SELECT n_nationkey FROM nation WHERE NOT (n_nationkey IN (1, 2, 3)) ORDER BY n_nationkey`},
		pgCase{name: "NotNotInList", sql: `SELECT n_nationkey FROM nation WHERE NOT (n_nationkey NOT IN (1, 2, 3)) ORDER BY n_nationkey`},
		pgCase{name: "NotBetween", sql: `SELECT n_nationkey FROM nation WHERE NOT (n_nationkey BETWEEN 10 AND 19) ORDER BY n_nationkey`},
		pgCase{name: "NotNotBetween", sql: `SELECT n_nationkey FROM nation WHERE NOT (n_nationkey NOT BETWEEN 10 AND 19) ORDER BY n_nationkey`},
		pgCase{name: "NotLike", sql: `SELECT n_name FROM nation WHERE NOT (n_name LIKE 'A%') ORDER BY n_name`},
		pgCase{name: "NotNotLike", sql: `SELECT n_name FROM nation WHERE NOT (n_name NOT LIKE 'A%') ORDER BY n_name`},
		pgCase{name: "NotDoubleNegation", sql: `SELECT n_nationkey FROM nation WHERE NOT (NOT (n_regionkey = 1)) ORDER BY n_nationkey`},
		// De Morgan, both directions.
		pgCase{name: "NotOfAnd", sql: `SELECT n_nationkey FROM nation
			WHERE NOT (n_regionkey = 1 AND n_nationkey < 10) ORDER BY n_nationkey`},
		pgCase{name: "NotOfOr", sql: `SELECT n_nationkey FROM nation
			WHERE NOT (n_regionkey = 1 OR n_nationkey < 10) ORDER BY n_nationkey`},
		// A negation the lowering cannot vectorize must keep it too: the row
		// evaluator is the fallback, and dropping the NOT there would be the
		// same defect one layer down.
		pgCase{name: "NotOverFunctionCall", sql: `SELECT n_nationkey FROM nation
			WHERE NOT (ABS(n_nationkey) < 10) ORDER BY n_nationkey`},

		// The nullable arm.
		pgCase{name: "NotEqualityNullable", sql: `SELECT d_key FROM dec_probe WHERE NOT (d_2 = 12.75) ORDER BY d_key`},
		pgCase{name: "NotInequalityNullable", sql: `SELECT d_key FROM dec_probe WHERE NOT (d_2 <> 12.75) ORDER BY d_key`},
		pgCase{name: "NotLessThanNullable", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE NOT (d_2 < 0)`},
		pgCase{name: "NotInListNullable", sql: `SELECT d_key FROM dec_probe WHERE NOT (d_2 IN (12.75, -20.00, 0.25)) ORDER BY d_key`},
		pgCase{name: "NotNotInListNullable", sql: `SELECT d_key FROM dec_probe WHERE NOT (d_2 NOT IN (12.75, -20.00, 0.25)) ORDER BY d_key`},
		pgCase{name: "NotBetweenNullable", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE NOT (d_2 BETWEEN -1 AND 1)`},
		pgCase{name: "NotNotBetweenNullable", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE NOT (d_2 NOT BETWEEN -1 AND 1)`},
		pgCase{name: "NotIsNullNullable", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE NOT (d_2 IS NULL)`},
		pgCase{name: "NotIsNotNullNullable", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE NOT (d_2 IS NOT NULL)`},
		// Kleene, not Boolean: NOT (A AND B) is TRUE wherever either side is
		// FALSE, even where the other is UNKNOWN — so these two are not
		// complements of each other, and neither is the complement of the
		// un-negated form.
		pgCase{name: "NotOfAndWithNullable", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE NOT (d_key < 100 AND d_2 > 0)`},
		pgCase{name: "NotOfOrWithNullable", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE NOT (d_key < 100 OR d_2 > 0)`},
		// Two nullable columns, nulling on different strides.
		pgCase{name: "NotOfAndTwoNullables", sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE NOT (d_2 > 0 AND d_4 > 0)`},
	)

	// --- Set operations --------------------------------------------------
	//
	// UNION dedups and UNION ALL does not; EXCEPT and INTERSECT dedup and their
	// ALL forms carry multiplicity; an ORDER BY after a set operation sorts the
	// WHOLE result and its ordinal refers to the result's columns.
	out = append(out,
		pgCase{name: "UnionAllOrderByOrdinal", sql: `SELECT r_regionkey FROM region UNION ALL SELECT r_regionkey FROM region ORDER BY 1`},
		pgCase{name: "UnionDedup", sql: `SELECT n_regionkey FROM nation WHERE n_nationkey < 5 UNION
			SELECT n_regionkey FROM nation WHERE n_nationkey >= 5 ORDER BY 1`},
		pgCase{name: "ExceptDedup", sql: `SELECT n_regionkey FROM nation EXCEPT SELECT r_regionkey FROM region WHERE r_regionkey < 2 ORDER BY 1`},
		pgCase{name: "IntersectDedup", sql: `SELECT n_regionkey FROM nation INTERSECT SELECT r_regionkey FROM region ORDER BY 1`},
		// NULL is a member like any other in a set operation, and matches a
		// NULL on the other side.
		pgCase{name: "UnionWithNulls", sql: `SELECT NULLIF(n_regionkey, 1) AS k FROM nation
			UNION SELECT NULLIF(r_regionkey, 1) FROM region ORDER BY k`},
		// The ALL forms carry multiplicity — min(countA, countB) and
		// max(0, countA−countB) copies per row — and differ from the
		// distinct forms exactly when an arm holds duplicates, which
		// nation's five-nations-per-region guarantees (#346).
		pgCase{name: "IntersectAllMultiplicity", sql: `SELECT n_regionkey FROM nation
			INTERSECT ALL SELECT r_regionkey FROM region ORDER BY 1`},
		pgCase{name: "ExceptAllMultiplicity", sql: `SELECT n_regionkey FROM nation
			EXCEPT ALL SELECT r_regionkey FROM region ORDER BY 1`},
		// A set operation decides membership by EQUALITY, so its dedup key
		// has to agree with the comparator — and two DECIMAL columns of
		// DIFFERENT scale are where those two can part company: 12.75 and
		// 12.7500 are one number and two renderings, and the local path keyed
		// on the rendering (#499). d_2 is (d_key-100)*0.25 and d_4 is
		// (d_key-100)*0.0625, so the two arms overlap on a quarter of their
		// values and neither contains the other.
		pgCase{name: "UnionAcrossDecimalScales",
			sql: `SELECT COUNT(*) AS n FROM (SELECT d_2 AS v FROM dec_probe UNION SELECT d_4 FROM dec_probe) u`},
		pgCase{name: "UnionAllAcrossDecimalScales",
			sql: `SELECT COUNT(*) AS n FROM (SELECT d_2 AS v FROM dec_probe UNION ALL SELECT d_4 FROM dec_probe) u`},
		pgCase{name: "IntersectAcrossDecimalScales",
			sql: `SELECT COUNT(*) AS n FROM (SELECT d_2 AS v FROM dec_probe INTERSECT SELECT d_4 FROM dec_probe) u`},
		pgCase{name: "ExceptAcrossDecimalScales",
			sql: `SELECT COUNT(*) AS n FROM (SELECT d_2 AS v FROM dec_probe EXCEPT SELECT d_4 FROM dec_probe) u`},
		pgCase{name: "ExceptReversedAcrossDecimalScales",
			sql: `SELECT COUNT(*) AS n FROM (SELECT d_4 AS v FROM dec_probe EXCEPT SELECT d_2 FROM dec_probe) u`},
		// The tell from the filing: GROUP BY over the same concatenation keys
		// through the COLUMNAR encoding and was already right, so it has to
		// agree with the set operation above — one engine, one answer to "are
		// these the same value".
		pgCase{name: "GroupByAcrossDecimalScales",
			sql: `SELECT COUNT(*) AS n FROM (SELECT v FROM
				(SELECT d_2 AS v FROM dec_probe UNION ALL SELECT d_4 FROM dec_probe) t GROUP BY v) g`},
		// The VALUES, not only their count: two wrong keys can cancel.
		pgCase{name: "UnionAcrossDecimalScalesRows",
			sql: `SELECT v FROM (SELECT d_2 AS v FROM dec_probe UNION SELECT d_4 FROM dec_probe) u ORDER BY v`},
	)

	// --- Set operations over NUMERIC arms of different type ---------------
	//
	// A set operation's result type is the COMMON type of its arms, and for
	// the numeric family PostgreSQL resolves it as: numeric over an integer
	// (an integer converts to numeric implicitly and not back), and double
	// precision over numeric (float8 is the PREFERRED type of the category).
	// Arm ORDER does not change either answer. Verified against live
	// postgres:17-alpine; these entries are what keeps the engine's ladder
	// (physical.setOpWiden) tied to it.
	//
	// The d_key list is chosen so every d_4 value it selects is a whole
	// number of HUNDREDTHS — d_4 is (d_key-100)*0.0625, exact at scale 2
	// whenever d_key-100 is a multiple of four. That isolates the TYPE
	// question from the SCALE question: these rows have the same value at
	// either scale, so an entry that fails here failed on the type
	// resolution and not on a lost digit.
	//
	// Value selection like that is exactly what ADR-0012 item 3 forbids as a
	// way of DODGING a divergence, so the entry immediately after these picks
	// d_key values whose d_4 is NOT a whole hundredth and is PINNED to the
	// divergence it then finds (#532). The narrow entries above are the
	// isolation; the pinned one is the coverage, and it fails the day #532
	// lands.
	out = append(out,
		pgCase{name: "SetOpUnionAllAcrossDecimalScales", ordered: true,
			sql: `SELECT d_2 AS v FROM dec_probe WHERE d_key IN (0, 4, 8, 92, 96, 100, 104, 108, 196)
				UNION ALL SELECT d_4 FROM dec_probe WHERE d_key IN (0, 4, 8, 92, 96, 100, 104, 108, 196)
				ORDER BY 1`},
		pgCase{name: "SetOpUnionAllAcrossDecimalScalesReversed", ordered: true,
			sql: `SELECT d_4 AS v FROM dec_probe WHERE d_key IN (0, 4, 8, 92, 96, 100, 104, 108, 196)
				UNION ALL SELECT d_2 FROM dec_probe WHERE d_key IN (0, 4, 8, 92, 96, 100, 104, 108, 196)
				ORDER BY 1`},
		// The same-scale control: both arms already agree, so nothing is
		// coerced and the answer must not move.
		pgCase{name: "SetOpUnionAllSameDecimalScale", ordered: true,
			sql: `SELECT d_2 AS v FROM dec_probe WHERE d_key IN (1, 2, 3)
				UNION ALL SELECT d_2 FROM dec_probe WHERE d_key IN (2, 3, 4) ORDER BY 1`},
		// numeric with double precision resolves to double precision, so the
		// numeric arm's values are the ones that move.
		pgCase{name: "SetOpUnionAllDecimalWithDouble", ordered: true,
			sql: `SELECT d_2 AS v FROM dec_probe WHERE d_key IN (0, 4, 8, 96, 100, 104)
				UNION ALL SELECT CAST(d_key AS DOUBLE PRECISION) FROM dec_probe WHERE d_key IN (0, 4, 8)
				ORDER BY 1`},
		// The SCALE question, not dodged: d_key 1/2/3 give d_4 values of
		// -6.1875, -6.1250 and -6.0625, none of which is a whole number of
		// hundredths. The single-process engine used to re-read every row's
		// rendered text at the FIRST arm's scale, truncating those to
		// -6.18 / -6.12 / -6.06 and answering six values PostgreSQL does not
		// have. Fixed by widening the result to both arms' declared DECIMAL
		// scale before boxing, same as the stage DAG (#533). Gated, not pinned:
		// this entry WAS the #532 pin, and its agreeing is that fix's proof.
		// Refs #532.
		pgCase{name: "SetOpUnionAllAcrossDecimalScalesLosesDigits", ordered: true,
			sql: `SELECT d_2 AS v FROM dec_probe WHERE d_key IN (1, 2, 3)
				UNION ALL SELECT d_4 FROM dec_probe WHERE d_key IN (1, 2, 3)
				ORDER BY 1`},
		// The same pair with the FLOAT arm first, which the single-process
		// engine — the arm this corpus runs — could not answer at all: it
		// boxed each row and handed them to batch.FromRows under the FIRST
		// arm's schema, and a DECIMAL boxes as its rendered TEXT, so the #361
		// guard failed the store and the whole query with it. Gated, not
		// pinned: this entry WAS the #541 pin, and its agreeing is that fix's
		// proof. unifySetOpSchemas now resolves the arms' common type through
		// the same setOpWiden / setOpDecimalTarget the stage DAG uses
		// (physical.reconcileSetOpArmTypes) and MOVES each arm's boxes into
		// it, so the two paths cannot drift on the type or the values.
		// Refs #541.
		pgCase{name: "SetOpUnionAllDoubleWithDecimal", ordered: true,
			sql: `SELECT CAST(d_key AS DOUBLE PRECISION) AS v FROM dec_probe WHERE d_key IN (0, 4, 8)
				UNION ALL SELECT d_2 FROM dec_probe WHERE d_key IN (0, 4, 8, 96, 100, 104)
				ORDER BY 1`},
	)

	// --- Pagination ------------------------------------------------------
	out = append(out,
		pgCase{name: "OffsetAlone", sql: `SELECT n_nationkey FROM nation ORDER BY n_nationkey OFFSET 5`},
		pgCase{name: "OffsetThenLimit", sql: `SELECT n_nationkey FROM nation ORDER BY n_nationkey OFFSET 5 LIMIT 3`},
		pgCase{name: "LimitThenOffset", sql: `SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3 OFFSET 5`},
		pgCase{name: "OffsetPastEnd", sql: `SELECT n_nationkey FROM nation ORDER BY n_nationkey OFFSET 1000`,
			countOnly: true, why: "an empty result has no rows to fingerprint; at tolerance 0 the count is the entire answer"},
		// #481: `ORDER BY ... LIMIT 0` returned every row instead of zero — a
		// sentinel collision (0 doubled as both "a real LIMIT 0" and "no
		// limit at all") across exec.Sort.Limit, sortSourceAdapter's Top-K
		// guard, and the coordinator's MergeInfo.KeepRows. Plain `LIMIT 0`
		// (no ORDER BY) and `LIMIT 0 OFFSET n` never touched that
		// convention and already answered correctly, which is why they are
		// pinned here too — as a guard against the fix regressing them,
		// not because either was ever broken.
		pgCase{name: "OrderByLimitZero", sql: `SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 0`,
			countOnly: true, why: "an empty result has no rows to fingerprint; at tolerance 0 the count is the entire answer"},
		pgCase{name: "OrderByLimitZeroOffset", sql: `SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 0 OFFSET 5`,
			countOnly: true, why: "an empty result has no rows to fingerprint; at tolerance 0 the count is the entire answer"},
		pgCase{name: "PlainLimitZeroNoOrderBy", sql: `SELECT n_nationkey FROM nation LIMIT 0`,
			countOnly: true, why: "an empty result has no rows to fingerprint; at tolerance 0 the count is the entire answer"},
	)

	// --- A bound one level down: LIMIT/OFFSET inside a derived table ------
	//
	// #478: the DAG applied a LIMIT in exactly two places — the
	// coordinator's post-gather pass, which reads the plan ROOT, and a sort
	// stage's top-N, which needs an ORDER BY below the LIMIT and truncates
	// to limit+OFFSET without ever skipping. A LIMIT that reached neither
	// bounded nothing, so the derived table yielded every row and the outer
	// query computed over all of them. PostgreSQL says what each of these
	// answers; the corpus had no derived-table LIMIT of any kind.
	//
	// `LIMIT n` over an unordered derived table does not say WHICH rows it
	// yields, but the outer COUNT(*) makes the answer determined either
	// way, so a value compare is sound (ADR-0013's nondeterminism list
	// covers the row identity, not the cardinality).
	out = append(out,
		pgCase{name: "DerivedLimitUnderCount",
			sql: `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation LIMIT 3) u`},
		pgCase{name: "DerivedLimitOffsetUnderCount",
			sql: `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation LIMIT 3 OFFSET 5) u`},
		pgCase{name: "DerivedDistinctLimitUnderCount",
			sql: `SELECT COUNT(*) AS c FROM (SELECT DISTINCT n_regionkey FROM nation LIMIT 2) u`},
		pgCase{name: "DerivedGroupByLimitUnderCount",
			sql: `SELECT COUNT(*) AS c FROM (SELECT n_regionkey FROM nation GROUP BY n_regionkey LIMIT 2) u`},
		pgCase{name: "DerivedLimitZeroUnderCount",
			sql: `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation LIMIT 0) u`},
		pgCase{name: "DerivedOffsetAloneUnderCount",
			sql: `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation OFFSET 20) u`},
		pgCase{name: "DerivedOrderByLimitOffsetUnderCount",
			sql: `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3 OFFSET 5) u`},
		// The OFFSET must skip the RIGHT rows, not just the right number of
		// them — this is the entry that would catch a bound applied without
		// its skip.
		pgCase{name: "DerivedOrderByLimitOffsetValues",
			sql: `SELECT n_nationkey FROM (SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3 OFFSET 5) u
				ORDER BY n_nationkey`},
		pgCase{name: "DerivedLimitFeedsJoin",
			sql: `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3) u
				JOIN region r ON u.n_nationkey = r.r_regionkey`},
		pgCase{name: "DerivedLimitFeedsWindow",
			sql: `SELECT MAX(rn) AS c FROM (SELECT n_nationkey, ROW_NUMBER() OVER (ORDER BY n_nationkey) AS rn
				FROM (SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 4) v) w`},
		// #525: a LIMIT under a LIMIT. Every entry above has exactly one per
		// query, and one per query is what made the ownership rule look
		// disjoint: walkStages scanned backwards for a sort to bound and
		// reached the INNER LIMIT's, overwrote its 3 with the outer's 5, and
		// then suppressed the outer's own stage on the strength of the sort
		// it had just mis-claimed. `LIMIT <page>` over an already-bounded
		// inner query is an ordinary BI pagination spelling.
		pgCase{name: "NestedLimitSortedInner",
			sql: `SELECT COUNT(*) AS c FROM
				(SELECT n_nationkey FROM
					(SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3) i LIMIT 5) o`},
		pgCase{name: "NestedLimitSortedInnerOuterOffset",
			sql: `SELECT COUNT(*) AS c FROM
				(SELECT n_nationkey FROM
					(SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3) i LIMIT 5 OFFSET 1) o`},
		pgCase{name: "NestedLimitOuterTighter",
			sql: `SELECT COUNT(*) AS c FROM
				(SELECT n_nationkey FROM
					(SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 5) i LIMIT 2) o`},
		pgCase{name: "NestedLimitInnerOffsetOnly",
			sql: `SELECT COUNT(*) AS c FROM
				(SELECT n_nationkey FROM
					(SELECT n_nationkey FROM nation ORDER BY n_nationkey OFFSET 20) i LIMIT 3) o`},
		pgCase{name: "NestedLimitBareInner",
			sql: `SELECT COUNT(*) AS c FROM
				(SELECT n_nationkey FROM
					(SELECT n_nationkey FROM nation LIMIT 3) i LIMIT 5) o`},
		// The VALUES, at the root and one level down: the two bounds have to
		// COMPOSE, so the OFFSET skips into the inner's three rows rather
		// than into the whole relation.
		pgCase{name: "NestedLimitRootOuterValues",
			sql: `SELECT n_nationkey FROM
				(SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3) i LIMIT 5`,
			ordered: true},
		pgCase{name: "NestedLimitOffsetValues",
			sql: `SELECT n_nationkey FROM
				(SELECT n_nationkey FROM
					(SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3) i LIMIT 5 OFFSET 1) o
				ORDER BY n_nationkey`,
			ordered: true},
	)

	// --- Subquery predicates: where a bound inside a subquery binds --------
	//
	// `IN (SELECT ...)` lowers to a semi join, and until #516/#482 the
	// lowering got both halves of "what does the subquery yield" wrong: it
	// named the join key from the SELECT list as written (so an aliased
	// self-IN matched nothing and answered ZERO rows) and it dropped the
	// subquery's LIMIT entirely (so a bounded subquery matched against the
	// FULL unbounded column and the predicate selected every row, for any n).
	// PostgreSQL is the authority on both, and neither shape was in this
	// corpus at all — there was no `IN (SELECT ...)` entry of any kind.
	//
	// Every bounded entry carries an ORDER BY inside the subquery: a bare
	// LIMIT does not say WHICH rows it yields, so the two engines may
	// legitimately pick different ones (ADR-0013's nondeterminism list).
	//
	// These run the EngineSemantics arm, which is embedded and single-process
	// by construction — it is where PostgreSQL's answer is established. That
	// used to be the whole of their coverage, because a bounded subquery
	// could not execute on the stage DAG at all (#524). It can now, and the
	// DAG's agreement with this arm is gated separately, entry for entry, in
	// two_path_invariance_test.go.
	out = append(out,
		pgCase{name: "InSubqueryAliasedSelfJoin",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_regionkey IN
				(SELECT b.n_regionkey FROM nation b WHERE b.n_nationkey < 10)`},
		pgCase{name: "InSubqueryAliasedSelectItem",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_regionkey IN
				(SELECT b.n_regionkey AS rk FROM nation b WHERE b.n_nationkey < 10)`},
		pgCase{name: "NotInSubqueryAliasedSelfJoin",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_nationkey NOT IN
				(SELECT b.n_nationkey FROM nation b WHERE b.n_nationkey < 10)`},
		pgCase{name: "InSubqueryGroupedAliased",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_regionkey IN
				(SELECT b.n_regionkey FROM nation b GROUP BY b.n_regionkey HAVING COUNT(*) > 1)`},
		pgCase{name: "InSubqueryOrderedLimit",
			sql: `SELECT COUNT(*) AS c FROM nation WHERE n_nationkey IN
				(SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3)`},
		pgCase{name: "InSubqueryOrderedLimitOffset",
			sql: `SELECT COUNT(*) AS c FROM nation WHERE n_nationkey IN
				(SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3 OFFSET 5)`},
		pgCase{name: "InSubqueryLimitZero",
			sql: `SELECT COUNT(*) AS c FROM nation WHERE n_nationkey IN
				(SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 0)`},
		pgCase{name: "NotInSubqueryOrderedLimit",
			sql: `SELECT COUNT(*) AS c FROM nation WHERE n_nationkey NOT IN
				(SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3)`},
	)

	// --- The same predicate over a subquery that JOINS ---------------------
	//
	// Every entry above reads ONE relation, and that is the premise #516's
	// fix rests on: with one relation, the inner plan's bottom Scan emits the
	// select item under its bare source name, so a qualifier on it can be
	// stripped. A subquery that JOINS breaks the premise — which relation's
	// columns come out of the join bare is decided by reorderJoins from
	// estimated row counts, at Optimize step 73, long after decorrelation has
	// named the key at step 36 — and stripping anyway answers over whichever
	// relation the estimator happened to put on the probe.
	//
	// The self-join is what makes that visible: both inner relations carry
	// `n_nationkey`, and the filtered one is the SMALLER, so a strip resolves
	// to the other relation's column and the answer becomes 10 instead of 3.
	// Only a self-join can show it in this schema, because TPC-H prefixes
	// every column with its table and nothing else collides.
	//
	// All five were #526 pins: a QUALIFIED item over a joined inner named a
	// column the semi join's build schema does not carry, exec.HashJoin's key
	// repair swapped the pair on #516's false premise, and the join matched
	// nothing — IN answered 0 and NOT IN answered every row. They are plain
	// entries now: the decorrelations record the relation and column each
	// build-side reference MEANS, and repairDecorrelatedSpelling settles the
	// text after reorderJoins has decided which relation's columns the inner
	// join emits bare.
	out = append(out,
		pgCase{name: "InSubqueryJoinedInnerLeadQualified",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_nationkey IN
				(SELECT c.n_nationkey FROM nation c JOIN nation b ON b.n_regionkey = c.n_regionkey
				 WHERE c.n_nationkey < 3)`},
		pgCase{name: "InSubqueryJoinedInnerLeadQualifiedValues",
			sql: `SELECT a.n_nationkey AS k FROM nation a WHERE a.n_nationkey IN
				(SELECT c.n_nationkey FROM nation c JOIN nation b ON b.n_regionkey = c.n_regionkey
				 WHERE c.n_nationkey < 3)
				ORDER BY k`, ordered: true},
		pgCase{name: "NotInSubqueryJoinedInnerLeadQualified",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_nationkey NOT IN
				(SELECT c.n_nationkey FROM nation c JOIN nation b ON b.n_regionkey = c.n_regionkey
				 WHERE c.n_nationkey < 3)`},
		// Across two DIFFERENT tables, where no bare name collides. This is
		// the ORDINARY spelling of a joined inner and it was wrong for the
		// same reason, which is what said #526 is a property of the JOIN and
		// not of the name collision the self-join above needs. The non-lead
		// twin is the one no strip could have got right: with nothing
		// colliding the join emits the column BARE, so the qualified spelling
		// names nothing whichever side the estimator picks.
		pgCase{name: "InSubqueryJoinedInnerCrossTable",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_regionkey IN
				(SELECT r.r_regionkey FROM region r JOIN nation b ON b.n_regionkey = r.r_regionkey
				 WHERE r.r_regionkey < 2)`},
		pgCase{name: "InSubqueryJoinedInnerCrossTableNonLead",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_regionkey IN
				(SELECT b.n_regionkey FROM region r JOIN nation b ON b.n_regionkey = r.r_regionkey
				 WHERE r.r_regionkey < 2)`},
		// #527: a correlated EXISTS over a joined inner, both directions.
		// Both inner relations carry n_nationkey, so a stripped correlation
		// column resolved to whichever one reorderJoins put on the probe.
		pgCase{name: "ExistsJoinedInnerQualifiedCorrelation",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE EXISTS
				(SELECT 1 FROM nation c JOIN nation b ON b.n_regionkey = c.n_regionkey
				 WHERE c.n_nationkey = a.n_nationkey AND b.n_nationkey < 3)`},
		pgCase{name: "NotExistsJoinedInnerQualifiedCorrelation",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE NOT EXISTS
				(SELECT 1 FROM nation c JOIN nation b ON b.n_regionkey = c.n_regionkey
				 WHERE c.n_nationkey = a.n_nationkey AND b.n_nationkey < 3)`},
		// An INEQUALITY correlation rides the semi join's JoinFilter rather
		// than a key, and the physical planner's filter builders stripped the
		// qualifier a second time (#527).
		pgCase{name: "ExistsJoinedInnerInequalityCorrelation",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE EXISTS
				(SELECT 1 FROM nation c JOIN nation b ON b.n_regionkey = c.n_regionkey
				 WHERE c.n_regionkey = a.n_regionkey AND c.n_nationkey > a.n_nationkey)`},
	)

	// --- The same predicate over a DERIVED-TABLE inner (#571) -------------
	//
	// The parser keeps a FROM-subquery as a table whose NAME is its own SQL
	// text, and the three decorrelations called NewScan on that text — a scan
	// of a table the catalog does not have, which yields ZERO batches and no
	// error. The semi/anti join's build side was therefore empty, so `IN`
	// answered nothing and `NOT IN` answered every row. The rewrites decline
	// this shape now and the predicate is executed as written.
	//
	// The last pair is the shape the issue was filed from: a NULL reaches the
	// derived list, so NOT IN's three-valued rule (#507) has to survive the
	// route the decline takes. An empty build side answered 0 for that one
	// too, by accident — the IN twin is what tells the two apart.
	out = append(out,
		pgCase{name: "InSubqueryDerivedInner",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_regionkey IN
				(SELECT s.rk FROM (SELECT b.n_regionkey AS rk FROM nation b WHERE b.n_nationkey < 3) s)`},
		pgCase{name: "NotInSubqueryDerivedInner",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_regionkey NOT IN
				(SELECT s.rk FROM (SELECT b.n_regionkey AS rk FROM nation b WHERE b.n_nationkey < 3) s)`},
		pgCase{name: "InSubqueryDerivedInnerValues",
			sql: `SELECT a.n_nationkey AS k FROM nation a WHERE a.n_regionkey IN
				(SELECT s.rk FROM (SELECT b.n_regionkey AS rk FROM nation b WHERE b.n_nationkey < 3) s)
				ORDER BY k`, ordered: true},
		pgCase{name: "InSubqueryDerivedInnerJoined",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_nationkey IN
				(SELECT s.k FROM (SELECT c.n_nationkey AS k, c.n_regionkey AS rk FROM nation c) s
				 JOIN nation b ON b.n_regionkey = s.rk WHERE s.k < 3)`},
		pgCase{name: "NotInSubqueryDerivedInnerJoined",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_nationkey NOT IN
				(SELECT s.k FROM (SELECT c.n_nationkey AS k, c.n_regionkey AS rk FROM nation c) s
				 JOIN nation b ON b.n_regionkey = s.rk WHERE s.k < 3)`},
		pgCase{name: "ExistsDerivedInnerCorrelation",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE EXISTS
				(SELECT 1 FROM (SELECT b.n_regionkey AS rk FROM nation b WHERE b.n_nationkey < 3) s
				 WHERE s.rk = a.n_regionkey)`},
		pgCase{name: "NotExistsDerivedInnerCorrelation",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE NOT EXISTS
				(SELECT 1 FROM (SELECT b.n_regionkey AS rk FROM nation b WHERE b.n_nationkey < 3) s
				 WHERE s.rk = a.n_regionkey)`},
		pgCase{name: "ScalarSubqueryDerivedInner",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_nationkey >
				(SELECT MAX(s.k) FROM (SELECT b.n_nationkey AS k FROM nation b WHERE b.n_regionkey = 0) s)`},
		pgCase{name: "NotInSubqueryDerivedInnerNullInList",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_regionkey NOT IN
				(SELECT s.rk FROM (SELECT r.r_regionkey AS rk FROM nation b
				 LEFT JOIN region r ON r.r_regionkey = b.n_regionkey AND r.r_regionkey < 2) s)`},
		pgCase{name: "InSubqueryDerivedInnerNullInList",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_regionkey IN
				(SELECT s.rk FROM (SELECT r.r_regionkey AS rk FROM nation b
				 LEFT JOIN region r ON r.r_regionkey = b.n_regionkey AND r.r_regionkey < 2) s)`},
	)

	// --- NOT IN's three-valued rule over NULLs (#507) ------------------
	//
	// `decorrelateInSubqueries` lowers NOT IN to an anti join, and an anti
	// join asks a TWO-valued question: did this probe row match nothing.
	// NOT IN's rule is three-valued — UNKNOWN, so WHERE drops the row, the
	// moment the probe key is NULL or the subquery returned a NULL the key
	// did not match on some other value. Every TPC-H column is NOT NULL, so
	// the NULLs have to be manufactured, and a LEFT JOIN is the way to do it
	// that both engines lower the same way.
	out = append(out,
		// A NULL PROBE key: the outer LEFT JOIN leaves r_regionkey NULL for
		// every nation outside regions 0-1, and those rows compare UNKNOWN
		// against every list value whether they matched or not.
		pgCase{name: "NotInSubqueryNullProbeKey",
			sql: `SELECT COUNT(*) AS c FROM nation a
				LEFT JOIN region r ON r.r_regionkey = a.n_regionkey AND r.r_regionkey < 2
				WHERE r.r_regionkey NOT IN (SELECT b.n_regionkey FROM nation b WHERE b.n_nationkey < 5)`},
		pgCase{name: "NotInSubqueryNullProbeKeyValues",
			sql: `SELECT a.n_nationkey AS k FROM nation a
				LEFT JOIN region r ON r.r_regionkey = a.n_regionkey AND r.r_regionkey < 2
				WHERE r.r_regionkey NOT IN (SELECT b.n_regionkey FROM nation b WHERE b.n_nationkey < 5)
				ORDER BY k`, ordered: true},
		// The IN twin is the control: a semi join IS the right two-valued
		// question, and a fix to NOT IN must not move it.
		pgCase{name: "InSubqueryNullProbeKey",
			sql: `SELECT COUNT(*) AS c FROM nation a
				LEFT JOIN region r ON r.r_regionkey = a.n_regionkey AND r.r_regionkey < 2
				WHERE r.r_regionkey IN (SELECT b.n_regionkey FROM nation b WHERE b.n_nationkey < 5)`},
		// A NULL in the LIST: the inner LEFT JOIN puts one there, and it
		// poisons every non-matching comparison, so nothing survives.
		pgCase{name: "NotInSubqueryNullInList",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_regionkey NOT IN
				(SELECT r.r_regionkey FROM nation b
				 LEFT JOIN region r ON r.r_regionkey = b.n_regionkey AND r.r_regionkey < 2)`},
		pgCase{name: "InSubqueryNullInList",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_regionkey IN
				(SELECT r.r_regionkey FROM nation b
				 LEFT JOIN region r ON r.r_regionkey = b.n_regionkey AND r.r_regionkey < 2)`},
		// The control that says the anti join still ANSWERS rather than
		// having been turned into a way of returning nothing.
		pgCase{name: "NotInSubqueryNullFreeList",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_regionkey NOT IN
				(SELECT b.n_regionkey FROM nation b WHERE b.n_nationkey < 5)`},
		// An EMPTY subquery is the boundary of the three-valued rule, and
		// getting it wrong is the shape a NULL-guard invites: `k NOT IN ()`
		// is TRUE for every row INCLUDING one whose key is NULL, because
		// there is no value for the comparison to be UNKNOWN about. Both
		// rules are guarded on the build having rows at all.
		pgCase{name: "NotInSubqueryEmptySet",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_regionkey NOT IN
				(SELECT b.n_regionkey FROM nation b WHERE b.n_nationkey > 999)`},
		pgCase{name: "InSubqueryEmptySet",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_regionkey IN
				(SELECT b.n_regionkey FROM nation b WHERE b.n_nationkey > 999)`},
		// The estimator's semi/anti SWAP (a build 3× the probe becomes a
		// RightAntiJoin, which marks build entries during the probe instead
		// of taking the semi/anti probe path) must not fire for a null-aware
		// anti join: the two rules live on the path it stops taking. region
		// (5 rows) against nation (25) crosses the ratio, so this is the
		// shape where the rule went missing on the single-process path while
		// the DAG kept it.
		pgCase{name: "NotInSubqueryNullInListSwapShape",
			sql: `SELECT COUNT(*) AS c FROM region a WHERE a.r_regionkey NOT IN
				(SELECT r.r_regionkey FROM nation b
				 LEFT JOIN region r ON r.r_regionkey = b.n_regionkey AND r.r_regionkey < 2)`},
		pgCase{name: "NotInSubqueryNullFreeListSwapShape",
			sql: `SELECT COUNT(*) AS c FROM region a WHERE a.r_regionkey NOT IN
				(SELECT b.n_regionkey FROM nation b WHERE b.n_nationkey < 5)`},
	)

	// --- IN-subqueries the semi-join rewrite DECLINES (#524) -------------
	//
	// A LIMIT/OFFSET (#482), an ungrouped aggregate item, and a computed item
	// (#516) each leave the IN a subquery PREDICATE rather than a semi join.
	// The stage DAG had no way to execute one and errored; the coordinator now
	// materializes the set and the predicate becomes a literal list. These
	// entries pin the ANSWER against PostgreSQL, which is what says the
	// materialized set means the same thing the subquery did — LIMIT included.
	out = append(out,
		// The VALUES of a bounded set, not just its count: the LIMIT decides
		// WHICH three, and a materialization that kept the wrong three counts
		// the same.
		pgCase{name: "InSubqueryBoundedByLimitValues",
			sql: `SELECT a.n_nationkey AS k FROM nation a WHERE a.n_nationkey IN
				(SELECT b.n_nationkey FROM nation b ORDER BY b.n_nationkey LIMIT 3)
				ORDER BY k`, ordered: true},
		// LIMIT 0 is a bound, not an absence (#481): the set is EMPTY, so IN
		// is false for every row and NOT IN is TRUE for every row — an empty
		// set has nothing to be UNKNOWN about, so even a NULL key survives.
		// (The IN half is InSubqueryLimitZero, above.)
		pgCase{name: "NotInSubqueryLimitZero",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_nationkey NOT IN
				(SELECT b.n_nationkey FROM nation b ORDER BY b.n_nationkey LIMIT 0)`},
		pgCase{name: "InSubqueryUngroupedAggregate",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_nationkey IN
				(SELECT MAX(b.n_nationkey) FROM nation b)`},
		pgCase{name: "InSubqueryComputedItem",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_nationkey IN
				(SELECT b.n_nationkey + 0 FROM nation b WHERE b.n_nationkey < 10)`},
		// A STRING set: the values ride the filter as TEXT, and a quoting slip
		// is a wrong answer with no error attached.
		pgCase{name: "InSubqueryBoundedStringSet",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_name IN
				(SELECT b.n_name FROM nation b ORDER BY b.n_name LIMIT 4)`},
		pgCase{name: "InSubqueryStringSetValues",
			sql: `SELECT a.n_name AS n FROM nation a WHERE a.n_name IN
				(SELECT b.n_name FROM nation b ORDER BY b.n_name LIMIT 4)
				ORDER BY n`, ordered: true},
	)

	// --- String functions -------------------------------------------------
	//
	// SQL string positions are 1-based, SUBSTRING clamps rather than erroring
	// on an out-of-range start, and TRIM/POSITION/REPLACE each have a defined
	// answer a client will hold the engine to.
	out = append(out,
		pgCase{name: "SubstrOneBased", sql: `SELECT SUBSTR('abcdef', 1, 3) AS a, SUBSTR('abcdef', 3) AS b,
			SUBSTR('abcdef', 0, 3) AS zero_start, SUBSTR('abcdef', 4, 100) AS past_end`},
		pgCase{name: "StringLengthFunctions", sql: `SELECT LENGTH('abc') AS l, CHAR_LENGTH('abc') AS cl,
			OCTET_LENGTH('abc') AS ol, LENGTH('') AS empty`},
		pgCase{name: "TrimFamily", sql: `SELECT TRIM('  pad  ') AS t, LTRIM('  pad  ') AS l, RTRIM('  pad  ') AS r,
			UPPER('MiXeD') AS u, LOWER('MiXeD') AS lo`},
		// POSITION(needle IN haystack) itself parses since #374; REPLACE in
		// the same statement was a separate, unrelated gap #374's own pin
		// never named — it only reached REPLACE once POSITION stopped
		// failing first. REPLACE was a reserved lexer keyword that never
		// reached the generic function-call dispatch (the same shape #371
		// fixed for EVERY); parsePrimary now special-cases it the same way,
		// gated on a following '(' so CREATE OR REPLACE is unaffected
		// (#382, fixed).
		pgCase{name: "PositionAndReplace", sql: `SELECT POSITION('cd' IN 'abcdef') AS p, POSITION('zz' IN 'abcdef') AS missing,
			REPLACE('abcabc', 'b', 'X') AS r`},
		pgCase{name: "ConcatOperator", sql: `SELECT 'a' || 'b' AS ab, 'a' || NULL AS a_null, NULL || 'b' AS null_b`},
		// LIKE: % and _ , and the anchoring TPC-H exercises only in one shape.
		pgCase{name: "LikePatterns", sql: `SELECT n_name, (n_name LIKE 'A%') AS pre, (n_name LIKE '%A') AS suf,
			(n_name LIKE '_RAZIL') AS underscore, (n_name LIKE '%AN%') AS mid, (n_name NOT LIKE 'A%') AS neg
			FROM nation ORDER BY n_name`},
		pgCase{name: "LikeWithNull", sql: `SELECT (NULL LIKE 'a%') AS null_left, ('a' LIKE NULL) AS null_right`},
	)

	// --- Aggregate and window semantics ------------------------------------
	out = append(out,
		// A GROUP BY over an expression, keyed and ordered by an ALIAS.
		pgCase{name: "AliasedGroupKeyOrderBy", sql: `SELECT o_orderpriority AS p, COUNT(*) AS c FROM orders GROUP BY o_orderpriority ORDER BY p`},
		pgCase{name: "GroupByOrdinal", sql: `SELECT o_orderstatus, COUNT(*) AS c FROM orders GROUP BY 1 ORDER BY 1`},
		pgCase{name: "HavingOnAggregate", sql: `SELECT o_orderstatus AS s, COUNT(*) AS c FROM orders
			GROUP BY o_orderstatus HAVING COUNT(*) > 100 ORDER BY s`},
		pgCase{name: "CountDistinct", sql: `SELECT COUNT(DISTINCT o_orderstatus) AS d, COUNT(DISTINCT o_custkey) AS dc,
			COUNT(*) AS n FROM orders`},
		// Sample vs population is a naming decision PostgreSQL settles: bare
		// STDDEV and VARIANCE are the SAMPLE forms.
		pgCase{name: "VarianceFamilyNaming", sql: `SELECT STDDEV(o_totalprice) AS s, STDDEV_SAMP(o_totalprice) AS ss,
			STDDEV_POP(o_totalprice) AS sp, VARIANCE(o_totalprice) AS v,
			VAR_SAMP(o_totalprice) AS vs, VAR_POP(o_totalprice) AS vp FROM orders`},
		// #371's shape: the accumulator was fine, its INPUT was not — no
		// boolean-valued node (comparison, AND/OR/NOT, IS, LIKE, BETWEEN,
		// IN) declared a type, so the pre-aggregate projection fell back to
		// Float64 and dropped every boolean write.
		pgCase{name: "BoolAggregates", sql: `SELECT BOOL_AND(n_regionkey >= 0) AS all_nonneg, BOOL_OR(n_regionkey > 3) AS any_big,
			BOOL_AND(n_regionkey > 3) AS all_big FROM nation`},
		// The default window frame is RANGE, so a running total over a TIED
		// ORDER BY key advances by peer group and not by row. Getting it wrong
		// is a plausible number, never an error.
		//
		// The ROWS control deliberately orders by n_nationkey, which is UNIQUE.
		// A ROWS frame over a TIED key has no cross-engine answer at all — the
		// frame advances one row at a time through an order SQL does not
		// determine — so comparing one would be comparing two arbitrary
		// choices. The RANGE half is exactly the part ties DO determine.
		pgCase{name: "WindowDefaultFrameIsRange", sql: `SELECT n_nationkey, n_regionkey,
			SUM(n_regionkey) OVER (ORDER BY n_regionkey) AS s_default,
			COUNT(*) OVER (ORDER BY n_regionkey) AS c_default,
			SUM(n_regionkey) OVER (ORDER BY n_regionkey RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s_range,
			SUM(n_regionkey) OVER (ORDER BY n_nationkey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s_rows_unique
			FROM nation ORDER BY n_nationkey`},
		pgCase{name: "WindowRankFamily", sql: `SELECT n_nationkey,
			ROW_NUMBER() OVER (ORDER BY n_nationkey) AS rn,
			RANK() OVER (ORDER BY n_regionkey) AS rk,
			DENSE_RANK() OVER (ORDER BY n_regionkey) AS drk,
			NTILE(4) OVER (ORDER BY n_nationkey) AS nt
			FROM nation ORDER BY n_nationkey`},
		pgCase{name: "WindowValueFunctions", sql: `SELECT n_nationkey, n_name,
			LAG(n_name) OVER (ORDER BY n_nationkey) AS lag_name,
			LEAD(n_name) OVER (ORDER BY n_nationkey) AS lead_name,
			FIRST_VALUE(n_name) OVER (ORDER BY n_nationkey) AS first_name,
			LAST_VALUE(n_name) OVER (ORDER BY n_nationkey
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_name
			FROM nation ORDER BY n_nationkey`},
		pgCase{name: "StringAggSeparator", sql: `SELECT LENGTH(STRING_AGG(o_orderstatus, '::')) AS n FROM orders`},
	)

	// --- Windowed MIN/MAX (#569) --------------------------------------------
	//
	// `MIN(c) OVER (…)` and `MIN(c) … GROUP BY g` are the same question asked
	// twice, and until #569 one of the two spellings FAILED the query for
	// twelve of Wadjet's twenty-two types: the window declared FLOAT64 for
	// anything outside a ten-type allow-list and then could not write the
	// value it chose into it.
	//
	// PostgreSQL is the reference for the four of those types it has an
	// aggregate for — `numeric` and the three that map onto `inet`. It has
	// none for `uuid`, `macaddr`, `bytea` or `boolean` (verified live on
	// postgres:17-alpine: each errors with "function min(...) does not
	// exist"), so those are Wadjet extensions in the sense ADR-0012 §5
	// already records for BOOL, and the type-matrix corpus gates them
	// instead.
	//
	// Every PARTITION BY here names a BARE column. That is not stylistic: a
	// window whose PARTITION BY is an EXPRESSION (`PARTITION BY d_key % 7`)
	// or a QUALIFIED reference (`PARTITION BY p.n_grp`) silently loses the
	// partitioning in wadjet and answers the whole input as one partition —
	// found by the first version of these entries, filed separately, and not
	// #569's subject. Writing the key as a bare column keeps these entries
	// about the value MIN/MAX chooses and the type it declares.
	out = append(out,
		// The ordering literals below are chosen against the fixture's own
		// text-order inversions (pgNetRows): the answers cannot come out
		// right under a lexical comparison of the rendered addresses.
		pgCase{name: "WindowMinMaxInetV4", sql: `SELECT n_key, n_grp,
			MIN(n_v4) OVER (PARTITION BY n_grp ORDER BY n_key
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS w_lo,
			MAX(n_v4) OVER (PARTITION BY n_grp ORDER BY n_key
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS w_hi
			FROM net_probe ORDER BY n_key`},
		pgCase{name: "WindowMinMaxInetV6", sql: `SELECT n_key, n_grp,
			MIN(n_v6) OVER (PARTITION BY n_grp ORDER BY n_key
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS w_lo,
			MAX(n_v6) OVER (PARTITION BY n_grp ORDER BY n_key
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS w_hi
			FROM net_probe ORDER BY n_key`},
		pgCase{name: "WindowMinMaxInetCidr", sql: `SELECT n_key, n_grp,
			MIN(n_cidr) OVER (PARTITION BY n_grp ORDER BY n_key
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS w_lo,
			MAX(n_cidr) OVER (PARTITION BY n_grp ORDER BY n_key
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS w_hi
			FROM net_probe ORDER BY n_key`},
		// The empty-PARTITION-BY form takes a different evaluator in Wadjet:
		// it streams the whole input as one partition and carries the running
		// extreme as a BOXED value across batches, so the comparison there is
		// the boxed one rather than the columnar one. Same answer required.
		pgCase{name: "WindowMinMaxInetOverEverything", sql: `SELECT n_key,
			MIN(n_v4) OVER () AS v4_lo, MAX(n_v4) OVER () AS v4_hi,
			MIN(n_v6) OVER () AS v6_lo, MAX(n_v6) OVER () AS v6_hi,
			MIN(n_cidr) OVER () AS c_lo, MAX(n_cidr) OVER () AS c_hi
			FROM net_probe ORDER BY n_key`},
		// The running frame, and its agreement with the grouped aggregate at
		// the partition's last row. RANGE is the default frame and the ORDER
		// BY key is unique, so every row's frame is [first .. this row].
		pgCase{name: "WindowMinMaxInetRunning", sql: `SELECT n_key, n_grp,
			MIN(n_v4) OVER (PARTITION BY n_grp ORDER BY n_key) AS run_lo,
			MAX(n_v4) OVER (PARTITION BY n_grp ORDER BY n_key) AS run_hi
			FROM net_probe ORDER BY n_key`},
		// A partition whose every value is NULL answers NULL, and a partition
		// with SOME nulls ignores them — PostgreSQL's rule for MIN/MAX, which
		// a window that wrote a zero instead would pass every count-based
		// check for.
		pgCase{name: "WindowMinMaxInetNullPartition", sql: `SELECT n_key, m,
			MIN(v) OVER (PARTITION BY m) AS lo, MAX(v) OVER (PARTITION BY m) AS hi
			FROM (SELECT n_key, CASE WHEN n_key % 11 = 0 THEN 0 ELSE 1 END AS m,
				CASE WHEN n_key % 11 = 0 THEN NULL ELSE n_v6 END AS v
				FROM net_probe) s ORDER BY n_key`},

		// DECIMAL. exactNumeric on every one: the answer IS the digits, and a
		// float64 rendering agrees about the first six of them whatever the
		// window did (#455's lesson).
		pgCase{name: "WindowMinMaxDecimalScale2", exactNumeric: true, sql: `SELECT d_key,
			MIN(d_2) OVER (PARTITION BY d_grp ORDER BY d_key
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS w_lo,
			MAX(d_2) OVER (PARTITION BY d_grp ORDER BY d_key
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS w_hi
			FROM dec_probe ORDER BY d_key`},
		pgCase{name: "WindowMinMaxDecimalScale4Running", exactNumeric: true, sql: `SELECT d_key,
			MIN(d_4) OVER (ORDER BY d_key) AS run_lo,
			MAX(d_4) OVER (ORDER BY d_key) AS run_hi
			FROM dec_probe ORDER BY d_key`},
		// The WIDE arm. Every value needs more than 64 bits, so a window that
		// answered through a float64 — which is what the FLOAT64 declaration
		// would have done had the write not been refused outright — loses
		// everything past the 16th digit while still looking like a number.
		pgCase{name: "WindowMinMaxDecimalWide", exactNumeric: true, sql: `SELECT d_key,
			MIN(d_wide) OVER (PARTITION BY d_grp
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS w_lo,
			MAX(d_wide) OVER (PARTITION BY d_grp
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS w_hi
			FROM dec_probe ORDER BY d_key`},
		pgCase{name: "WindowMinMaxDecimalOverEverything", exactNumeric: true, sql: `SELECT d_key,
			MIN(d_wide) OVER () AS lo, MAX(d_wide) OVER () AS hi
			FROM dec_probe ORDER BY d_key`},

		// Windowed SUM/AVG over a DECIMAL (#586, #475). MIN/MAX above COPY an
		// input value; these ACCUMULATE one, and until ADR-0024 they did it
		// in float64 and declared FLOAT64 — so `SUM(d) OVER (PARTITION BY g)`
		// answered 4.1266696257e+06 where `SUM(d) … GROUP BY g` answered
		// 4126669.6257 for the same rows. Both spellings are here so the
		// oracle sees them agree with PostgreSQL and with each other.
		//
		// exactNumeric on every SUM: PostgreSQL's sum(numeric) is exact and
		// so is wadjet's, so anything short of digit-for-digit equality is a
		// defect — and a float rendering agrees about the first six digits
		// whatever the accumulator did, which is exactly how #455's grouped
		// half shipped green.
		pgCase{name: "WindowSumDecimalScale2Partition", exactNumeric: true, sql: `SELECT d_key,
			SUM(d_2) OVER (PARTITION BY d_grp) AS w_sum FROM dec_probe ORDER BY d_key`},
		pgCase{name: "WindowSumDecimalScale4Running", exactNumeric: true, sql: `SELECT d_key,
			SUM(d_4) OVER (ORDER BY d_key) AS run_sum FROM dec_probe ORDER BY d_key`},
		// The WIDE arm: every value needs more than 64 bits, so a float64
		// accumulator loses everything past the 16th digit while still
		// looking like a number.
		pgCase{name: "WindowSumDecimalWide", exactNumeric: true, sql: `SELECT d_key,
			SUM(d_wide) OVER (PARTITION BY d_grp) AS w_sum,
			SUM(d_wide) OVER () AS all_sum FROM dec_probe ORDER BY d_key`},
		// A SLIDING frame, where the accumulator RETRACTS the row leaving the
		// frame. That subtraction has to be exact and checked; a float
		// running total additionally loses associativity, so the same frame
		// reached by adding and subtracting is a different number from the
		// one reached by summing its rows.
		pgCase{name: "WindowSumDecimalSlidingFrame", exactNumeric: true, sql: `SELECT d_key,
			SUM(d_wide) OVER (PARTITION BY d_grp ORDER BY d_key
				ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS slide_sum,
			SUM(d_2) OVER (ORDER BY d_key ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS slide2
			FROM dec_probe ORDER BY d_key`},
		// A window over a DECIMAL EXPRESSION rather than a bare column. The
		// argument is not a name on the batch, so it has to be MATERIALIZED
		// before the operator can read it — the same pre-projection a
		// computed PARTITION BY key gets. Until #672 nothing materialized it
		// and `SUM(d * 2) OVER ()` answered NULL in every row on both paths;
		// materializing it without carrying the DECIMAL declaration answers a
		// float, which is the half these entries pin. The GROUPED spelling
		// rides alongside because the two must agree — they are the same
		// question written twice (ADR-0024 item 2).
		pgCase{name: "WindowSumDecimalExpressionArgument", exactNumeric: true, sql: `SELECT d_key,
			SUM(d_4 * 2) OVER () AS all_sum,
			SUM(d_2 * 2) OVER (PARTITION BY d_grp) AS grp_sum
			FROM dec_probe ORDER BY d_key`},
		pgCase{name: "WindowSumDecimalExpressionMatchesGrouped", exactNumeric: true, sql: `SELECT d_grp,
			SUM(d_2 * 2) AS grouped_sum FROM dec_probe GROUP BY d_grp ORDER BY d_grp`},
		pgCase{name: "WindowMinMaxDecimalExpressionArgument", exactNumeric: true, sql: `SELECT d_key,
			MIN(d_4 * 2) OVER () AS lo, MAX(d_4 * 2) OVER () AS hi
			FROM dec_probe ORDER BY d_key`},
		// The same argument one level down, where the expression names a
		// DERIVED TABLE's alias: on the stage DAG the Project below the
		// window emits no stage, so the materialized key's expression TEXT
		// named a column the window's input does not carry and the operator
		// wrote NULL in every row (#656 follow-up, A3).
		pgCase{name: "WindowSumDecimalExpressionOverAnAlias", exactNumeric: true, sql: `SELECT d_key,
			SUM(v * 2) OVER () AS all_sum FROM (
				SELECT d_key, d_4 AS v FROM dec_probe) s ORDER BY d_key`},

		// --- the window's OUTPUT NAME (#694) ---------------------------------
		//
		// exec.Window APPENDS its result to the input batch and a bare window
		// used to write under the user's ALIAS, so an alias spelling an input
		// column's name gave the projection two columns of that name and it
		// took the first — the INPUT column, silently, on both paths. The
		// window now writes a `__win_N` slot of its own and the projection
		// publishes that under the requested name.
		//
		// Every entry below is a query PostgreSQL answers with the WINDOW's
		// value; wadjet used to answer the shadowed column's.
		pgCase{name: "WindowAliasShadowsAnotherColumn", exactNumeric: true, sql: `SELECT d_key,
			SUM(d_2) OVER () AS d_grp FROM dec_probe ORDER BY d_key`},
		pgCase{name: "WindowAliasShadowsItsOwnArgument", exactNumeric: true, sql: `SELECT d_key,
			SUM(d_2) OVER () AS d_2 FROM dec_probe ORDER BY d_key`},
		pgCase{name: "WindowAliasShadowsThePartitionKey", exactNumeric: true, sql: `SELECT d_key,
			SUM(d_2) OVER (PARTITION BY d_grp) AS d_grp FROM dec_probe ORDER BY d_key`},
		// The shadowed column BESIDE the window that shadows it: both have to
		// survive, which a "let the window replace the input column" repair
		// would have broken in the other direction.
		pgCase{name: "WindowAliasShadowsAColumnSelectedBeside", exactNumeric: true, sql: `SELECT d_key,
			SUM(d_2) OVER () AS d_grp, d_grp AS orig FROM dec_probe ORDER BY d_key`},
		// A ranking function takes no argument at all, so this is the output
		// name and nothing else.
		pgCase{name: "WindowRowNumberAliasShadowsAColumn", sql: `SELECT d_key,
			ROW_NUMBER() OVER (ORDER BY d_key) AS d_grp FROM dec_probe ORDER BY d_key`},
		// Consumed one level up, where the outer reference has to find the
		// window's value and not the base column that reached the same name.
		pgCase{name: "WindowAliasShadowsAColumnThroughADerivedTable", exactNumeric: true, sql: `SELECT d_key,
			d_grp FROM (SELECT d_key, SUM(d_2) OVER () AS d_grp FROM dec_probe) s
			ORDER BY d_key`},
		// ORDER BY the shadowing alias: the sort used to key on the base
		// column, so the ROWS came back in the wrong sequence as well.
		pgCase{name: "WindowAliasShadowsAColumnOrderedByIt", exactNumeric: true, sql: `SELECT d_key,
			SUM(d_2) OVER () AS d_grp FROM dec_probe ORDER BY d_grp, d_key`},
		// An UNALIASED window. PostgreSQL names the column after the FUNCTION
		// — `sum`, `row_number`, `min` — and wadjet named it after the window
		// call's TEXT, `sum(d_2) OVER (...)`, which no client recognises. The
		// value arm sees the name because it compares by column name, and it
		// is the arm that can: nothing else in the corpus has an unaliased
		// window at all.
		pgCase{name: "WindowUnaliasedSum", exactNumeric: true, sql: `SELECT d_key,
			SUM(d_2) OVER () FROM dec_probe ORDER BY d_key`},
		pgCase{name: "WindowUnaliasedRowNumber", sql: `SELECT d_key,
			ROW_NUMBER() OVER (ORDER BY d_key) FROM dec_probe ORDER BY d_key`},
		pgCase{name: "WindowUnaliasedMinMax", exactNumeric: true, sql: `SELECT d_key,
			MIN(d_2) OVER (), MAX(d_2) OVER () FROM dec_probe ORDER BY d_key`},
		// ORDER BY a POSITION over an unaliased window: the positional
		// resolver rewrites the ordinal to the select item's NAME, so it has
		// to make the same choice the projection does — under the text
		// spelling it produced a key with parentheses in it, which the sort
		// could not resolve and the DAG failed at dispatch for.
		pgCase{name: "WindowUnaliasedOrderedByPosition", exactNumeric: true, sql: `SELECT
			SUM(d_2) OVER () FROM dec_probe ORDER BY 1, 1`},

		// --- an aggregate's INPUT expression over a derived column (#702) ----
		//
		// TPC-H Q08's shape. The derived table's Project emits no stage on the
		// DAG, so the aggregate's argument TEXT named a column the batch does
		// not carry: `volume` read NULL on every row and the SUM came back as
		// the total of its ELSE branch.
		//
		// This oracle reaches only the SINGLE-PROCESS engine, which runs that
		// Project as a real operator and was already right, so these entries
		// do NOT fail on the unfixed tree — verified by reverting the fix and
		// re-running. They are here for what they DO give: PostgreSQL's own
		// answer for every shape, which is the number
		// coordinator.TestAggregateOverADerivedColumnTwoPath asserts on both
		// paths, and a ratchet on the half of the pair that was right, since
		// a repair aimed at the DAG could as easily have moved this one.
		pgCase{name: "AggregateCaseOverAComputedDerivedColumn", exactNumeric: true, sql: `SELECT
			SUM(CASE WHEN d_grp = 1 THEN volume ELSE 0 END) AS v FROM (
				SELECT d_grp, d_2 * d_4 AS volume FROM dec_probe) s`},
		pgCase{name: "AggregateCaseOverARenamedDerivedColumn", exactNumeric: true, sql: `SELECT
			SUM(CASE WHEN d_grp = 1 THEN v ELSE 0 END) AS v FROM (
				SELECT d_grp, d_4 AS v FROM dec_probe) s`},
		// The SHADOWING arm, and the one that answered a plausible different
		// number rather than a zero: the derived alias `d_2` names d_4, and
		// the base table has a column called `d_2`.
		pgCase{name: "AggregateCaseOverAShadowingDerivedAlias", exactNumeric: true, sql: `SELECT
			SUM(CASE WHEN d_grp = 1 THEN d_2 ELSE 0 END) AS v FROM (
				SELECT d_grp, d_4 AS d_2 FROM dec_probe) s`},
		pgCase{name: "AggregateCastOverADerivedColumn", sql: `SELECT
			SUM(CAST(v AS BIGINT)) AS v FROM (SELECT d_2 AS v FROM dec_probe) s`},
		pgCase{name: "AggregateCoalesceOverADerivedColumn", exactNumeric: true, sql: `SELECT
			SUM(COALESCE(v, 0)) AS v FROM (SELECT d_4 AS v FROM dec_probe) s`},
		pgCase{name: "AggregateCaseOverADerivedColumnGrouped", exactNumeric: true, sql: `SELECT d_grp,
			SUM(CASE WHEN d_grp = 1 THEN v ELSE 0 END) AS v FROM (
				SELECT d_grp, d_4 AS v FROM dec_probe) s GROUP BY d_grp ORDER BY d_grp`},
		// Two derived tables, so the walk has to resolve level by level.
		pgCase{name: "AggregateCaseOverTwoDerivedTables", exactNumeric: true, sql: `SELECT
			SUM(CASE WHEN d_grp = 1 THEN w ELSE 0 END) AS v FROM (
				SELECT d_grp, v AS w FROM (
					SELECT d_grp, d_4 AS v FROM dec_probe) s1) s2`},
		// An EMPTY frame is NULL, not 0 — the distinction a running total
		// that starts at zero cannot make on its own.
		pgCase{name: "WindowSumDecimalEmptyFrame", exactNumeric: true, sql: `SELECT d_key,
			SUM(d_2) OVER (PARTITION BY d_grp ORDER BY d_key
				ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS back_sum
			FROM dec_probe ORDER BY d_key`},
		// NULLs inside a partition. The partition key and the NULL predicate
		// are deliberately INDEPENDENT (%3 against %11), so most partitions
		// are MIXED — some rows NULL, some not. An earlier draft derived both
		// from `d_key % 11 = 0`, which made every partition either all-NULL
		// or all-present and left the case that actually matters untested:
		// PostgreSQL excludes a NULL from the sum AND from AVG's denominator,
		// so an AVG dividing by the frame's WIDTH is wrong on exactly the
		// mixed partitions and right on the pure ones.
		//
		// The all-NULL partition is still covered — `m = 3` collects
		// d_key % 3 = 0 rows, and the second entry below pins a partition
		// whose every row is NULL against PostgreSQL's NULL, not 0.
		pgCase{name: "WindowSumAvgDecimalNullPartition", exactNumeric: true, sql: `SELECT d_key, m,
			SUM(v) OVER (PARTITION BY m) AS s, COUNT(v) OVER (PARTITION BY m) AS c FROM (
				SELECT d_key, d_key % 3 AS m,
					CASE WHEN d_key % 11 = 0 THEN NULL ELSE d_4 END AS v
				FROM dec_probe) s ORDER BY d_key`, knownBug: windowCountNullPin, issue: "#670"},
		pgCase{name: "WindowSumDecimalAllNullPartition", exactNumeric: true, sql: `SELECT d_key, m,
			SUM(v) OVER (PARTITION BY m) AS s FROM (
				SELECT d_key, CASE WHEN d_key % 11 = 0 THEN 0 ELSE 1 END AS m,
					CASE WHEN d_key % 11 = 0 THEN NULL ELSE d_4 END AS v
				FROM dec_probe) s ORDER BY d_key`},
		// A frame of exactly ONE row, which is where the accumulator's
		// SLIDE — not any single frame — used to decide the answer: it added
		// the arriving row before subtracting the departing one, so it
		// transiently held two frames' worth. Over a DECIMAL that transient
		// could leave the 128-bit carrier and refuse a query PostgreSQL
		// answers; here it is gated on the values, which is what a divergence
		// would look like on data that does not overflow.
		pgCase{name: "WindowSumDecimalCurrentRowOnlyFrame", exactNumeric: true, sql: `SELECT d_key,
			SUM(d_wide) OVER (PARTITION BY d_grp ORDER BY d_key
				ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS one_row,
			SUM(d_2) OVER (ORDER BY d_key ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS one2
			FROM dec_probe ORDER BY d_key`},
		// AVG keeps the FLOAT comparison, and for exactly the reason the
		// grouped WideDecimalAvg does: both engines divide exactly, but
		// PostgreSQL picks a result scale giving at least 16 significant
		// digits while wadjet widens the input scale by a fixed 4
		// (batch.AvgScaleIncrement, ADR-0012 item 9). The two agree to
		// min(both scales) and differ in how many digits past that they keep,
		// which no exact comparison can express. What IS gated here is that
		// the window's AVG agrees with PostgreSQL's numeric division at all —
		// a float64 accumulator diverges well before the 16th digit.
		pgCase{name: "WindowAvgDecimalScale2Partition", sql: `SELECT d_key,
			AVG(d_2) OVER (PARTITION BY d_grp) AS w_avg FROM dec_probe ORDER BY d_key`},
		pgCase{name: "WindowAvgDecimalWideRunning", sql: `SELECT d_key,
			AVG(d_wide) OVER (ORDER BY d_key) AS run_avg FROM dec_probe ORDER BY d_key`},
		pgCase{name: "WindowAvgDecimalSlidingFrame", sql: `SELECT d_key,
			AVG(d_4) OVER (PARTITION BY d_grp ORDER BY d_key
				ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS slide_avg
			FROM dec_probe ORDER BY d_key`},
		// Mixed partitions again (see WindowSumAvgDecimalNullPartition), and
		// this is the entry the AVG denominator turns on: dividing by the
		// frame's width instead of its non-NULL count moves every average in
		// a partition that holds one NULL.
		pgCase{name: "WindowAvgDecimalNullPartition", sql: `SELECT d_key, m,
			AVG(v) OVER (PARTITION BY m) AS a FROM (
				SELECT d_key, d_key % 3 AS m,
					CASE WHEN d_key % 11 = 0 THEN NULL ELSE d_4 END AS v
				FROM dec_probe) s ORDER BY d_key`},
		pgCase{name: "WindowAvgDecimalAllNullPartition", sql: `SELECT d_key, m,
			AVG(v) OVER (PARTITION BY m) AS a FROM (
				SELECT d_key, CASE WHEN d_key % 11 = 0 THEN 0 ELSE 1 END AS m,
					CASE WHEN d_key % 11 = 0 THEN NULL ELSE d_4 END AS v
				FROM dec_probe) s ORDER BY d_key`},
		// G2: the AVG-denominator and all-NULL-frame rules moved on the
		// FLOAT/INT accumulator too — they live in the same slide — so they
		// need their own entries over a NON-DECIMAL column. Nothing else in
		// this corpus asks PostgreSQL what a windowed AVG over a float or an
		// integer with NULLs in the frame means, and the DECIMAL entries
		// above cannot stand in: they exercise a different accumulator.
		pgCase{name: "WindowAvgFloatWithNullsInFrame", sql: `SELECT o_orderkey, m,
			AVG(v) OVER (PARTITION BY m) AS a,
			SUM(v) OVER (PARTITION BY m) AS s,
			AVG(v) OVER (PARTITION BY m ORDER BY o_orderkey
				ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS slide_a
			FROM (SELECT o_orderkey, o_custkey % 3 AS m,
				CASE WHEN o_orderkey % 7 = 0 THEN NULL ELSE o_totalprice END AS v
				FROM orders WHERE o_orderkey < 4000) s ORDER BY o_orderkey`},
		pgCase{name: "WindowAvgIntWithNullsInFrame", sql: `SELECT o_orderkey, m,
			AVG(v) OVER (PARTITION BY m) AS a,
			SUM(v) OVER (PARTITION BY m ORDER BY o_orderkey
				ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS one_row
			FROM (SELECT o_orderkey, o_orderkey % 4 AS m,
				CASE WHEN o_orderkey % 5 = 0 THEN NULL ELSE o_custkey END AS v
				FROM orders WHERE o_orderkey < 4000) s ORDER BY o_orderkey`},
		// A partition of a non-DECIMAL column whose every row is NULL: SQL
		// says NULL for both SUM and AVG, and a running total that starts at
		// zero answers 0 unless something stops it.
		pgCase{name: "WindowAvgFloatAllNullPartition", sql: `SELECT o_orderkey, m,
			AVG(v) OVER (PARTITION BY m) AS a, SUM(v) OVER (PARTITION BY m) AS s
			FROM (SELECT o_orderkey, CASE WHEN o_orderkey % 7 = 0 THEN 0 ELSE 1 END AS m,
				CASE WHEN o_orderkey % 7 = 0 THEN NULL ELSE o_totalprice END AS v
				FROM orders WHERE o_orderkey < 4000) s ORDER BY o_orderkey`},
		// The CONTROL: the windowed and the GROUPED spelling of one question,
		// in one query, over the same rows. They are the same number, so a
		// change that moved only one of them shows here even if both look
		// plausible on their own.
		pgCase{name: "WindowSumMatchesGroupedSum", exactNumeric: true, sql: `SELECT g, w, s
			FROM (SELECT DISTINCT d_grp AS g, SUM(d_4) OVER (PARTITION BY d_grp) AS w
				FROM dec_probe) a
			JOIN (SELECT d_grp AS g2, SUM(d_4) AS s FROM dec_probe GROUP BY d_grp) b
				ON a.g = b.g2 ORDER BY g`},

		// The types that ALREADY worked, so the widening did not disturb
		// them: STRING, DATE (stored as text on both sides — see
		// createPostgresSchema), TIMESTAMP-free INT and FLOAT.
		pgCase{name: "WindowMinMaxStringAndDate", sql: `SELECT o_orderkey,
			MIN(o_orderstatus) OVER (PARTITION BY o_custkey
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s_lo,
			MAX(o_orderdate) OVER (PARTITION BY o_custkey
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS d_hi
			FROM orders WHERE o_orderkey < 4000 ORDER BY o_orderkey`},
		pgCase{name: "WindowMinMaxNumeric", sql: `SELECT o_orderkey,
			MIN(o_totalprice) OVER (PARTITION BY o_custkey
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS f_lo,
			MAX(o_custkey) OVER (PARTITION BY o_orderstatus
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS i_hi
			FROM orders WHERE o_orderkey < 4000 ORDER BY o_orderkey`},
	)

	// --- Joins and outer-join NULL padding ----------------------------------
	out = append(out,
		pgCase{name: "LeftJoinMissIsNull", sql: `SELECT n.n_name, r.r_name FROM nation n
			LEFT JOIN region r ON n.n_name = r.r_name ORDER BY n.n_name`},
		pgCase{name: "LeftJoinMissCount", sql: `SELECT COUNT(*) AS rows_out, COUNT(r.r_name) AS matched
			FROM nation n LEFT JOIN region r ON n.n_name = r.r_name`},
		pgCase{name: "OuterWhereAntiJoinIdiom", sql: `SELECT n.n_name FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey AND r.r_regionkey < 3
			WHERE r.r_regionkey IS NULL ORDER BY n.n_name`},
		pgCase{name: "FullJoinUnmatchedBothSides", sql: `SELECT n.n_name, r.r_name FROM nation n
			FULL OUTER JOIN region r ON n.n_name = r.r_name ORDER BY n.n_name, r.r_name`},
		pgCase{name: "RightJoinUnmatchedValues", sql: `SELECT n.n_name, r.r_name FROM region r
			RIGHT JOIN nation n ON r.r_regionkey = n.n_nationkey ORDER BY n.n_name`},
		pgCase{name: "SelfJoinInequalityConjunct", sql: `SELECT COUNT(*) AS c FROM supplier a
			JOIN supplier b ON a.s_nationkey = b.s_nationkey AND a.s_suppkey < b.s_suppkey`},
	)

	// --- Type resolution ----------------------------------------------------
	//
	// The polymorphic family, over the column types whose declaration is the
	// only thing that can decide the output type.
	out = append(out,
		pgCase{name: "PolymorphicOverStringColumns", sql: `SELECT n_nationkey,
			NULLIF(n_name, 'ALGERIA') AS nullif_str,
			COALESCE(n_name, n_comment) AS coalesce_two,
			GREATEST(n_name, n_comment) AS greatest_str,
			LEAST(n_name, n_comment) AS least_str
			FROM nation ORDER BY n_nationkey`},
		pgCase{name: "PolymorphicOverNumericColumns", sql: `SELECT n_nationkey,
			NULLIF(n_nationkey, 1) AS nullif_int,
			COALESCE(n_regionkey, 0) AS coalesce_int,
			GREATEST(n_nationkey, n_regionkey) AS greatest_int,
			LEAST(n_nationkey, n_regionkey) AS least_int
			FROM nation ORDER BY n_nationkey`},
		// The CHOICE family over a numeric column, asked TWICE — once of a
		// column that is FLOAT64 in both fixtures and once of one the
		// DECIMAL variant retypes. Moving the original from ps_supplycost to
		// l_quantity would have kept this entry green under TPCH_DECIMAL=1
		// at the cost of leaving the decimal shape — which is exactly what
		// #695 is — with no corpus entry at all. Both spellings stay, and
		// the decimal one is a pinned ratchet that flips when #695 lands.
		pgCase{name: "PolymorphicOverFloatColumns", sql: `SELECT l_orderkey, l_linenumber,
			COALESCE(l_quantity, 0) AS coalesce_float,
			GREATEST(l_quantity, l_linenumber) AS greatest_mixed,
			LEAST(l_quantity, l_linenumber) AS least_mixed
			FROM lineitem WHERE l_orderkey <= 20 ORDER BY l_orderkey, l_linenumber`},
		polymorphicOverDecimalCase(),
		pgCase{name: "CaseWhenTypeResolution", sql: `SELECT n_nationkey,
			CASE WHEN n_regionkey = 0 THEN 'zero' WHEN n_regionkey = 1 THEN 'one' ELSE 'many' END AS s,
			CASE WHEN n_regionkey = 0 THEN 1 ELSE 2 END AS i,
			CASE WHEN n_regionkey = 0 THEN NULL ELSE n_name END AS nullable
			FROM nation ORDER BY n_nationkey`},
		// Casts a client writes constantly. The `double precision` spelling is
		// covered separately (RoundHalfDouble) rather than folded in here.
		pgCase{name: "CastFamily", sql: `SELECT CAST('42' AS integer) AS i, CAST(42 AS text) AS s,
			CAST('1996-01-10' AS date) AS d`},
		// A cast from a fractional number to an integer. PostgreSQL ROUNDS
		// (half away from zero); most engines truncate, and Wadjet did until
		// #373. Kept as its own entry so the casts above stay gated.
		pgCase{name: "CastFloatToInteger", sql: `SELECT CAST(4.7 AS integer) AS up, CAST(4.2 AS integer) AS down,
			CAST(-4.7 AS integer) AS neg`},
		pgCase{name: "DateArithmetic", sql: `SELECT DATE '1996-01-10' - DATE '1996-01-01' AS gap,
			CAST('1996-01-10' AS date) - 1 AS prev, CAST('1996-01-10' AS date) + 5 AS nxt`},
		pgCase{name: "ExtractFromDate", sql: `SELECT EXTRACT(YEAR FROM DATE '1996-03-15') AS y,
			EXTRACT(MONTH FROM DATE '1996-03-15') AS m, EXTRACT(DAY FROM DATE '1996-03-15') AS d`},
		// #451: a DATE literal's day count used to be computed via
		// t.Sub(epoch), a time.Duration that saturates at ±math.MaxInt64 ns
		// (~292 years) instead of reporting an overflow, so every date
		// outside roughly 1677-09-22..2262-04-11 silently answered the
		// window's edge. PostgreSQL's own `date` range reaches 4713 BC to
		// 5874897 AD, comfortably past every literal here, so it is the
		// authority these are checked against — not just Go arithmetic
		// re-deriving its own answer.
		pgCase{name: "DateFarFromEpoch", sql: `SELECT
			DATE '9999-12-31' - DATE '1970-01-01' AS far_future,
			DATE '0001-01-01' - DATE '1970-01-01' AS far_past,
			DATE '2262-04-12' - DATE '1970-01-01' AS just_past_old_clamp_upper,
			DATE '1677-09-21' - DATE '1970-01-01' AS just_past_old_clamp_lower,
			DATE '9999-12-31' = DATE '9999-12-31' AS far_future_self_equal,
			DATE '9999-12-31' > DATE '2262-04-11' AS far_future_greater_than_old_clamp_edge`},
	)

	// --- DISTINCT, and where its dedup key comes from -------------------
	//
	// #466 is the family: a DISTINCT is executed by being lowered to a GROUP
	// BY over the projection below it, and every shape where that lowering
	// declines or picks the wrong keys is a wrong ANSWER, not an error. The
	// answers are what PostgreSQL is here to settle; the two-path invariance
	// suite then carries the same statements to the stage DAG.
	//
	// A star DISTINCT is the sharp case: `*` names no columns, so the dedup
	// key has to be reconstructed from the relation's schema. Reconstructing
	// it from the PRUNED set instead gave 14979 for the lineitem entry — the
	// number of distinct l_orderkeys — which is as plausible a number as the
	// right one (#479).
	out = append(out,
		pgCase{name: "DerivedDistinctUnderCount",
			sql: `SELECT COUNT(*) AS c FROM (SELECT DISTINCT n_regionkey FROM nation) u`},
		pgCase{name: "DerivedDistinctMultiColumnUnderCount",
			sql: `SELECT COUNT(*) AS c FROM (SELECT DISTINCT o_orderstatus, o_orderpriority FROM orders) u`},
		pgCase{name: "DerivedStarDistinctUnderCount",
			sql: `SELECT COUNT(*) AS c FROM (SELECT DISTINCT * FROM supplier) u`},
		pgCase{name: "DerivedQualifiedStarDistinctUnderCount",
			sql: `SELECT COUNT(*) AS c FROM (SELECT DISTINCT s.* FROM supplier s) u`},
		pgCase{name: "DerivedStarDistinctWideTableUnderCount",
			sql: `SELECT COUNT(*) AS c FROM (SELECT DISTINCT * FROM lineitem) u`},
		pgCase{name: "DerivedStarDistinctOverJoinUnderCount",
			sql: `SELECT COUNT(*) AS c FROM (SELECT DISTINCT * FROM nation JOIN region ON 1=1) u`},
		pgCase{name: "DerivedStarDistinctBehindWhereUnderCount",
			sql: `SELECT COUNT(*) AS c FROM (SELECT DISTINCT * FROM nation WHERE n_regionkey = 1) u`},
		// The group-key test is an AST question. A text pre-check for the
		// word "select" used to run first and fired on a string LITERAL, so
		// the rewrite declined and the query was refused outright.
		pgCase{name: "DerivedDistinctStringLiteralNamingSelect",
			sql: `SELECT COUNT(*) AS c FROM (SELECT DISTINCT n_name, 'x select y' AS lit FROM nation) u`},
		// An aggregate projection has no group key, so the lowering cannot
		// happen at all — the distributed planner refuses these and the
		// coordinator routes them to its single-process pipeline. The
		// semantics are still PostgreSQL's.
		pgCase{name: "DerivedDistinctOverAggregateProjection",
			sql: `SELECT COUNT(*) AS c FROM
				(SELECT DISTINCT o_orderstatus, SUM(o_totalprice) AS s FROM orders GROUP BY o_orderstatus) u`},
		pgCase{name: "GroupedOverDerivedDistinctAggregateProjection",
			sql: `SELECT k, COUNT(*) AS c FROM
				(SELECT DISTINCT n_regionkey AS k, COUNT(*) AS n FROM nation GROUP BY n_regionkey) u
				GROUP BY k ORDER BY k`},
		// #467's shape: a QUALIFIED group key naming a derived table's
		// alias. Correct on the engine this arm exercises; the stage DAG
		// could not resolve it, which the two-path suite gates.
		pgCase{name: "GroupByQualifiedAliasOverDerivedDistinct",
			sql: `SELECT k, COUNT(*) AS c FROM
				(SELECT DISTINCT n_regionkey AS k, n_name FROM nation) u
				GROUP BY u.k ORDER BY k`},
	)

	// --- #467 / #468: a derived table's SELECT-list alias ---------------------
	//
	// PostgreSQL is the authority for two rules the DAG got wrong. First,
	// a reference qualified by the derived table's own alias (`x.k`, `u.k`,
	// `y.j`) names that table's OUTPUT column — the alias, not anything of
	// the underlying relation. Second, an ORDER BY term inside the derived
	// table binds to the SELECT-list alias even when the alias SHADOWS a
	// base column of the same relation, so `SELECT s_acctbal AS s_suppkey
	// ... ORDER BY s_suppkey DESC` orders by ACCTBAL. Both were verified
	// live on postgres:17-alpine before the fix; these entries are what
	// keeps them verified.
	out = append(out,
		pgCase{name: "DerivedAliasInDerivedSortKey",
			sql: `SELECT k FROM (SELECT s1.s_suppkey AS k, s1.s_name FROM supplier s1
				ORDER BY s1.s_name, s1.s_suppkey DESC) x`},
		pgCase{name: "DerivedAliasInDerivedSortKeyBothTerms",
			sql: `SELECT k, nm FROM (SELECT s_suppkey AS k, s_name AS nm FROM supplier s1
				ORDER BY nm, s1.s_suppkey DESC) x`},
		pgCase{name: "DerivedAliasSortKeyUnderLimit",
			sql: `SELECT SUM(k) AS c FROM (SELECT s_suppkey AS k FROM supplier s1
				ORDER BY k DESC LIMIT 7) t`},
		pgCase{name: "DerivedAliasJoinKeyQualified",
			sql: `SELECT x.k FROM (SELECT s_suppkey AS k FROM supplier s1
				ORDER BY s1.s_suppkey DESC) x JOIN nation ON x.k = n_nationkey ORDER BY x.k`},
		pgCase{name: "DerivedAliasJoinKeyBare",
			sql: `SELECT x.k FROM (SELECT s_suppkey AS k FROM supplier s1) x
				JOIN nation ON k = n_nationkey ORDER BY x.k`},
		pgCase{name: "DerivedAliasGroupKeyQualified",
			sql: `SELECT k, COUNT(*) AS c FROM (SELECT n_regionkey AS k, n_name FROM nation) u
				GROUP BY u.k ORDER BY k`},
		pgCase{name: "DerivedAliasShufflePartitionKey",
			sql: `SELECT COUNT(*) AS c FROM (SELECT DISTINCT s_nationkey AS a FROM supplier) x
				JOIN (SELECT DISTINCT n_nationkey AS b FROM nation) y ON x.a = y.b`},
		// #468. The outer SELECT deliberately omits the shadowing column:
		// carrying it out is what made both engines agree, so the repro
		// needs it gone and the control keeps it.
		pgCase{name: "DerivedAliasShadowsBaseColumnInSort",
			sql: `SELECT real_key FROM (SELECT s_acctbal AS s_suppkey, s_suppkey AS real_key
				FROM supplier ORDER BY s_suppkey DESC) x`},
		pgCase{name: "DerivedAliasShadowsBaseColumnUnderLimit",
			sql: `SELECT real_key FROM (SELECT s_acctbal AS s_suppkey, s_suppkey AS real_key
				FROM supplier ORDER BY s_suppkey DESC LIMIT 1) x`},
		pgCase{name: "DerivedAliasShadowCarriedOut",
			sql: `SELECT s_suppkey, real_key FROM (SELECT s_acctbal AS s_suppkey, s_suppkey AS real_key
				FROM supplier ORDER BY s_suppkey DESC) x`},
		// #327's root-level family, re-asserted: the same shadowing rule one
		// level UP, which this fix must not disturb.
		pgCase{name: "RootAliasShadowsBaseColumnInSort",
			sql: `SELECT s_acctbal AS s_suppkey, s_name FROM supplier ORDER BY s_suppkey DESC`},
		pgCase{name: "RootAliasInSortKey",
			sql: `SELECT s_suppkey AS k FROM supplier ORDER BY k DESC`},
		pgCase{name: "RootAliasShadowsAnotherItemsSource",
			sql: `SELECT n_name AS n_comment, n_comment AS c FROM nation ORDER BY n_name`},
		// Two-level derived nesting: the rename chains and the outer
		// reference is qualified.
		pgCase{name: "DerivedAliasChainedSortKey",
			sql: `SELECT j FROM (SELECT k AS j FROM (SELECT s_suppkey AS k, s_name FROM supplier) x
				ORDER BY k DESC) y`},
		pgCase{name: "DerivedAliasChainedJoinKey",
			sql: `SELECT COUNT(*) AS c FROM
				(SELECT k AS j FROM (SELECT s_nationkey AS k FROM supplier) x) y
				JOIN nation ON y.j = n_nationkey`},
		pgCase{name: "DerivedAliasChainedGroupKey",
			sql: `SELECT y.j, COUNT(*) AS c FROM
				(SELECT k AS j FROM (SELECT s_nationkey AS k FROM supplier) x) y
				GROUP BY y.j ORDER BY y.j`},
	)

	// --- #488: a QUALIFIED ORDER BY term names the INPUT column --------------
	//
	// PostgreSQL matches an ORDER BY term against the SELECT list's output
	// names only when the term is a BARE identifier. `x.col` is resolved in
	// the FROM scope like any other expression, so it names the input column
	// even when a select alias shadows it — verified live, where these two
	// statements come back in OPPOSITE orders. Wadjet answered both the same
	// way (the alias's), on both arms and silently.
	out = append(out,
		pgCase{name: "QualifiedSortTermOverShadowingAlias", ordered: true,
			sql: `SELECT s.s_acctbal AS s_suppkey, s.s_name FROM supplier s
				ORDER BY s.s_suppkey DESC`},
		pgCase{name: "QualifiedSortTermByTableName", ordered: true,
			sql: `SELECT s_acctbal AS s_suppkey, s_name FROM supplier
				ORDER BY supplier.s_suppkey DESC`},
		// The BARE spelling of the same query, which binds the alias. Both
		// entries have to hold at once — that is the whole rule.
		pgCase{name: "BareSortTermOverShadowingAlias", ordered: true,
			sql: `SELECT s_acctbal AS s_suppkey, s_name FROM supplier
				ORDER BY s_suppkey DESC`},
		// Over a self-join the qualifier is the only thing that says which arm
		// the term means; the bare fallback matched the other one.
		pgCase{name: "QualifiedSortTermOverSelfJoin", ordered: true,
			sql: `SELECT n2.n_name, n1.n_regionkey FROM nation n1
				JOIN nation n2 ON n1.n_regionkey = n2.n_nationkey
				ORDER BY n1.n_name`},
	)

	// --- #489: a self-join INSIDE a derived table ---------------------------
	//
	// Both arms answer to the same bare column name, so a SELECT-list alias
	// over one of them can only be resolved through the spelling that keeps
	// the qualifier. The derived table's own alias used to be written OVER the
	// inner ones, after which nothing could tell the arms apart: PostgreSQL 17
	// answers 5 groups here and wadjet answered 25, on both arms.
	out = append(out,
		pgCase{name: "DerivedSelfJoinGroupByQualifiedAlias", ordered: true,
			sql: `SELECT u.b, COUNT(*) AS c FROM
				(SELECT n1.n_name AS a, n2.n_name AS b FROM nation n1
					JOIN nation n2 ON n1.n_regionkey = n2.n_nationkey) u
				GROUP BY u.b ORDER BY u.b`},
		pgCase{name: "DerivedSelfJoinGroupByBareAlias", ordered: true,
			sql: `SELECT b, COUNT(*) AS c FROM
				(SELECT n1.n_name AS a, n2.n_name AS b FROM nation n1
					JOIN nation n2 ON n1.n_regionkey = n2.n_nationkey) u
				GROUP BY b ORDER BY b`},
		// The other arm's alias, whose answer differs — a fix that bound both
		// to the same side would pass one of these and fail the other.
		pgCase{name: "DerivedSelfJoinGroupByOtherArm", ordered: true,
			sql: `SELECT u.a, COUNT(*) AS c FROM
				(SELECT n1.n_name AS a, n2.n_name AS b FROM nation n1
					JOIN nation n2 ON n1.n_regionkey = n2.n_nationkey) u
				GROUP BY u.a ORDER BY u.a`},
		// The projection itself, without an aggregate on top: the two columns
		// came back identical.
		pgCase{name: "DerivedSelfJoinProjectsBothArms", ordered: true,
			sql: `SELECT a, b FROM
				(SELECT n1.n_name AS a, n2.n_name AS b FROM nation n1
					JOIN nation n2 ON n1.n_regionkey = n2.n_nationkey) u
				ORDER BY a, b`},
	)

	// --- #490: window, UNION and three-way derived-alias consumers ----------
	//
	// Three more consumers of a derived table's alias. On the stage DAG the
	// first two failed loud and the third resolved a join key against the arm
	// that does not own it — which the single-process pipeline did too, and
	// answered a silently wrong SUM for.
	out = append(out,
		pgCase{name: "DerivedAliasSortOverWindowProducer", ordered: true,
			sql: `SELECT k, rn FROM
				(SELECT s_suppkey AS k, ROW_NUMBER() OVER (ORDER BY s_name) AS rn
					FROM supplier) x
				ORDER BY k`},
		pgCase{name: "DerivedAliasInUnionArm",
			sql: `SELECT SUM(k) AS s FROM
				(SELECT k FROM (SELECT s_suppkey AS k FROM supplier) x
				 UNION ALL SELECT n_nationkey FROM nation) u`},
		pgCase{name: "DerivedAliasInUnionDistinctArm",
			sql: `SELECT COUNT(*) AS c FROM
				(SELECT k FROM (SELECT s_suppkey AS k FROM supplier) x
				 UNION SELECT n_nationkey FROM nation) u`},
		pgCase{name: "ThreeSiblingDerivedAliasesJoined",
			sql: `SELECT SUM(y.b) AS s FROM
				(SELECT DISTINCT s_nationkey AS a FROM supplier) x
				JOIN (SELECT DISTINCT n_nationkey AS b FROM nation) y ON x.a = y.b
				JOIN (SELECT DISTINCT r_regionkey AS c FROM region) z ON z.c = y.b`},
		pgCase{name: "ThreeSiblingDerivedAliasesJoinedNoDistinct",
			sql: `SELECT SUM(y.b) AS s FROM
				(SELECT s_nationkey AS a FROM supplier) x
				JOIN (SELECT n_nationkey AS b FROM nation) y ON x.a = y.b
				JOIN (SELECT r_regionkey AS c FROM region) z ON z.c = y.b`},
	)

	// --- #489 follow-up: a CORRELATED subquery into a derived table ---------
	//
	// A derived table whose inner scan carries an alias of its own. The
	// correlated-subquery collectors resolve `u.did` by asking which names a
	// subtree answers to, and a scan inside a derived table answers to both
	// its own alias and the derived one — the distinction #489 drew for the
	// join arms. When only ONE of the two was recorded the reference was no
	// longer seen as outer, the subquery stayed per-row, and the answer was
	// 0 rows silently on the single-process pipeline (loud on the DAG).
	out = append(out,
		pgCase{name: "CorrelatedExistsIntoDerivedTable",
			sql: `SELECT COUNT(*) AS c FROM (SELECT n1.n_nationkey AS did FROM nation n1) u
				WHERE EXISTS (SELECT 1 FROM region WHERE region.r_regionkey = u.did)`},
		pgCase{name: "CorrelatedNotExistsIntoDerivedTable",
			sql: `SELECT COUNT(*) AS c FROM (SELECT n1.n_nationkey AS did FROM nation n1) u
				WHERE NOT EXISTS (SELECT 1 FROM region WHERE region.r_regionkey = u.did)`},
		pgCase{name: "CorrelatedScalarSubqueryIntoDerivedTable", ordered: true,
			sql: `SELECT u.did, (SELECT COUNT(*) FROM region WHERE region.r_regionkey = u.did) AS c
				FROM (SELECT n1.n_nationkey AS did FROM nation n1) u ORDER BY u.did`},
		// The same three with an UNALIASED inner scan, which is the spelling
		// that always worked — a fix that traded one for the other passes
		// half of these.
		pgCase{name: "CorrelatedExistsIntoDerivedTableUnaliased",
			sql: `SELECT COUNT(*) AS c FROM (SELECT n_nationkey AS did FROM nation) u
				WHERE EXISTS (SELECT 1 FROM region WHERE region.r_regionkey = u.did)`},
		pgCase{name: "CorrelatedNotExistsIntoDerivedTableUnaliased",
			sql: `SELECT COUNT(*) AS c FROM (SELECT n_nationkey AS did FROM nation) u
				WHERE NOT EXISTS (SELECT 1 FROM region WHERE region.r_regionkey = u.did)`},
		// A self-joined derived table, where the inner aliases are the only
		// thing telling the arms apart AND the outer scope has to see `u`.
		pgCase{name: "CorrelatedExistsIntoSelfJoinedDerivedTable",
			sql: `SELECT COUNT(*) AS c FROM
				(SELECT n1.n_nationkey AS did, n2.n_name AS nm FROM nation n1
					JOIN nation n2 ON n1.n_regionkey = n2.n_nationkey) u
				WHERE EXISTS (SELECT 1 FROM region WHERE region.r_regionkey = u.did)`},
	)

	// --- #513 follow-up: DUPLICATE output column names ----------------------
	//
	// PostgreSQL answers `SELECT abs(a), abs(b)` with two columns both called
	// `abs`. This arm compares cells POSITIONALLY (see comparePostgres), which
	// is what makes these entries meaningful at all: a name comparison cannot
	// tell the two columns apart, and neither could the engine's own row map.

	// --- #513 follow-up: DUPLICATE output column names ----------------------
	//
	// PostgreSQL answers `SELECT abs(a), abs(b)` with two columns both called
	// `abs`. This arm compares cells POSITIONALLY (see comparePostgres), which
	// is what makes these entries meaningful at all: a name comparison cannot
	// tell the two columns apart, and neither could the engine's own row map.
	out = append(out,
		pgCase{name: "DuplicateNameScalarFuncs", ordered: true,
			sql: `SELECT ABS(n_nationkey), ABS(n_regionkey) FROM nation ORDER BY n_nationkey`},
		pgCase{name: "DuplicateNameFuncAndAlias", ordered: true,
			sql: `SELECT ABS(n_nationkey), n_regionkey AS abs FROM nation ORDER BY n_nationkey`},
		// The ORDER BY term is not in the SELECT list, so the hidden-sort-key
		// trim sits between the projection and the result — the projection
		// that used to copy its columns by NAME.
		pgCase{name: "DuplicateNameUnderHiddenSortKey", ordered: true,
			sql: `SELECT ABS(n_nationkey), ABS(n_regionkey) FROM nation ORDER BY n_comment`},
		pgCase{name: "DuplicateNameExplicitAlias", ordered: true,
			sql: `SELECT ABS(n_nationkey) AS x, ABS(n_regionkey) AS x FROM nation ORDER BY n_nationkey`},
	)

	// --- MIN/MAX float NaN ordering (#457) -----------------------------------
	//
	// PostgreSQL's float order (float8_cmp_internal) places NaN ABOVE every
	// other value, so a group containing a NaN answers MAX = NaN and MIN =
	// the smallest non-NaN value, wherever the NaN sits in arrival order —
	// verified live: MIN/MAX over {1.0, NaN, -Infinity, +Infinity} in every
	// arrival order all render MIN=-Infinity/MAX=NaN, an all-NaN group
	// answers NaN for both, and NULLs are skipped as usual (a NaN plus NULLs
	// still answers NaN for both). CAST('NaN' AS DOUBLE PRECISION) is the
	// SQL both engines parse identically (Go's strconv.ParseFloat and
	// PostgreSQL's float8in both read the token), so this is the same query
	// text against both.
	out = append(out,
		pgCase{name: "MinMaxFloatNaNGrouped", sql: `SELECT g, MIN(v) AS lo, MAX(v) AS hi FROM (
			SELECT 1 AS g, CAST(1.0 AS DOUBLE PRECISION) AS v
			UNION ALL SELECT 1, CAST('NaN' AS DOUBLE PRECISION)
			UNION ALL SELECT 1, CAST('-Infinity' AS DOUBLE PRECISION)
			UNION ALL SELECT 1, CAST('Infinity' AS DOUBLE PRECISION)
			UNION ALL SELECT 2, CAST('-Infinity' AS DOUBLE PRECISION)
			UNION ALL SELECT 2, CAST('NaN' AS DOUBLE PRECISION)
			UNION ALL SELECT 2, CAST('Infinity' AS DOUBLE PRECISION)
			UNION ALL SELECT 2, CAST(1.0 AS DOUBLE PRECISION)
			UNION ALL SELECT 3, CAST('NaN' AS DOUBLE PRECISION)
			UNION ALL SELECT 3, CAST(1.0 AS DOUBLE PRECISION)
			UNION ALL SELECT 3, CAST('-Infinity' AS DOUBLE PRECISION)
			UNION ALL SELECT 3, CAST('Infinity' AS DOUBLE PRECISION)
			UNION ALL SELECT 4, CAST('NaN' AS DOUBLE PRECISION)
			UNION ALL SELECT 4, CAST('NaN' AS DOUBLE PRECISION)
			UNION ALL SELECT 4, CAST('NaN' AS DOUBLE PRECISION)
			UNION ALL SELECT 5, CAST('NaN' AS DOUBLE PRECISION)
			UNION ALL SELECT 5, CAST(NULL AS DOUBLE PRECISION)
			UNION ALL SELECT 5, CAST(NULL AS DOUBLE PRECISION)
			UNION ALL SELECT 6, CAST(NULL AS DOUBLE PRECISION)
			UNION ALL SELECT 6, CAST('NaN' AS DOUBLE PRECISION)
			UNION ALL SELECT 6, CAST(2.0 AS DOUBLE PRECISION)
		) AS t GROUP BY g ORDER BY g`},
		pgCase{name: "MinMaxFloatNaNScalar", sql: `SELECT MIN(v) AS lo, MAX(v) AS hi FROM (
			SELECT CAST('NaN' AS DOUBLE PRECISION) AS v
			UNION ALL SELECT CAST(1.0 AS DOUBLE PRECISION)
			UNION ALL SELECT CAST('-Infinity' AS DOUBLE PRECISION)
			UNION ALL SELECT CAST('Infinity' AS DOUBLE PRECISION)
		) AS t`},
		pgCase{name: "MinMaxFloatAllNaNScalar", sql: `SELECT MIN(v) AS lo, MAX(v) AS hi FROM (
			SELECT CAST('NaN' AS DOUBLE PRECISION) AS v
			UNION ALL SELECT CAST('NaN' AS DOUBLE PRECISION)
			UNION ALL SELECT CAST('NaN' AS DOUBLE PRECISION)
		) AS t`},
	)

	// --- Float PREDICATES and float KEYS under the same order (#459) --------
	//
	// #457 (above) settled the aggregate accumulators. The rest of the tree
	// still read floats as IEEE754: `=` was not reflexive for NaN, `>` did not
	// place NaN above everything, and the GROUP BY / DISTINCT / hash-join key
	// hashed raw bits, so -0.0 and +0.0 were two groups and never joined.
	// PostgreSQL says otherwise on every one of those, and this is the
	// standing proof — the same SQL, both engines, no exemptions.
	//
	// pgFloatRows is the eight-row fixture read live off postgres:17-alpine
	// while these entries were written: one NaN, both zeros, both infinities,
	// two ordinary values and a NULL.
	out = append(out,
		pgCase{name: "FloatSelfEqualityIncludesNaN",
			sql: `SELECT COUNT(*) AS n FROM (` + pgFloatRows + `) t WHERE v = v`},
		pgCase{name: "FloatGreaterThanAdmitsNaN",
			sql: `SELECT k FROM (` + pgFloatRows + `) t WHERE v > 1e300 ORDER BY k`},
		pgCase{name: "FloatGreaterEqualAdmitsNaN",
			sql: `SELECT k FROM (` + pgFloatRows + `) t WHERE v >= 1e300 ORDER BY k`},
		pgCase{name: "FloatLessThanExcludesNaN",
			sql: `SELECT k FROM (` + pgFloatRows + `) t WHERE v < 1e300 ORDER BY k`},
		pgCase{name: "FloatNotEqualAdmitsNaN",
			sql: `SELECT k FROM (` + pgFloatRows + `) t WHERE v <> 1e300 ORDER BY k`},
		pgCase{name: "FloatComparedToNaNConstant",
			sql: `SELECT k FROM (` + pgFloatRows + `) t
				WHERE v = CAST('NaN' AS DOUBLE PRECISION) OR v >= CAST('NaN' AS DOUBLE PRECISION) ORDER BY k`},
		pgCase{name: "FloatLessEqualNaNConstantAdmitsAll",
			sql: `SELECT COUNT(*) AS n FROM (` + pgFloatRows + `) t WHERE v <= CAST('NaN' AS DOUBLE PRECISION)`},
		pgCase{name: "FloatInListWithNaN",
			sql: `SELECT k FROM (` + pgFloatRows + `) t WHERE v IN (CAST('NaN' AS DOUBLE PRECISION), 1.0) ORDER BY k`},
		pgCase{name: "FloatNotInListWithNaN",
			sql: `SELECT k FROM (` + pgFloatRows + `) t WHERE v NOT IN (CAST('NaN' AS DOUBLE PRECISION), 1.0) ORDER BY k`},
		// The KEY half. The two zeros are one group of two, the NaN is its
		// own group of one, and the NULL is a group as GROUP BY (not
		// COUNT(DISTINCT)) counts it.
		pgCase{name: "FloatGroupKeyFoldsZerosAndNaN",
			sql: `SELECT COUNT(*) AS c, MIN(k) AS lo FROM (` + pgFloatRows + `) t GROUP BY v ORDER BY c, lo`},
		pgCase{name: "FloatDistinctFoldsZerosAndNaN",
			sql: `SELECT COUNT(*) AS n FROM (SELECT DISTINCT v FROM (` + pgFloatRows + `) t) d`},
		pgCase{name: "FloatCountDistinctFoldsZerosAndNaN",
			sql: `SELECT COUNT(DISTINCT v) AS n FROM (` + pgFloatRows + `) t`},
	)

	// --- DECIMAL group / DISTINCT / join keys (#474) ------------------------
	//
	// The key for a DECIMAL was the float64 BITS of the value, which holds ~16
	// significant digits against a DECIMAL(38,10)'s 38 — so values that differ
	// only past the 16th shared a group, a distinct value and a join match.
	// The repair has to be exact AND scale-blind, because `numeric '12.75' =
	// numeric '12.7500'` is TRUE: the cross-scale join below is the half a
	// raw-unscaled-bytes key would have broken, and it runs over two REAL
	// columns of different scale (d_2 at scale 2 and d_4 at scale 4 coincide
	// wherever 0.25a = 0.0625b).
	out = append(out,
		pgCase{name: "DecimalWideGroupByCount",
			sql: `SELECT COUNT(*) AS n FROM (SELECT d_wide FROM dec_probe GROUP BY d_wide) g`},
		pgCase{name: "DecimalWideCountDistinct", sql: `SELECT COUNT(DISTINCT d_wide) AS n FROM dec_probe`},
		// The IS NOT NULL guards are load-bearing, not decoration: the
		// serialized join key encodes a NULL as a lone flag byte, so two NULL
		// rows key alike and the equi-join pairs them — a separate defect
		// (#459's NULL-joins-NULL note) that would otherwise swamp what these
		// entries measure. The unguarded forms are the gate for THAT.
		pgCase{name: "DecimalWideSelfJoin",
			sql: `SELECT COUNT(*) AS n FROM dec_probe a JOIN dec_probe b
				ON a.d_wide = b.d_wide WHERE a.d_wide IS NOT NULL`},
		pgCase{name: "DecimalNarrowPairGroupBy",
			sql: `SELECT COUNT(*) AS n FROM (SELECT d_2, d_4 FROM dec_probe GROUP BY d_2, d_4) g`},
		pgCase{name: "DecimalCrossScaleJoinCount",
			sql: `SELECT COUNT(*) AS n FROM dec_probe a JOIN dec_probe b ON a.d_2 = b.d_4
				WHERE a.d_2 IS NOT NULL AND b.d_4 IS NOT NULL`},
		pgCase{name: "DecimalCrossScaleJoinKeys",
			sql: `SELECT a.d_key, b.d_key FROM dec_probe a JOIN dec_probe b ON a.d_2 = b.d_4
				WHERE a.d_2 IS NOT NULL AND b.d_4 IS NOT NULL ORDER BY a.d_key, b.d_key`},
		pgCase{name: "DecimalCrossScaleSemiJoin",
			sql: `SELECT a.d_key FROM dec_probe a WHERE EXISTS
				(SELECT 1 FROM dec_probe b WHERE b.d_4 = a.d_2) ORDER BY a.d_key`},
		// The same two joins WITHOUT the IS NOT NULL guards. Both columns are
		// nullable, so these also assert that a NULL key matches nothing —
		// itself included — which is what the serialized join key's lone
		// null-flag byte got wrong (#459).
		pgCase{name: "DecimalWideSelfJoinWithNulls",
			sql: `SELECT COUNT(*) AS n FROM dec_probe a JOIN dec_probe b ON a.d_wide = b.d_wide`},
		pgCase{name: "DecimalCrossScaleJoinWithNulls",
			sql: `SELECT COUNT(*) AS n FROM dec_probe a JOIN dec_probe b ON a.d_2 = b.d_4`},
		// A NULL key must not match in a plain equi-join over any type, and
		// the STRING key is the encoding the defect lived in.
		pgCase{name: "NullableStringKeySelfJoin",
			sql: `SELECT COUNT(*) AS n FROM dec_probe a JOIN dec_probe b ON a.d_2 = b.d_2`},
	)

	// A TEXT value that LOOKS like a number is still text (#504). Wadjet's
	// row-at-a-time path used to read any string that parsed as a number
	// numerically against the other operand, which made a genuine STRING
	// column compare NUMERICALLY there and as TEXT through the vectorized
	// kernel — one predicate with two answers.
	//
	// PostgreSQL cannot be asked the shape that broke (`s = 1.5` is 42883,
	// "operator does not exist: text = numeric" — an overload-resolution
	// failure wadjet has no overload set to reproduce; ADR-0012 item 5). It
	// CAN be asked the QUOTED form, which is the same comparison once the
	// literal is typed, and that is what these gate: the byte order where
	// "1.50" and "1.5" are two values and "9" sorts ABOVE "10".
	out = append(out,
		pgCase{name: "NumericLookingTextEq",
			sql: `SELECT k FROM (` + pgNumericTextRows + `) t WHERE v = '1.5' ORDER BY k`},
		pgCase{name: "NumericLookingTextEqTrailingZero",
			sql: `SELECT k FROM (` + pgNumericTextRows + `) t WHERE v = '1.50' ORDER BY k`},
		pgCase{name: "NumericLookingTextNe",
			sql: `SELECT k FROM (` + pgNumericTextRows + `) t WHERE v <> '1.5' ORDER BY k`},
		pgCase{name: "NumericLookingTextGt",
			sql: `SELECT k FROM (` + pgNumericTextRows + `) t WHERE v > '1.5' ORDER BY k`},
		pgCase{name: "NumericLookingTextLt",
			sql: `SELECT k FROM (` + pgNumericTextRows + `) t WHERE v < '10' ORDER BY k`},
		pgCase{name: "NumericLookingTextGtTwoDigit",
			sql: `SELECT k FROM (` + pgNumericTextRows + `) t WHERE v > '10' ORDER BY k`},
		pgCase{name: "NumericLookingTextOrder",
			sql: `SELECT v FROM (` + pgNumericTextRows + `) t ORDER BY v`},
		// The three boxed sites over the same values, where wadjet reads the
		// operand through a different evaluator.
		pgCase{name: "NumericLookingTextSimpleCase",
			sql: `SELECT k FROM (` + pgNumericTextRows + `) t
				WHERE CASE v WHEN '1.5' THEN 1 ELSE 0 END = 1 ORDER BY k`},
		pgCase{name: "NumericLookingTextIsDistinctFrom",
			sql: `SELECT k FROM (` + pgNumericTextRows + `) t WHERE v IS DISTINCT FROM '1.5' ORDER BY k`},
		pgCase{name: "NumericLookingTextGreatest",
			sql: `SELECT k FROM (` + pgNumericTextRows + `) t WHERE GREATEST(v, '1.5') = v ORDER BY k`},
		pgCase{name: "NumericLookingTextLeast",
			sql: `SELECT k FROM (` + pgNumericTextRows + `) t WHERE LEAST(v, '1.5') = '1.5' ORDER BY k`},
	)

	// --- ARRAY[...] built from a column (#596) ---------------------------
	//
	// collectASTColumnRefs (internal/planner/logical/optimizer.go) had no
	// case for ArrayLitNode, so a column referenced ONLY inside ARRAY[...]
	// was pruned out of the scan before the ARRAY constructor ever ran,
	// which read a column that was not in the batch and produced NULL per
	// element. None of the entries below reference the target column
	// anywhere else in the query — the filters and ORDER BY keys are chosen
	// so nothing but the ARRAY[...] expression itself keeps the column
	// alive, which is exactly the shape that went silently wrong.
	out = append(out,
		pgCase{name: "ArrayLitSingleColumn",
			sql: `SELECT n_nationkey, ARRAY[n_name] AS a FROM nation WHERE n_nationkey < 5 ORDER BY n_nationkey`},
		// array_to_string, not the raw two-element array: PostgreSQL's own
		// array literal text form ("{a,b}") and wadjet's container
		// rendering are a text-CONVENTION difference neither engine is
		// wrong about (same family as the DECIMAL boxing note above).
		// array_to_string is common ground both engines answer identically,
		// and its argument is still ARRAY[n_name, n_comment] nested inside a
		// function call — the same collectASTColumnRefs walk
		// (FuncCallNode -> ArrayLitNode -> ColRef) #596 fixed.
		pgCase{name: "ArrayLitTwoColumns",
			sql: `SELECT n_nationkey, array_to_string(ARRAY[n_name, n_comment], '|') AS a FROM nation WHERE n_nationkey < 5 ORDER BY n_nationkey`},
		pgCase{name: "ArrayLitFuncArg",
			sql: `SELECT n_nationkey, ARRAY[UPPER(n_name)] AS a FROM nation WHERE n_nationkey < 5 ORDER BY n_nationkey`},
		pgCase{name: "ArrayLitArithmetic",
			sql: `SELECT n_nationkey, ARRAY[n_regionkey + 1] AS a FROM nation WHERE n_nationkey < 5 ORDER BY n_nationkey`},
		pgCase{name: "ArrayLitGroupBy",
			sql: `SELECT ARRAY[n_regionkey] AS a, COUNT(*) AS n FROM nation GROUP BY ARRAY[n_regionkey] ORDER BY a`},
		pgCase{name: "ArrayLitOrderBy",
			sql: `SELECT n_nationkey FROM nation ORDER BY ARRAY[n_name], n_nationkey LIMIT 5`},
		pgCase{name: "ArrayLitWhereEquality",
			sql: `SELECT n_nationkey FROM nation WHERE ARRAY[n_name] = ARRAY['CANADA'] ORDER BY n_nationkey`},
		// No ArrayLitNested entry here (unlike the DuckDB and two-path
		// corpora): PostgreSQL's ARRAY[ARRAY[x]] is not a nested container —
		// it builds one genuine N-DIMENSIONAL array (`{{ALGERIA}}`, a 1x1
		// two-dimensional text[] value), which is a different TYPE than
		// wadjet's ARRAY-of-ARRAY container (matching DuckDB's nested LIST).
		// Verified live: comparing the two answers content-digest-diverges
		// on every row even though both sides hold "ALGERIA" — PostgreSQL
		// has no equivalent of a jagged/nested array to be authoritative
		// about here, so per ADR-0012 this is not a semantics question
		// PostgreSQL can settle, not a #596-style defect.
		//
		// One row of the 25-row nation table (regionkey 1) is forced to NULL
		// via NULLIF so the constructed array holds a genuine NULL element,
		// not just a pruned-away one. Indexed + IS NULL rather than the raw
		// array: both engines DROP a NULL element from array_to_string's
		// join (so that form can't tell "NULL survived" from "column was
		// pruned to NULL and then dropped"), and the raw array's per-engine
		// NULL-slot text rendering is exactly the boxing difference
		// ArrayLitTwoColumns' comment describes. Indexing into element 1
		// and asking IS NULL answers a plain boolean both engines render
		// identically, while still routing n_regionkey through NULLIF
		// nested inside ArrayLitNode nested inside a subscript expression.
		pgCase{name: "ArrayLitOverNullColumn",
			sql: `SELECT n_nationkey, (ARRAY[NULLIF(n_regionkey, 1)])[1] IS NULL AS a FROM nation WHERE n_nationkey < 5 ORDER BY n_nationkey`},
		pgCase{name: "ArrayLitInDerivedTable",
			sql: `SELECT a FROM (SELECT ARRAY[n_name] AS a FROM nation WHERE n_nationkey < 5) t ORDER BY a`},
	)

	// --- GROUP BY / HAVING: what a grouped query MEANS (#590, #591) --------
	//
	// Every entry here is a query PostgreSQL ANSWERS. The ones it refuses —
	// an ungrouped column in SELECT / HAVING / ORDER BY — are 42803 and live
	// in the wire arm's runWireErrors, where a SQLSTATE is comparable.
	//
	// The shape that matters is TLP's: `HAVING p`, `HAVING NOT p` and
	// `HAVING p IS NULL` partition the ungated GROUP BY, so the three arms of
	// each predicate below must sum to the unfiltered answer. Wadjet failed
	// that at volume in two different ways — an aggregate predicate the
	// lowering never registered (so `MAX(v) IS NULL` was true for every group
	// and `IS NOT NULL` for none) and a synthetic `__having_0` column in the
	// result — and neither was visible to the DuckDB arm, because both arms
	// of a two-path comparison get the first one identically wrong.
	out = append(out,
		// The ungated GROUP BY each partition below must reconstruct.
		pgCase{name: "HavingBaseGroups", sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k ORDER BY k`},

		// BOOL_OR: a bare aggregate AS the predicate — #591 repro 1's shape.
		pgCase{name: "HavingBoolOrBare",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING BOOL_OR(flag) ORDER BY k`},
		pgCase{name: "HavingBoolOrNot",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING NOT BOOL_OR(flag) ORDER BY k`},
		pgCase{name: "HavingBoolOrIsNull",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING (BOOL_OR(flag)) IS NULL ORDER BY k`},
		// BOOL_AND, the same three arms over the other boolean aggregate.
		pgCase{name: "HavingBoolAndBare",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING BOOL_AND(flag) ORDER BY k`},
		pgCase{name: "HavingBoolAndNot",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING NOT BOOL_AND(flag) ORDER BY k`},
		pgCase{name: "HavingBoolAndIsNull",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING (BOOL_AND(flag)) IS NULL ORDER BY k`},

		// MAX over a column whose group-2 values are all NULL: the aggregate
		// is NULL for exactly one group, so IS NULL and IS NOT NULL split the
		// four groups 1/3. Answering "every group" and "no group" — which is
		// what #591 repro 2 did — passes neither.
		pgCase{name: "HavingMaxIsNull",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING MAX(v) IS NULL ORDER BY k`},
		pgCase{name: "HavingMaxIsNotNull",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING MAX(v) IS NOT NULL ORDER BY k`},
		pgCase{name: "HavingMinIsNull",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING MIN(v) IS NULL ORDER BY k`},
		pgCase{name: "HavingMinIsNotNull",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING MIN(v) IS NOT NULL ORDER BY k`},
		pgCase{name: "HavingSumIsNull",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING SUM(v) IS NULL ORDER BY k`},
		pgCase{name: "HavingSumIsNotNull",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING SUM(v) IS NOT NULL ORDER BY k`},
		// COUNT is never NULL, which is a different fact about the same
		// lowering: IS NULL must answer NO groups and IS NOT NULL all of them.
		pgCase{name: "HavingCountIsNull",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING COUNT(v) IS NULL ORDER BY k`},
		pgCase{name: "HavingCountIsNotNull",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING COUNT(v) IS NOT NULL ORDER BY k`},

		// Comparisons, and their negations. Every one of these reaches the
		// aggregate through a node the aggregate walkers used to stop at.
		pgCase{name: "HavingCountCmp",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING COUNT(*) > 1 ORDER BY k`},
		pgCase{name: "HavingCountCmpNegated",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING NOT (COUNT(*) > 1) ORDER BY k`},
		pgCase{name: "HavingSumCmp",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING SUM(v) > 15 ORDER BY k`},
		pgCase{name: "HavingMinCmp",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING MIN(v) <= 7 ORDER BY k`},
		pgCase{name: "HavingCountEqZero",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING COUNT(v) = 0 ORDER BY k`},
		// Conjunction, disjunction, IN and BETWEEN over aggregates: AND/OR
		// used to fail outright ("filter column \"count(*)\" does not exist"),
		// IN and BETWEEN silently returned nothing.
		pgCase{name: "HavingConjunction",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING COUNT(*) > 1 AND MAX(v) > 0 ORDER BY k`},
		pgCase{name: "HavingDisjunction",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING COUNT(*) > 1 OR MAX(v) > 6 ORDER BY k`},
		pgCase{name: "HavingAggIn",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING MAX(v) IN (20, 5) ORDER BY k`},
		pgCase{name: "HavingAggBetween",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING MAX(v) BETWEEN 1 AND 9 ORDER BY k`},
		// An aggregate the SELECT list already computes: HAVING must reference
		// that column, not a second copy of the same aggregate.
		pgCase{name: "HavingReusesSelectedAggregate",
			sql: `SELECT k, COUNT(*) AS c FROM (` + pgHavingRows + `) t GROUP BY k HAVING COUNT(*) > 1 ORDER BY k`},
		// (`HAVING <select alias>` is deliberately NOT here: PostgreSQL refuses
		// it — an output alias is visible to GROUP BY and ORDER BY but not to
		// HAVING — so it cannot be ground truth for anything. Wadjet accepts
		// it as an extension, gated in the planner's own binder tests.)
		// An aggregate inside a NULL check in the SELECT list, which is the
		// same walker gap one clause over.
		pgCase{name: "SelectAggregateIsNull",
			sql: `SELECT k, MAX(v) IS NULL AS mx_null FROM (` + pgHavingRows + `) t GROUP BY k ORDER BY k`},
		pgCase{name: "SelectAggregateNegated",
			sql: `SELECT k, NOT BOOL_OR(flag) AS none_set FROM (` + pgHavingRows + `) t GROUP BY k ORDER BY k`},

		// The LEGAL side of #590: a grouped query whose SELECT list and
		// ORDER BY stay inside the grouped expressions. These are the queries
		// the 42803 refusal must NOT touch — a false positive here breaks
		// working SQL, which is the more expensive of the two mistakes.
		pgCase{name: "GroupedSelectsGroupedColumn",
			sql: `SELECT n_regionkey, COUNT(*) AS c FROM nation GROUP BY n_regionkey ORDER BY n_regionkey`},
		pgCase{name: "GroupedSelectsExpressionOverKey",
			sql: `SELECT n_regionkey + 1 AS r1, COUNT(*) AS c FROM nation GROUP BY n_regionkey ORDER BY r1`},
		pgCase{name: "GroupedSelectsQualifiedKey",
			sql: `SELECT nation.n_regionkey, COUNT(*) AS c FROM nation GROUP BY n_regionkey ORDER BY 1`},
		pgCase{name: "GroupedByQualifiedKey",
			sql: `SELECT n_regionkey, COUNT(*) AS c FROM nation GROUP BY nation.n_regionkey ORDER BY 1`},
		pgCase{name: "GroupedByOrdinal",
			sql: `SELECT n_regionkey, COUNT(*) AS c FROM nation GROUP BY 1 ORDER BY 1`},
		pgCase{name: "GroupedByOutputAlias",
			sql: `SELECT n_regionkey AS r, COUNT(*) AS c FROM nation GROUP BY r ORDER BY r`},
		pgCase{name: "GroupedByExpressionAlias",
			sql: `SELECT n_regionkey % 3 AS r, COUNT(*) AS c FROM nation GROUP BY r ORDER BY r`},
		pgCase{name: "GroupedByExpressionRepeated",
			sql: `SELECT SUBSTR(n_name, 1, 1) AS ini, COUNT(*) AS c FROM nation
				GROUP BY SUBSTR(n_name, 1, 1) ORDER BY ini`},
		pgCase{name: "GroupedKeyNotSelected",
			sql: `SELECT COUNT(*) AS c FROM nation GROUP BY n_regionkey ORDER BY c, n_regionkey`},
		pgCase{name: "GroupedExtraKeyNotSelected",
			sql: `SELECT n_regionkey FROM nation GROUP BY n_regionkey, n_nationkey ORDER BY n_regionkey, n_nationkey`},
		pgCase{name: "GroupedOrderByAggregate",
			sql: `SELECT n_regionkey, COUNT(*) AS c FROM nation GROUP BY n_regionkey ORDER BY COUNT(*), n_regionkey`},
		pgCase{name: "GroupedOrderByUnselectedAggregate",
			sql: `SELECT n_regionkey FROM nation GROUP BY n_regionkey ORDER BY MAX(n_nationkey)`,
			knownBug: pgBugUnsupported + " ORDER BY an aggregate the SELECT list does not carry is refused " +
				"outright (\"an aggregate expression that is not itself a select item cannot be sorted on\"). " +
				"resolveOrderBy already materializes an unselected ORDER BY COLUMN under a hidden projection " +
				"(#320); the aggregate spelling needs the same treatment one node down. A refusal is the safe " +
				"direction of failure, which is why this is pinned rather than fixed here",
			issue: "#597"},
		pgCase{name: "GroupedCorrelatedOuterColumn",
			sql: `SELECT r_regionkey, (SELECT COUNT(*) FROM nation n WHERE n.n_regionkey = r.r_regionkey) AS c
				FROM region r ORDER BY r_regionkey`},

		// The SQLancer TLP reductions, verbatim from #590 and #591, over the
		// same fixture their tables described. Every finding this soak makes
		// becomes a permanent corpus entry.
		pgCase{name: "SQLancerTLPHavingBoolOr",
			sql: `SELECT k FROM (` + pgHavingRows + `) t4 GROUP BY k HAVING BOOL_OR(flag) ORDER BY k`},
		pgCase{name: "SQLancerTLPHavingBoolOrNegated",
			sql: `SELECT k FROM (` + pgHavingRows + `) t4 GROUP BY k HAVING NOT (BOOL_OR(flag)) ORDER BY k`},
		pgCase{name: "SQLancerTLPHavingBoolOrUnknown",
			sql: `SELECT k FROM (` + pgHavingRows + `) t4 GROUP BY k HAVING (BOOL_OR(flag)) IS NULL ORDER BY k`},
		pgCase{name: "SQLancerHavingMaxCastToInt",
			sql: `SELECT k FROM (` + pgHavingRows + `) t GROUP BY k HAVING MAX(CAST(flag AS INTEGER)) > 0 ORDER BY k`},
	)

	out = append(out, postgresBytesCases()...)

	// PostgreSQL-valid DATE spellings the widened accept-set must PARSE, not
	// reject (#560). mk_outer.dt holds 2024-01-01..2024-01-05, so each of
	// these non-canonical forms of 2024-01-03 must count the same rows as the
	// canonical one in BOTH engines — a value oracle catches an accept-set
	// divergence as "wadjet cannot answer" when it rejects a literal
	// PostgreSQL takes. The zero-padded canonical entries the error corpus
	// already carries only test the reject side; these test the accept side.
	out = append(out,
		pgCase{name: "DateCanonicalSpelling",
			sql: `SELECT COUNT(*) AS n FROM mk_outer WHERE dt = '2024-01-03'`},
		pgCase{name: "DateSingleDigitFields",
			sql: `SELECT COUNT(*) AS n FROM mk_outer WHERE dt = '2024-1-3'`},
		pgCase{name: "DateSlashSeparator",
			sql: `SELECT COUNT(*) AS n FROM mk_outer WHERE dt = '2024/01/03'`},
		pgCase{name: "DateCompactEightDigit",
			sql: `SELECT COUNT(*) AS n FROM mk_outer WHERE dt = '20240103'`},
		pgCase{name: "DateSurroundingWhitespace",
			sql: `SELECT COUNT(*) AS n FROM mk_outer WHERE dt = ' 2024-01-03 '`},
	)

	// The OTHER side of the accept-set: a short/MDY DATE spelling PostgreSQL
	// PARSES its own way (a leading field it reads as the MONTH, not the
	// year) — '5/6/7' is 2007-05-06 and '01/02/2026' is 2026-01-02. wadjet
	// deliberately REFUSES these rather than guess year-first and store a
	// value that differs from PostgreSQL's (#560 invariant). Pinned to #639:
	// the day wadjet parses DateStyle-ordered dates, it will answer these and
	// this pin fires. Without the pin the entry is a plain divergence; with it
	// the corpus records exactly which spellings are deferred and proves they
	// still ERROR (wadjet cannot answer) rather than silently diverging.
	out = append(out,
		pgCase{name: "DateShortMDYRefused",
			sql:      `SELECT COUNT(*) AS n FROM mk_outer WHERE dt = '5/6/7'`,
			knownBug: pgBugUnsupported + " a short DATE literal whose field order PostgreSQL takes from DateStyle (MDY: '5/6/7' is 2007-05-06) is refused, not guessed year-first",
			issue:    "#639"},
		pgCase{name: "DateTrailingYearMDYRefused",
			sql:      `SELECT COUNT(*) AS n FROM mk_outer WHERE dt = '01/02/2026'`,
			knownBug: pgBugUnsupported + " a trailing-4-digit-year MDY DATE literal ('01/02/2026' is 2026-01-02 to PostgreSQL) is refused, not guessed",
			issue:    "#639"},
	)

	out = append(out, multiKeyCorrelatedCases()...)

	// --- A window's PARTITION BY / ORDER BY key spelling (#585) -------------
	//
	// Every window entry above writes its keys as BARE columns, and that was
	// load-bearing: a key spelled any other way was silently DROPPED and the
	// window ran over one partition spanning the input. `PARTITION BY n.n_regionkey`
	// missed the batch's bare `n_regionkey`; `PARTITION BY n_nationkey % 3`
	// named a column nothing had computed. Neither errored — ROW_NUMBER()
	// numbered straight through every region and SUM OVER returned the
	// whole-table sum, which is a plausible-looking answer to a different
	// query.
	//
	// PostgreSQL is the authority here for the reason ADR-0012 gives: these
	// are the spellings a BI client emits, and what a client expects of them
	// is defined by the server it was written against. It partitions in every
	// shape below (verified live on postgres:17-alpine), including the two
	// wadjet had no answer for at all — a ROW field path and an unresolvable
	// name, where PostgreSQL's answer is an ERROR and silence is the wrong
	// one.
	//
	// Every ORDER BY inside an OVER ends on a UNIQUE column, so no frame
	// depends on tie order (the reason WindowDefaultFrameIsRange states), and
	// every entry has a total top-level ORDER BY so the row SEQUENCE is part
	// of the comparison.
	out = append(out,
		pgCase{name: "WindowKeyQualifiedRankFamily", ordered: true, sql: `SELECT n.n_nationkey,
			ROW_NUMBER() OVER (PARTITION BY n.n_regionkey ORDER BY n.n_nationkey) AS rn,
			RANK() OVER (PARTITION BY n.n_regionkey ORDER BY n.n_nationkey) AS rk,
			DENSE_RANK() OVER (PARTITION BY n.n_regionkey ORDER BY n.n_nationkey) AS drk
			FROM nation n ORDER BY n.n_nationkey`},
		// No ORDER BY inside the OVER: the whole-partition shape, where a lost
		// key is a whole-TABLE aggregate rather than a running one.
		pgCase{name: "WindowKeyQualifiedAggregatesNoOrderBy", ordered: true, sql: `SELECT n.n_nationkey,
			SUM(n.n_regionkey) OVER (PARTITION BY n.n_regionkey) AS s,
			COUNT(*) OVER (PARTITION BY n.n_regionkey) AS c,
			MIN(n.n_name) OVER (PARTITION BY n.n_regionkey) AS lo
			FROM nation n ORDER BY n.n_nationkey`},
		pgCase{name: "WindowKeyQualifiedValueFunctions", ordered: true, sql: `SELECT n.n_nationkey,
			LAG(n.n_name) OVER (PARTITION BY n.n_regionkey ORDER BY n.n_nationkey) AS lag_name,
			LEAD(n.n_name) OVER (PARTITION BY n.n_regionkey ORDER BY n.n_nationkey) AS lead_name,
			FIRST_VALUE(n.n_name) OVER (PARTITION BY n.n_regionkey ORDER BY n.n_nationkey) AS first_name
			FROM nation n ORDER BY n.n_nationkey`},

		// The expression families, one entry each.
		pgCase{name: "WindowKeyExpressionModulo", ordered: true, sql: `SELECT n_nationkey,
			ROW_NUMBER() OVER (PARTITION BY n_nationkey % 3 ORDER BY n_nationkey) AS rn,
			COUNT(*) OVER (PARTITION BY n_nationkey % 3) AS c
			FROM nation ORDER BY n_nationkey`},
		pgCase{name: "WindowKeyExpressionArithmetic", ordered: true, sql: `SELECT n_nationkey,
			SUM(n_regionkey) OVER (PARTITION BY n_regionkey - 1 ORDER BY n_nationkey) AS s
			FROM nation ORDER BY n_nationkey`},
		pgCase{name: "WindowKeyExpressionFunction", ordered: true, sql: `SELECT n_nationkey,
			COUNT(*) OVER (PARTITION BY SUBSTR(n_name, 1, 1)) AS c
			FROM nation ORDER BY n_nationkey`},
		pgCase{name: "WindowKeyExpressionCase", ordered: true, sql: `SELECT n_nationkey,
			ROW_NUMBER() OVER (PARTITION BY CASE WHEN n_regionkey < 2 THEN 'lo' ELSE 'hi' END
				ORDER BY n_nationkey) AS rn
			FROM nation ORDER BY n_nationkey`},
		// The ORDER BY inside the OVER is the expression, which is the same
		// resolution and a different clause: dropping it loses the frame's
		// order rather than the partition, and answers a running total
		// accumulated in the wrong sequence.
		pgCase{name: "WindowKeyExpressionInWindowOrderBy", ordered: true, sql: `SELECT n_nationkey,
			ROW_NUMBER() OVER (PARTITION BY n_regionkey ORDER BY n_nationkey % 5, n_nationkey) AS rn
			FROM nation ORDER BY n_nationkey`},
		pgCase{name: "WindowKeyExpressionOrderByDescNullsFirst", ordered: true, sql: `SELECT n_nationkey,
			ROW_NUMBER() OVER (PARTITION BY n_regionkey
				ORDER BY NULLIF(n_nationkey, 3) DESC NULLS FIRST, n_nationkey) AS rn
			FROM nation ORDER BY n_nationkey`},

		// PostgreSQL puts every NULL key in ONE partition — the same rule
		// GROUP BY follows, and the one an engine that skips a null key
		// breaks. NULLIF makes region 2's five nations the NULL partition.
		pgCase{name: "WindowKeyNullPartition", ordered: true, sql: `SELECT n_nationkey,
			COUNT(*) OVER (PARTITION BY NULLIF(n_regionkey, 2)) AS c,
			ROW_NUMBER() OVER (PARTITION BY NULLIF(n_regionkey, 2) ORDER BY n_nationkey) AS rn
			FROM nation ORDER BY n_nationkey`},

		// One key list mixing all three spellings, and two OVER clauses
		// SHARING one computed key — the case a per-clause materialization
		// computes twice.
		pgCase{name: "WindowKeyMixedSpellings", ordered: true, sql: `SELECT n.n_nationkey,
			ROW_NUMBER() OVER (PARTITION BY n_regionkey, n.n_name, n.n_nationkey % 2
				ORDER BY n.n_nationkey) AS rn
			FROM nation n ORDER BY n.n_nationkey`},
		pgCase{name: "WindowKeySharedExpressionAcrossClauses", ordered: true, sql: `SELECT n_nationkey,
			ROW_NUMBER() OVER (PARTITION BY n_nationkey % 4 ORDER BY n_nationkey) AS rn,
			COUNT(*) OVER (PARTITION BY n_nationkey % 4) AS c
			FROM nation ORDER BY n_nationkey`},

		// The window's input is not a scan: an aggregate, a derived table, a
		// CTE. Each gives the planner a different amount of schema to type
		// the materialized key from, and an INT64 key declared FLOAT64 merges
		// partitions that differ past 2^53.
		pgCase{name: "WindowKeyOverGroupByHaving", ordered: true, sql: `SELECT n_regionkey,
			ROW_NUMBER() OVER (PARTITION BY n_regionkey % 2 ORDER BY n_regionkey) AS rn
			FROM nation GROUP BY n_regionkey HAVING COUNT(*) > 1 ORDER BY n_regionkey`},
		pgCase{name: "WindowKeyOverDerivedTable", ordered: true, sql: `SELECT u.k,
			ROW_NUMBER() OVER (PARTITION BY u.k % 3 ORDER BY u.k) AS rn
			FROM (SELECT n_nationkey AS k FROM nation WHERE n_nationkey < 12) u ORDER BY u.k`},
		pgCase{name: "WindowKeyOverCTE", ordered: true, sql: `WITH u AS (
				SELECT n_nationkey AS k, n_regionkey AS r FROM nation)
			SELECT u.k, COUNT(*) OVER (PARTITION BY u.r) AS c,
			ROW_NUMBER() OVER (PARTITION BY u.k % 2 ORDER BY u.k) AS rn
			FROM u ORDER BY u.k`},
	)
	// --- A BOOLEAN-VALUED EXPRESSION USED AS THE PREDICATE (#592) ----------
	//
	// `Cast.Eval` had no boolean arm, so a CAST to BOOLEAN returned its
	// operand unconverted and three consumers read the box three different
	// ways: the projection coerced an integer to `!= 0` through
	// Vector.SetValue, NOT and IS NULL read the same truthiness through
	// toBoolVal, and the FILTER asked `v.(bool)`, failed the assertion, and
	// answered FALSE for every row. One expression, three readings — which is
	// why the SQLancer TLP-WHERE oracle found it: its partition IS those three
	// readings, and the arm that contributed nothing made it undercount.
	//
	// n_regionkey is an `integer` on both sides, which is the one integer
	// width PostgreSQL HAS a boolean cast for; it is 0 for five of the 25
	// nations, so the FALSE arm is not empty. Wadjet extends the same rule to
	// BIGINT, which PostgreSQL cannot be asked at all ("cannot cast type
	// bigint to boolean") — that half is a deliberate divergence recorded in
	// ADR-0012 item 5 and gated in wadjet.TestCastToBooleanTruthTable, not
	// here, because an entry PostgreSQL refuses is not a question about
	// wadjet.
	out = append(out,
		pgCase{name: "BareCastIntPredicate",
			sql: `SELECT n_nationkey FROM nation WHERE CAST(n_regionkey AS BOOLEAN) ORDER BY n_nationkey`},
		pgCase{name: "BareCastIntPredicateColonCast",
			sql: `SELECT n_nationkey FROM nation WHERE (n_regionkey)::BOOLEAN ORDER BY n_nationkey`},
		pgCase{name: "BareCastIntNegated",
			sql: `SELECT n_nationkey FROM nation WHERE NOT CAST(n_regionkey AS BOOLEAN) ORDER BY n_nationkey`},
		pgCase{name: "BareCastIntEqTrue",
			sql: `SELECT n_nationkey FROM nation WHERE CAST(n_regionkey AS BOOLEAN) = TRUE ORDER BY n_nationkey`},
		pgCase{name: "BareCastIntIsTrue",
			sql: `SELECT n_nationkey FROM nation WHERE CAST(n_regionkey AS BOOLEAN) IS TRUE ORDER BY n_nationkey`},
		pgCase{name: "BareCastIntIsNotFalse",
			sql: `SELECT n_nationkey FROM nation WHERE CAST(n_regionkey AS BOOLEAN) IS NOT FALSE ORDER BY n_nationkey`},
		// The SELECT list and the WHERE clause over ONE cast, in the same
		// corpus: the divergence was between them, so an arm that only
		// projected or only filtered could not see it.
		pgCase{name: "BareCastIntProjected",
			sql: `SELECT n_nationkey, CAST(n_regionkey AS BOOLEAN) AS b FROM nation ORDER BY n_nationkey`},
		pgCase{name: "BareCastThroughDerivedTable",
			sql: `SELECT n_nationkey FROM (SELECT n_nationkey, CAST(n_regionkey AS BOOLEAN) AS b FROM nation) s
				WHERE b ORDER BY n_nationkey`},

		// A NULL-bearing cast, so the three-valued rule is exercised: a WHERE
		// admits only TRUE, so a NULL predicate drops the row from BOTH the
		// predicate and its negation.
		pgCase{name: "BareCastNullablePredicate",
			sql: `SELECT n_nationkey FROM nation WHERE CAST(NULLIF(n_regionkey, 1) AS BOOLEAN) ORDER BY n_nationkey`},
		pgCase{name: "BareCastNullableNegated",
			sql: `SELECT n_nationkey FROM nation WHERE NOT CAST(NULLIF(n_regionkey, 1) AS BOOLEAN) ORDER BY n_nationkey`},
		pgCase{name: "BareCastNullableIsNull",
			sql: `SELECT n_nationkey FROM nation WHERE CAST(NULLIF(n_regionkey, 1) AS BOOLEAN) IS NULL ORDER BY n_nationkey`},
		pgCase{name: "BareCastNullableIsUnknown",
			sql: `SELECT n_nationkey FROM nation WHERE CAST(NULLIF(n_regionkey, 1) AS BOOLEAN) IS UNKNOWN ORDER BY n_nationkey`},
		pgCase{name: "BareCastNullableProjected",
			sql: `SELECT n_nationkey, CAST(NULLIF(n_regionkey, 1) AS BOOLEAN) AS b FROM nation ORDER BY n_nationkey`},
		pgCase{name: "BareCastNullableCoalesce",
			sql: `SELECT n_nationkey FROM nation
				WHERE COALESCE(CAST(NULLIF(n_regionkey, 1) AS BOOLEAN), FALSE) ORDER BY n_nationkey`},

		// TLP-WHERE's partition itself: the three arms of one predicate are
		// the whole table, once each. This is the assertion that failed.
		pgCase{name: "BareCastTLPWherePartition",
			sql: `SELECT n_nationkey FROM nation WHERE CAST(NULLIF(n_regionkey, 1) AS BOOLEAN)
				UNION ALL SELECT n_nationkey FROM nation WHERE NOT (CAST(NULLIF(n_regionkey, 1) AS BOOLEAN))
				UNION ALL SELECT n_nationkey FROM nation WHERE CAST(NULLIF(n_regionkey, 1) AS BOOLEAN) IS NULL
				ORDER BY n_nationkey`},

		// The other clauses a bare boolean can be the whole of.
		pgCase{name: "BareCastCaseWhen",
			sql: `SELECT n_nationkey FROM nation
				WHERE CASE WHEN CAST(n_regionkey AS BOOLEAN) THEN TRUE ELSE FALSE END ORDER BY n_nationkey`},
		pgCase{name: "BareCastInJoinOn",
			sql: `SELECT n.n_nationkey AS k FROM nation n JOIN region r
				ON n.n_regionkey = r.r_regionkey AND CAST(n.n_nationkey AS BOOLEAN) ORDER BY k`},
		// HAVING over a bare boolean of a GROUPED column. `logical.rewriteExpr`
		// took EVERY function call in a HAVING for an aggregate and rewrote it
		// to a column named after the call's rendered text, so a HAVING naming
		// no aggregate at all silently admitted NO GROUP.
		pgCase{name: "BareCastInHaving",
			sql: `SELECT n_regionkey AS r, COUNT(*) AS n FROM nation GROUP BY n_regionkey
				HAVING CAST(n_regionkey AS BOOLEAN) ORDER BY r`},
		pgCase{name: "BareCoalesceInHaving",
			sql: `SELECT n_regionkey AS r, COUNT(*) AS n FROM nation GROUP BY n_regionkey
				HAVING COALESCE(CAST(NULLIF(n_regionkey, 1) AS BOOLEAN), FALSE) ORDER BY r`},
		pgCase{name: "BareFuncCallInHaving",
			sql: `SELECT n_regionkey AS r, COUNT(*) AS n FROM nation GROUP BY n_regionkey
				HAVING ABS(n_regionkey) > 1 ORDER BY r`},

		// A boolean LITERAL as the whole predicate, both ways round.
		pgCase{name: "BareCastLiteralTrue",
			sql: `SELECT COUNT(*) AS n FROM nation WHERE CAST(1 AS BOOLEAN)`},
		pgCase{name: "BareCastLiteralFalse",
			sql: `SELECT COUNT(*) AS n FROM nation WHERE CAST(0 AS BOOLEAN)`},
		pgCase{name: "BareCastLiteralNull",
			sql: `SELECT COUNT(*) AS n FROM nation WHERE CAST(NULL AS BOOLEAN)`},
	)

	// PostgreSQL's boolean INPUT function, which is what `text::boolean` runs
	// (parse_bool_with_len, src/backend/utils/adt/bool.c). The prefix rule is
	// the part a hand-rolled reader gets wrong: any non-empty prefix of
	// "true"/"false"/"yes"/"no" is a value, and `'o'` alone is an ERROR
	// because it cannot choose between "on" and "off". Asked of the engine
	// that defines it rather than pinned to a table written from memory.
	//
	// The REFUSALS ('garbage', 'o', '2') are not here: PostgreSQL answers
	// those with 22P02 and this arm fatals on an oracle error by design, so
	// the error half is gated in wadjet.TestCastTextToBooleanIsPostgresBoolin
	// against the same transcript.
	out = append(out,
		pgCase{name: "CastTextToBooleanCanonical",
			sql: `SELECT CAST('t' AS BOOLEAN) AS a, CAST('true' AS BOOLEAN) AS b,
				CAST('yes' AS BOOLEAN) AS c, CAST('on' AS BOOLEAN) AS d, CAST('1' AS BOOLEAN) AS e,
				CAST('f' AS BOOLEAN) AS g, CAST('false' AS BOOLEAN) AS h,
				CAST('no' AS BOOLEAN) AS i, CAST('off' AS BOOLEAN) AS j, CAST('0' AS BOOLEAN) AS k`},
		pgCase{name: "CastTextToBooleanPrefixesAndCase",
			sql: `SELECT CAST('tr' AS BOOLEAN) AS a, CAST('tru' AS BOOLEAN) AS b,
				CAST('fa' AS BOOLEAN) AS c, CAST('fals' AS BOOLEAN) AS d,
				CAST('ye' AS BOOLEAN) AS e, CAST('n' AS BOOLEAN) AS f, CAST('of' AS BOOLEAN) AS g,
				CAST('TRUE' AS BOOLEAN) AS h, CAST('Off' AS BOOLEAN) AS i,
				CAST('  true  ' AS BOOLEAN) AS j`},
		pgCase{name: "CastNullToBoolean", sql: `SELECT CAST(NULL AS BOOLEAN) AS b`},
	)

	// --- #622 follow-on: a disjunction with an IS NOT NULL branch ---------
	//
	// A vectorized OR branch that matches EVERY row returns the batch with a
	// nil selection vector (the "all rows" convention); OrFilter read that as
	// ZERO rows, so `col IS NULL OR col IS NOT NULL` over a column with no
	// nulls answered nothing. PostgreSQL is the authority: the predicate is a
	// tautology and admits every row. Found while building the #622 gate,
	// fixed in exec.OrFilter.
	out = append(out,
		pgCase{name: "DisjunctiveNullCheckIsTautology",
			sql: `SELECT COUNT(*) AS c FROM nation WHERE n_comment IS NULL OR n_comment IS NOT NULL`},
		pgCase{name: "DisjunctiveNullCheckIsTautologyReversed",
			sql: `SELECT COUNT(*) AS c FROM nation WHERE n_name IS NOT NULL OR n_name IS NULL`},
		pgCase{name: "DisjunctiveNullCheckSelectsEveryRow",
			sql: `SELECT n_nationkey FROM nation WHERE n_name IS NULL OR n_name IS NOT NULL ORDER BY n_nationkey`},
		pgCase{name: "DisjunctionWithAllRowsBranch",
			sql: `SELECT COUNT(*) AS c FROM nation WHERE n_nationkey = 3 OR n_name IS NOT NULL`},

		// The #622 theme in a shape PostgreSQL accepts: an aggregate over an
		// OUTER JOIN's null-padded rows must not move when an always-true
		// filter is added. TPC-H has no colliding column names, so the
		// qualifier-strip half of #622 lives in the coordinator two-path
		// gate; here the null-padding + always-true-filter half is checked
		// against live PostgreSQL. The ON's equality is hash/merge-joinable,
		// so PostgreSQL accepts the FULL JOIN (unlike the soak's bare-boolean
		// ON); the o_orderkey<0 conjunct makes every row unmatched.
		pgCase{name: "FullOuterAllUnmatchedAggregate",
			sql: `SELECT COUNT(o.o_orderkey) AS matched, COUNT(*) AS total
				FROM customer c FULL OUTER JOIN orders o
				ON c.c_custkey = o.o_custkey AND o.o_orderkey < 0`},
		pgCase{name: "LeftOuterAllUnmatchedAggregate",
			sql: `SELECT COUNT(o.o_orderkey) AS matched, MIN(o.o_totalprice) AS mn, MAX(o.o_totalprice) AS mx
				FROM customer c LEFT JOIN orders o
				ON c.c_custkey = o.o_custkey AND o.o_orderkey < 0`},
		pgCase{name: "LeftOuterAllUnmatchedAggregateAlwaysTrueFilter",
			sql: `SELECT COUNT(o.o_orderkey) AS matched, MIN(o.o_totalprice) AS mn, MAX(o.o_totalprice) AS mx
				FROM customer c LEFT JOIN orders o
				ON c.c_custkey = o.o_custkey AND o.o_orderkey < 0
				WHERE c.c_name IS NULL OR c.c_name IS NOT NULL`},
	)

	out = append(out, groupKeySpellingCases()...)

	return out
}

// groupKeySpellingCases is the #720/#723/#725 family: ONE computed GROUP BY
// key, said every way SQL allows, and every consumer that has to recognise it.
//
// PostgreSQL is the authority here for a reason the DuckDB arm cannot supply.
// The engine used to match a SELECT item to its group key by the rendered TEXT
// of the two spellings, so which parentheses the query carried decided whether
// the key column came back with values or with NULL on every row, and a HAVING
// over a computed key answered no rows at all. PostgreSQL matches by expression
// equivalence, which is what makes every entry below one question with one
// answer.
//
// Every entry aliases its computed item. An UNALIASED one would additionally
// pin the output column's NAME, which diverges for a reason of its own (#732),
// and this family is about which VALUE the item carries.
func groupKeySpellingCases() []pgCase {
	// The key is `l_partkey + 1` over a narrow slice of lineitem — small
	// enough to compare row for row, wide enough to have many groups.
	const where = `WHERE l_orderkey < 200`
	sel := func(item, groupBy string) string {
		return `SELECT ` + item + ` AS gk, COUNT(*) AS n, SUM(l_quantity) AS q FROM lineitem ` +
			where + ` GROUP BY ` + groupBy + ` ORDER BY gk`
	}
	var out []pgCase
	add := func(name, sql string) { out = append(out, pgCase{name: name, sql: sql, ordered: true}) }

	// The SELECT item against the GROUP BY term: parentheses on either side,
	// on both, nested, and identifier case in the item.
	add("GroupKeyPlain", sel(`l_partkey + 1`, `l_partkey + 1`))
	add("GroupKeyWhitespace", sel(`l_partkey+1`, `l_partkey + 1`))
	add("GroupKeyParenOnTheSelect", sel(`(l_partkey + 1)`, `l_partkey + 1`))
	add("GroupKeyParenOnTheGroupBy", sel(`l_partkey + 1`, `(l_partkey + 1)`))
	add("GroupKeyParenOnBoth", sel(`(l_partkey + 1)`, `(l_partkey + 1)`))
	add("GroupKeyParenNested", sel(`((l_partkey) + 1)`, `l_partkey + 1`))
	add("GroupKeyIdentifierCase", sel(`L_PARTKEY + 1`, `l_partkey + 1`))

	// Associativity is NOT spelling: these are two expressions, two keys, and
	// an identity that erased parentheses by printing without them would make
	// them one.
	add("GroupKeyLeftAssociative", sel(`l_partkey - 1 - 2`, `l_partkey - 1 - 2`))
	add("GroupKeyRightAssociative", sel(`l_partkey - (1 - 2)`, `l_partkey - (1 - 2)`))

	// ORDER BY the grouping expression rather than the alias, spelled both
	// ways. `ORDER BY (l_partkey + 1)` was refused outright.
	add("GroupKeyOrderByTheExpression",
		`SELECT l_partkey + 1 AS gk, COUNT(*) AS n FROM lineitem `+where+
			` GROUP BY l_partkey + 1 ORDER BY l_partkey + 1`)
	add("GroupKeyOrderByTheParenthesisedExpression",
		`SELECT l_partkey + 1 AS gk, COUNT(*) AS n FROM lineitem `+where+
			` GROUP BY l_partkey + 1 ORDER BY (l_partkey + 1)`)

	// HAVING over the computed key — #720, which answered ZERO rows on both
	// execution paths — in every spelling, alone and beside an aggregate
	// predicate, and with the key inside an aggregate where it must NOT be
	// re-pointed at the grouped column.
	having := func(name, pred string) {
		add(name, `SELECT l_partkey + 1 AS gk, COUNT(*) AS n FROM lineitem `+where+
			` GROUP BY l_partkey + 1 HAVING `+pred+` ORDER BY gk`)
	}
	having("GroupKeyHaving", `l_partkey + 1 > 100`)
	having("GroupKeyHavingParenthesised", `(l_partkey + 1) > 100`)
	having("GroupKeyHavingIdentifierCase", `L_PARTKEY + 1 > 100`)
	having("GroupKeyHavingNegated", `NOT (l_partkey + 1 > 100)`)
	having("GroupKeyHavingConjunction", `l_partkey + 1 > 100 AND COUNT(*) > 1`)
	having("GroupKeyHavingIsNotNull", `l_partkey + 1 IS NOT NULL`)
	having("GroupKeyHavingCast", `CAST(l_partkey + 1 AS BIGINT) > 100`)
	having("GroupKeyHavingOverAnAggregateOfTheKey", `SUM(l_partkey + 1) > 100`)
	having("GroupKeyHavingAggregateOnly", `COUNT(*) > 1`)

	// An expression OVER the key rather than the key itself, and one that
	// mixes the key with an aggregate — the shape the gather EVALUATES.
	add("GroupKeyExpressionOverTheKey",
		`SELECT (l_partkey + 1) * 2 AS gk, COUNT(*) AS n FROM lineitem `+where+
			` GROUP BY l_partkey + 1 ORDER BY gk`)
	add("GroupKeyFunctionOverTheKey",
		`SELECT ABS(l_partkey + 1) AS gk, COUNT(*) AS n FROM lineitem `+where+
			` GROUP BY l_partkey + 1 ORDER BY gk`)
	add("GroupKeyPlusAnAggregate",
		`SELECT (l_partkey + 1) + COUNT(*) AS x FROM lineitem `+where+
			` GROUP BY l_partkey + 1 ORDER BY x`)
	add("GroupKeyCaseOverTheKey",
		`SELECT CASE WHEN l_partkey + 1 > 100 THEN 'hi' ELSE 'lo' END AS b, COUNT(*) AS n
			FROM lineitem `+where+` GROUP BY l_partkey + 1 ORDER BY b, n`)

	// TWO computed keys, in either order, with the items spelled differently
	// from both.
	add("GroupKeyTwoComputed",
		`SELECT l_partkey + 1 AS a, l_suppkey * 2 AS b, COUNT(*) AS n FROM lineitem `+where+
			` GROUP BY l_partkey + 1, l_suppkey * 2 ORDER BY a, b`)
	add("GroupKeyTwoComputedParenthesised",
		`SELECT (l_partkey + 1) AS a, (l_suppkey * 2) AS b, COUNT(*) AS n FROM lineitem `+where+
			` GROUP BY l_partkey + 1, l_suppkey * 2 ORDER BY a, b`)
	add("GroupKeyTwoComputedReordered",
		`SELECT l_partkey + 1 AS a, l_suppkey * 2 AS b, COUNT(*) AS n FROM lineitem `+where+
			` GROUP BY l_suppkey * 2, l_partkey + 1 ORDER BY a, b`)

	// A key over a DECIMAL, a DATE and a STRING expression. Under the DECIMAL
	// fixture the first is exact fixed-point arithmetic and the comparison is
	// digit for digit.
	out = append(out, pgCase{name: "GroupKeyOverDecimal", ordered: true, exactNumeric: true,
		sql: `SELECT (l_extendedprice + 1) AS gk, COUNT(*) AS n FROM lineitem ` + where +
			` GROUP BY l_extendedprice + 1 ORDER BY gk`},
		// An expression OVER a DECIMAL key. The key's own (p,s) has to reach
		// the term written above it, or the whole term falls to the float
		// rule and renders exact fixed point through a float64 — right
		// digits, wrong number of them.
		pgCase{name: "GroupKeyDecimalExpressionOverTheKey", ordered: true, exactNumeric: true,
			sql: `SELECT (l_extendedprice + 1) * 2 AS gk, COUNT(*) AS n FROM lineitem ` + where +
				` GROUP BY l_extendedprice + 1 ORDER BY gk`},
		pgCase{name: "GroupKeyDecimalHavingOverTheKey", ordered: true, exactNumeric: true,
			sql: `SELECT (l_extendedprice + 1) AS gk, COUNT(*) AS n FROM lineitem ` + where +
				` GROUP BY l_extendedprice + 1 HAVING (l_extendedprice + 1) * 2 > 40000 ORDER BY gk`})
	// l_shipdate is a TEXT column in this fixture, so the DATE key is the
	// CAST — which is also the shape that types the key from something other
	// than its input column.
	add("GroupKeyOverDateCast",
		`SELECT (CAST(l_shipdate AS DATE)) AS d, COUNT(*) AS n FROM lineitem `+where+
			` GROUP BY CAST(l_shipdate AS DATE) ORDER BY d`)
	add("GroupKeyOverSubstr",
		`SELECT (SUBSTR(l_shipinstruct, 1, 6)) AS p, COUNT(*) AS n FROM lineitem `+where+
			` GROUP BY SUBSTR(l_shipinstruct, 1, 6) ORDER BY p`)
	add("GroupKeyOverConcat",
		`SELECT (l_returnflag || l_linestatus) AS k, COUNT(*) AS n FROM lineitem `+where+
			` GROUP BY l_returnflag || l_linestatus ORDER BY k`)

	// An outer reference to the key through a derived table and through a CTE,
	// with the two spellings deliberately different.
	add("GroupKeyDerivedTableOuterReference",
		`SELECT s.gk, s.n FROM (SELECT (l_partkey + 1) AS gk, COUNT(*) AS n FROM lineitem `+where+
			` GROUP BY l_partkey + 1) s WHERE s.gk > 100 ORDER BY s.gk`)
	add("GroupKeyCTEOuterReference",
		`WITH a AS (SELECT l_partkey + 1 AS gk, COUNT(*) AS n FROM lineitem `+where+
			` GROUP BY (l_partkey + 1)) SELECT gk, n FROM a WHERE gk > 100 ORDER BY gk`)
	add("GroupKeyCTEHavingThenOuterFilter",
		`WITH a AS (SELECT l_partkey + 1 AS gk, COUNT(*) AS n FROM lineitem `+where+
			` GROUP BY l_partkey + 1 HAVING l_partkey + 1 > 100) SELECT gk, n FROM a `+
			`WHERE gk > 150 ORDER BY gk`)

	// #725: a DELIMITED identifier that is itself valid arithmetic. The key
	// is a NAME, and the DAG shipped it to the worker with its quotes.
	add("GroupKeyDelimitedIdentifier",
		`SELECT "l_partkey + 1" AS gk, COUNT(*) AS n FROM `+
			`(SELECT l_partkey + 1 AS "l_partkey + 1" FROM lineitem `+where+`) s `+
			`GROUP BY "l_partkey + 1" ORDER BY gk`)
	add("GroupKeyDelimitedIdentifierHaving",
		`SELECT "l_partkey + 1" AS gk, COUNT(*) AS n FROM `+
			`(SELECT l_partkey + 1 AS "l_partkey + 1" FROM lineitem `+where+`) s `+
			`GROUP BY "l_partkey + 1" HAVING "l_partkey + 1" > 100 ORDER BY gk`)
	// The same delimited name over a column of a DIFFERENT type, resolved as
	// a SELECT-list ALIAS: PostgreSQL prefers an output name over an input
	// one in GROUP BY, and the alias is what this names.
	add("GroupKeyDelimitedAliasOverAnotherType",
		`SELECT l_returnflag AS "l_partkey + 1", COUNT(*) AS n FROM lineitem `+where+
			` GROUP BY "l_partkey + 1" ORDER BY 1`)

	// The COLLISION family: a relation carrying a column SPELLED like the
	// group key. The key is materialized into a reserved slot and published
	// by a rename, so the two cannot see each other; naming the materialized
	// column after the key made the INPUT column win on one engine and the
	// KEY win on the other (ADR-0026).
	coll := `(SELECT l_partkey, l_quantity AS "l_partkey + 1" FROM lineitem ` + where + `) s`
	add("GroupKeyCollidesWithAnInputColumn",
		`SELECT l_partkey + 1 AS k, COUNT(*) AS n FROM `+coll+` GROUP BY l_partkey + 1 ORDER BY k`)
	add("GroupKeyCollidesWithAnInputColumnKeyOnly",
		`SELECT l_partkey + 1 AS k FROM `+coll+` GROUP BY l_partkey + 1 ORDER BY k`)
	add("GroupKeyCollidesWithAnInputColumnHaving",
		`SELECT l_partkey + 1 AS k, COUNT(*) AS n FROM `+coll+
			` GROUP BY l_partkey + 1 HAVING l_partkey + 1 > 100 ORDER BY k`)
	add("GroupKeyCollidesWithAnInputColumnAggregated",
		`SELECT l_partkey + 1 AS k, MAX("l_partkey + 1") AS m FROM `+coll+
			` GROUP BY l_partkey + 1 ORDER BY k`)
	add("GroupKeyCollidesWithAnInputColumnCTE",
		`WITH a AS (SELECT l_partkey, l_quantity AS "l_partkey + 1" FROM lineitem `+where+`) `+
			`SELECT l_partkey + 1 AS k, COUNT(*) AS n FROM a GROUP BY l_partkey + 1 ORDER BY k`)
	add("GroupKeyCollidesOverAString",
		`SELECT l_returnflag || 'x' AS k, COUNT(*) AS n FROM `+
			`(SELECT l_returnflag, l_comment AS "l_returnflag || 'x'" FROM lineitem `+where+`) s `+
			`GROUP BY l_returnflag || 'x' ORDER BY k`)
	// The other direction: an AGGREGATE whose alias is spelled like the key.
	add("GroupKeyCollidesWithAnAggregateAlias",
		`SELECT l_partkey + 1 AS k, COUNT(*) AS "l_partkey + 1" FROM lineitem `+where+
			` GROUP BY l_partkey + 1 ORDER BY k`)
	add("GroupKeyCollidesWithAnAggregateAliasOtherCase",
		`SELECT l_partkey + 1 AS k, COUNT(*) AS "L_PARTKEY + 1" FROM lineitem `+where+
			` GROUP BY l_partkey + 1 ORDER BY k`)

	// A derived key over a RENAMED column: the DAG emits no stage for a
	// rename Project, so the key reached the worker spelled over a name the
	// scan does not emit and every row landed in ONE NULL group.
	add("GroupKeyOverARenamedColumn",
		`SELECT pk + 1 AS k, COUNT(*) AS n FROM (SELECT l_partkey AS pk FROM lineitem `+where+
			`) s GROUP BY pk + 1 ORDER BY k`)
	add("GroupKeyOverARenamedColumnCTE",
		`WITH s AS (SELECT l_partkey AS pk FROM lineitem `+where+`) `+
			`SELECT pk + 1 AS k, COUNT(*) AS n FROM s GROUP BY pk + 1 ORDER BY k`)
	add("GroupKeyOverARenamedDelimitedColumn",
		`SELECT "p k" + 1 AS k, COUNT(*) AS n FROM (SELECT l_partkey AS "p k" FROM lineitem `+
			where+`) s GROUP BY "p k" + 1 ORDER BY k`)

	// The SLOT the planner materializes a derived key into, over a relation
	// that already carries a column of that name. A stored or aliased name in
	// the reserved namespace is never refused at read, so the slot moves —
	// and the allocator must exclude the slots it has ALREADY ISSUED as well
	// as the names in scope, or two derived keys land in one column and the
	// second silently carries the first one's value.
	slotColl := `(SELECT l_partkey, l_suppkey, 1 AS "__gb_expr_0" FROM lineitem ` + where + `) s`
	add("GroupKeySlotCollidesTwoDerivedKeys",
		`SELECT l_partkey + 1 AS a, l_suppkey * 2 AS b, COUNT(*) AS n FROM `+slotColl+
			` GROUP BY l_partkey + 1, l_suppkey * 2 ORDER BY a, b`)
	add("GroupKeySlotCollidesReversedOrder",
		`SELECT l_partkey + 1 AS a, l_suppkey * 2 AS b, COUNT(*) AS n FROM `+slotColl+
			` GROUP BY l_suppkey * 2, l_partkey + 1 ORDER BY a, b`)
	add("GroupKeySlotCollidesThreeDerivedKeys",
		`SELECT l_partkey + 1 AS a, l_suppkey * 2 AS b, l_linenumber + 3 AS c, COUNT(*) AS n `+
			`FROM (SELECT l_partkey, l_suppkey, l_linenumber, 1 AS "__gb_expr_0" FROM lineitem `+
			where+`) s GROUP BY l_partkey + 1, l_suppkey * 2, l_linenumber + 3 ORDER BY a, b, c`)
	add("GroupKeySlotCollidesWithHaving",
		`SELECT l_partkey + 1 AS a, l_suppkey * 2 AS b, COUNT(*) AS n FROM `+slotColl+
			` GROUP BY l_partkey + 1, l_suppkey * 2 HAVING l_partkey + 1 > 100 ORDER BY a, b`)
	add("GroupKeySlotCollidesAggregateOverTheColumn",
		`SELECT l_partkey + 1 AS a, SUM("__gb_expr_0") AS s FROM `+slotColl+
			` GROUP BY l_partkey + 1 ORDER BY a`)

	// An ARITHMETIC key beside a DELIMITED COLUMN spelled the same way. The
	// recorded key text cannot tell them apart, so the arithmetic key was
	// resolved as a NAME and bound to the column. The two must answer
	// DIFFERENTLY.
	arith := `(SELECT l_quantity AS q, l_partkey AS "q + 1" FROM lineitem ` + where + `) s`
	add("GroupKeyArithmeticBesideADelimitedColumnOfThatText",
		`SELECT q + 1 AS k, COUNT(*) AS n FROM `+arith+` GROUP BY q + 1 ORDER BY k`)
	add("GroupKeyDelimitedColumnBesideThatArithmetic",
		`SELECT "q + 1" AS k, COUNT(*) AS n FROM `+arith+` GROUP BY "q + 1" ORDER BY k`)
	add("GroupKeyArithmeticBesideADelimitedColumnHaving",
		`SELECT q + 1 AS k, COUNT(*) AS n FROM `+arith+
			` GROUP BY q + 1 HAVING q + 1 > 20 ORDER BY k`)

	// A DELIMITED ALIAS under a POSITIONAL ORDER BY: the alias's case is part
	// of its name, and `ORDER BY 1` must resolve to the item and not to the
	// alias's TEXT re-parsed as an expression.
	add("OrderByOrdinalOverAnArithmeticLookingAlias",
		`SELECT l_partkey AS "L + 1" FROM lineitem `+where+` ORDER BY 1`)
	add("OrderByOrdinalOverAMixedCaseAlias",
		`SELECT l_partkey AS "Pk" FROM lineitem `+where+` ORDER BY 1`)
	add("OrderByOrdinalOverASpacedAlias",
		`SELECT l_partkey AS "P k" FROM lineitem `+where+` ORDER BY 1`)
	add("OrderByOrdinalOverAKeywordAlias",
		`SELECT l_partkey AS "select" FROM lineitem `+where+` ORDER BY 1`)
	add("OrderByOrdinalDescendingOverAMixedCaseAlias",
		`SELECT l_partkey AS "Pk" FROM lineitem `+where+` ORDER BY 1 DESC`)
	add("OrderBySecondOrdinalOverAMixedCaseAlias",
		`SELECT l_partkey AS "Pk", l_suppkey FROM lineitem `+where+` ORDER BY 2, 1`)
	add("OrderByTheDelimitedAliasItself",
		`SELECT l_partkey AS "Pk" FROM lineitem `+where+` ORDER BY "Pk"`)
	add("OrderByOrdinalOverAGroupedMixedCaseAlias",
		`SELECT l_partkey AS "Pk", COUNT(*) AS n FROM lineitem `+where+
			` GROUP BY l_partkey ORDER BY 1`)

	// DISTINCT and an outer aggregate over a derived key: two aggregates
	// keyed alike, where the outer one reads the inner one's OUTPUT.
	add("GroupKeyDistinctOverTheKey",
		`SELECT DISTINCT l_partkey + 1 AS k FROM lineitem `+where+
			` GROUP BY l_partkey + 1 ORDER BY k`)
	out = append(out,
		pgCase{name: "GroupKeyOuterSumOverTheKey",
			sql: `SELECT SUM(k) AS s, COUNT(*) AS n FROM (SELECT l_partkey + 1 AS k, ` +
				`COUNT(*) AS c FROM lineitem ` + where + ` GROUP BY l_partkey + 1) s`},
		pgCase{name: "GroupKeyOuterMaxOverTheKey",
			sql: `SELECT MAX(k) AS m FROM (SELECT l_partkey + 1 AS k, COUNT(*) AS c ` +
				`FROM lineitem ` + where + ` GROUP BY l_partkey + 1) s`})

	return out
}

// multiKeyCorrelatedCases is the #562 family: correlated EXISTS / NOT EXISTS /
// IN / NOT IN keyed on MORE THAN ONE column.
//
// Every other correlated entry in this corpus — and in the two-path corpus,
// the DuckDB corpus, the type matrix and the shape fuzzer — correlates on
// exactly ONE equality. #562 is what that blind spot cost: a two-column
// correlated EXISTS answered zero rows and its NOT EXISTS twin answered every
// row, because the build side's NDV narrowing read the join keys out of the
// condition text with a split on " and " while a decorrelation renders
// " AND ", kept the first conjunct's key and projected away the column the
// second conjunct compares.
//
// PostgreSQL is the only authority that could see it. The defect is in the
// LOGICAL optimizer, so wadjet's two execution paths agreed on the wrong
// answer, and 0 rows from a COUNT is not a shape a self-comparison can call
// wrong.
//
// The queries and the fixture come from internal/oracle/multikey, so this arm
// and the in-process gates (wadjet.TestMultiKeyCorrelatedSubqueries,
// coordinator.TestMultiKeyCorrelatedTwoPath) ask exactly the same questions —
// those two compare against the constants this arm re-derives live.
func multiKeyCorrelatedCases() []pgCase {
	// The pins come from the corpus itself (multikey.Case.KnownBug/Issue), not
	// from a second list here. Two arms already consume those fields, and a
	// duplicate map is a pin that can go stale in one place and not the other
	// — which it promptly did: the distinct-name arm's #577/#578 entries were
	// pinned in the corpus and unpinned here, and this arm failed on them.
	out := make([]pgCase, 0, 80)
	for _, c := range multikey.Corpus() {
		pc := pgCase{name: "MultiKey_" + c.Name, sql: c.SQL}
		if c.KnownBug != "" {
			pc.knownBug, pc.issue = pgBugWadjet+" "+c.KnownBug, c.Issue
		}
		out = append(out, pc)
	}
	return out
}

// postgresBytesCases is the BYTES/bytea family (#570). Before it, `bytea`
// appeared nowhere in benchmarks/ or internal/oracle/ at all: the TPC-H
// fixture has no BYTES column, so this arm had never compared a BYTES VALUE
// and the wire arm had never compared its OID. The wire half is where the
// defect lived; this half is what proves the ENGINE's answers were already
// PostgreSQL's, so the wire fix did not have to move them.
//
// Every entry is a rule PostgreSQL defines for bytea:
//
//   - comparison and ordering are BYTEWISE, in every collation (no COLLATE
//     applies to bytea), which is what wadjet does for BYTES anyway;
//   - an unknown-typed literal beside a bytea column is coerced by byteain,
//     so `b_val = 'hi'` is the two bytes and not the two characters of some
//     other type — the same reading wadjet gives it;
//   - `~~` over bytea EXISTS and is bytewise (verified live:
//     `'\xfffe0041'::bytea LIKE '%A%'` is true, matching the 0x41 BYTE),
//     which is why LIKE and `::text` disagree for this one type;
//   - `bytea::text` is `\x` plus lowercase hex under bytea_output = hex.
//
// MIN/MAX over a bare bytea column is deliberately absent: PostgreSQL has no
// min(bytea)/max(bytea) at all (verified live — "function min(bytea) does
// not exist", the same shape as min(boolean)), so asking for one would make
// the ORACLE refuse the entry rather than compare anything. The aggregate is
// exercised over the CAST instead, where PostgreSQL does have an answer.
func postgresBytesCases() []pgCase {
	return []pgCase{
		// The VALUE itself, straight through. pgx decodes bytea to []byte
		// and wadjet boxes BYTES as []byte, and the fingerprint digests both
		// as their raw bytes — so this compares the values and not two
		// renderings of them.
		pgCase{name: "BytesProjection", sql: `SELECT b_key, b_val FROM bytea_probe ORDER BY b_key`},
		pgCase{name: "BytesProjectionBothColumns", sql: `SELECT b_key, b_val, b_other FROM bytea_probe ORDER BY b_key`},

		// Ordering, both directions. NULL placement is item 5's rule and the
		// non-NULL order is bytewise: the empty value first, 0xff last.
		pgCase{name: "BytesOrderBy", sql: `SELECT b_key FROM bytea_probe ORDER BY b_val, b_key`},
		pgCase{name: "BytesOrderByDesc", sql: `SELECT b_key FROM bytea_probe ORDER BY b_val DESC, b_key`},
		pgCase{name: "BytesOrderByValues", sql: `SELECT b_val FROM bytea_probe ORDER BY b_val, b_key`},

		// Equality and the rest of the operator set against a literal. 'hi'
		// and 'Hi' differ only in case, which a byte comparison separates and
		// a locale one does not.
		pgCase{name: "BytesEq", sql: `SELECT b_key FROM bytea_probe WHERE b_val = 'hi' ORDER BY b_key`},
		pgCase{name: "BytesEqCaseDiffers", sql: `SELECT b_key FROM bytea_probe WHERE b_val = 'Hi' ORDER BY b_key`},
		pgCase{name: "BytesEqEmpty", sql: `SELECT b_key FROM bytea_probe WHERE b_val = '' ORDER BY b_key`},
		pgCase{name: "BytesNe", sql: `SELECT b_key FROM bytea_probe WHERE b_val <> 'hi' ORDER BY b_key`},
		pgCase{name: "BytesLt", sql: `SELECT b_key FROM bytea_probe WHERE b_val < 'hi' ORDER BY b_key`},
		pgCase{name: "BytesLe", sql: `SELECT b_key FROM bytea_probe WHERE b_val <= 'hi' ORDER BY b_key`},
		pgCase{name: "BytesGt", sql: `SELECT b_key FROM bytea_probe WHERE b_val > 'hi' ORDER BY b_key`},
		pgCase{name: "BytesGe", sql: `SELECT b_key FROM bytea_probe WHERE b_val >= 'hi' ORDER BY b_key`},
		// A PREFIX of another value: 'hi' < 'hi there', which is the one
		// ordering rule a length-first comparison would get wrong.
		pgCase{name: "BytesPrefixOrder", sql: `SELECT b_key FROM bytea_probe WHERE b_val > 'hi' AND b_val < 'wadjet' ORDER BY b_key`},
		pgCase{name: "BytesIn", sql: `SELECT b_key FROM bytea_probe WHERE b_val IN ('hi', 'A', '') ORDER BY b_key`},
		pgCase{name: "BytesBetween", sql: `SELECT b_key FROM bytea_probe WHERE b_val BETWEEN 'A' AND 'hi' ORDER BY b_key`},

		// NULL, and the empty value beside it: two different answers a wrong
		// NULL representation collapses into one.
		pgCase{name: "BytesIsNull", sql: `SELECT b_key FROM bytea_probe WHERE b_val IS NULL ORDER BY b_key`},
		pgCase{name: "BytesIsNotNull", sql: `SELECT b_key FROM bytea_probe WHERE b_val IS NOT NULL ORDER BY b_key`},
		pgCase{name: "BytesIsDistinctFrom", sql: `SELECT b_key FROM bytea_probe WHERE b_val IS DISTINCT FROM 'hi' ORDER BY b_key`},
		pgCase{name: "BytesCoalesce", sql: `SELECT b_key, COALESCE(b_val, b_other) AS v FROM bytea_probe ORDER BY b_key`},

		// Column against column, which takes a different path from column
		// against literal in every type family this ADR has had to fix.
		pgCase{name: "BytesColColEq", sql: `SELECT b_key FROM bytea_probe WHERE b_val = b_other ORDER BY b_key`},
		pgCase{name: "BytesColColLt", sql: `SELECT b_key FROM bytea_probe WHERE b_val < b_other ORDER BY b_key`},
		pgCase{name: "BytesJoinOnBytes",
			sql: `SELECT a.b_key AS ak, b.b_key AS bk FROM bytea_probe a JOIN bytea_probe b ON a.b_val = b.b_other ORDER BY ak, bk`},

		// The value as a GROUP BY / DISTINCT key, which is the serialized-key
		// path rather than the comparison one.
		pgCase{name: "BytesGroupBy", sql: `SELECT b_val, COUNT(*) AS n FROM bytea_probe GROUP BY b_val ORDER BY b_val`},
		pgCase{name: "BytesDistinct", sql: `SELECT DISTINCT b_val FROM bytea_probe ORDER BY b_val`},
		pgCase{name: "BytesCountDistinct", sql: `SELECT COUNT(DISTINCT b_val) AS n FROM bytea_probe`},

		// LENGTH/OCTET_LENGTH over bytea are the OCTET count in PostgreSQL,
		// not a character count — there are no characters.
		pgCase{name: "BytesLength", sql: `SELECT b_key, LENGTH(b_val) AS n, OCTET_LENGTH(b_val) AS o FROM bytea_probe ORDER BY b_key`},

		// The rendering #570 changed, asked of the engine that defines it.
		pgCase{name: "BytesCastText", sql: `SELECT b_key, CAST(b_val AS text) AS s FROM bytea_probe ORDER BY b_key`},
		pgCase{name: "BytesCastTextOrder", sql: `SELECT CAST(b_val AS text) AS s FROM bytea_probe ORDER BY s, b_key`},
		// MIN/MAX over the cast, since PostgreSQL has no min(bytea): the
		// aggregate still runs over BYTES-derived text on both sides.
		pgCase{name: "BytesMinMaxOverCast",
			sql: `SELECT MIN(CAST(b_val AS text)) AS lo, MAX(CAST(b_val AS text)) AS hi FROM bytea_probe`},

		// `~~` over bytea is BYTEWISE, so the pattern matches the 0x41 BYTE
		// in 0xff 0xfe 0x00 0x41 and not a hex digit of its \x spelling.
		// This is why CAST and LIKE deliberately disagree for BYTES.
		pgCase{name: "BytesLike", sql: `SELECT b_key FROM bytea_probe WHERE b_val LIKE '%A%' ORDER BY b_key`},
		pgCase{name: "BytesLikePrefix", sql: `SELECT b_key FROM bytea_probe WHERE b_val LIKE 'h%' ORDER BY b_key`},
		pgCase{name: "BytesNotLike", sql: `SELECT b_key FROM bytea_probe WHERE b_val NOT LIKE '%A%' ORDER BY b_key`},

		// The remaining half of #570, on the way IN. An unknown-typed
		// literal beside a bytea column is read by byteain in PostgreSQL, so
		// the HEX spelling names the same two bytes `'hi'` does. wadjet has
		// no BYTES literal — the lexer produces a string and nothing
		// re-types it from the column's declaration — so the six characters
		// are compared against two bytes and match nothing. Pinned, not
		// exempted: the day a bytea literal lands, this entry fails.
		pgCase{name: "BytesEqHexSpelledLiteral",
			sql: `SELECT b_key FROM bytea_probe WHERE b_val = '\x6869' ORDER BY b_key`,
			knownBug: pgBugWadjet + " PostgreSQL's byteain reads an unknown-typed literal beside a " +
				"bytea column, so '\\x6869' is the two bytes 0x68 0x69 and this finds the row. wadjet " +
				"has no BYTES literal at all: the lexer produces a string, nothing re-types it from the " +
				"column's declaration, and the SIX characters match nothing. The bytea Bind PARAMETER " +
				"path does decode both spellings (it has the declared OID to decode against); a " +
				"hand-written literal has no declaration to consult",
			issue: "#582"},
	}
}

// pgHavingRows is the six-row fixture the #590/#591 entries above are written
// over, as a derived table both engines parse identically. It reproduces the
// tables those issues minimized to: group 1 has both boolean values and two
// non-NULL v, group 2 is all-TRUE with every v NULL (so MAX(v) is NULL for
// exactly one group), group 3's only flag is NULL (so BOOL_OR and BOOL_AND
// are UNKNOWN there and the `p IS NULL` arm of a TLP partition is non-empty),
// and group 4 is all-FALSE. Four groups, and no two aggregates agree about
// which of them they select.
const pgHavingRows = `
	SELECT 1 AS k, CAST(TRUE AS BOOLEAN) AS flag, CAST(10 AS BIGINT) AS v
	UNION ALL SELECT 1, CAST(FALSE AS BOOLEAN), CAST(20 AS BIGINT)
	UNION ALL SELECT 2, CAST(TRUE AS BOOLEAN), CAST(NULL AS BIGINT)
	UNION ALL SELECT 2, CAST(TRUE AS BOOLEAN), CAST(NULL AS BIGINT)
	UNION ALL SELECT 3, CAST(NULL AS BOOLEAN), CAST(5 AS BIGINT)
	UNION ALL SELECT 4, CAST(FALSE AS BOOLEAN), CAST(7 AS BIGINT)`

// pgNumericTextRows is the five-row TEXT fixture the #504 entries above are
// written over, as a derived table both engines parse identically. The values
// are chosen so the byte order and the numeric order DISAGREE: "1.50" and
// "1.5" are one number and two strings, and "9" sorts above "10" as text and
// below it as a number.
const pgNumericTextRows = `
	SELECT 1 AS k, CAST('1.50' AS VARCHAR) AS v
	UNION ALL SELECT 2, CAST('1.5' AS VARCHAR)
	UNION ALL SELECT 3, CAST('abc' AS VARCHAR)
	UNION ALL SELECT 4, CAST('10' AS VARCHAR)
	UNION ALL SELECT 5, CAST('9' AS VARCHAR)`

// pgFloatRows is the eight-row float fixture the #459 entries above are
// written over, as a derived table both engines parse identically. NaN and
// ±Infinity have to be manufactured with CAST rather than stored: ingest
// JSON-encodes row-group statistics into the catalog manifest and
// encoding/json refuses them, so no wadjet table can hold one today.
const pgFloatRows = `
	SELECT 1 AS k, CAST('NaN' AS DOUBLE PRECISION) AS v
	UNION ALL SELECT 2, CAST(0.0 AS DOUBLE PRECISION)
	UNION ALL SELECT 3, CAST(-0.0 AS DOUBLE PRECISION)
	UNION ALL SELECT 4, CAST('Infinity' AS DOUBLE PRECISION)
	UNION ALL SELECT 5, CAST('-Infinity' AS DOUBLE PRECISION)
	UNION ALL SELECT 6, CAST(1.0 AS DOUBLE PRECISION)
	UNION ALL SELECT 7, CAST(2.0 AS DOUBLE PRECISION)
	UNION ALL SELECT 8, CAST(NULL AS DOUBLE PRECISION)`
