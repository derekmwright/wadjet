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
			pgRows, pgCols, pgErr := run(ctx, c.sql)
			if pgErr != nil {
				// PostgreSQL refusing the query means the entry is not a
				// question about Wadjet. It is always the corpus's fault, so
				// it fails loudly rather than being skipped.
				t.Fatalf("the ORACLE refused this query, so it cannot be ground truth for anything: %v\n  SQL: %s",
					pgErr, c.sql)
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

	out = append(out, postgresSemanticsCases()...)

	for i := range out {
		if !out[i].countOnly {
			out[i].ordered = hasTopLevelOrderBy(out[i].sql)
		}
	}
	return out
}

// postgresSemanticsCases is the half of the corpus DuckDB cannot answer for.
// Grouped by the rule each family pins.
func postgresSemanticsCases() []pgCase {
	var out []pgCase

	// --- NULL placement in ORDER BY -------------------------------------
	//
	// PostgreSQL's rule — NULLS LAST for ASC, NULLS FIRST for DESC — is the
	// one Wadjet claims to implement, and the DuckDB arm has to be CONFIGURED
	// into it (default_null_order). Here it is simply asked of the engine that
	// defines it, which is the difference between a convention and a citation.
	out = append(out,
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
		// happens to satisfy the predicates above is visible here — once the
		// query can run: PROJECTING an extremum over a DECIMAL column fails
		// outright today, because the registry declares the return type
		// FLOAT64 and the value arrives as the column's rendered text. That
		// is the declared-output-type layer, not the comparison rule this
		// family gates, so it is pinned rather than fixed here.
		pgCase{name: "DecimalColPairGreatestValue", exactNumeric: true,
			sql: `SELECT d_key, GREATEST(d_2, d_4) AS g, LEAST(d_2, d_4) AS l FROM dec_probe ORDER BY d_key`,
			knownBug: pgBugWadjet + ` a projected GREATEST/LEAST over a DECIMAL column cannot run at ` +
				`all — "cannot store string into FLOAT64 vector"`, issue: "#529"},
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
	// which follows that rule. The bare forms are PINNED: they take the
	// vectorized filter, whose integer and float arms read ANY string
	// constant as the type's ZERO, so `d_key > '2'` selects every row above
	// ZERO. That is #536, pre-existing and unchanged by this round; the pin
	// fails when it is fixed.
	const quotedNumericPin = pgBugWadjet + ` the vectorized filter reads a quoted numeric constant as the ` +
		`column type's ZERO, so this asks "> 0" instead of "> 2". The row-at-a-time path answers ` +
		`PostgreSQL's number (the CASE-wrapped entries beside this one gate it).`
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
		// The scan path, pinned.
		pgCase{name: "IntColumnVsQuotedNumericGt",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key > '2'`, knownBug: quotedNumericPin, issue: "#536"},
		pgCase{name: "IntColumnVsQuotedNumericLt",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key < '2'`, knownBug: quotedNumericPin, issue: "#536"},
		pgCase{name: "IntColumnVsQuotedNumericInList",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key IN ('2','3')`, knownBug: quotedNumericPin, issue: "#536"},
		pgCase{name: "IntColumnVsQuotedNumericBetween",
			sql: `SELECT COUNT(*) AS n FROM dec_probe WHERE d_key BETWEEN '2' AND '4'`, knownBug: quotedNumericPin, issue: "#536"},
		pgCase{name: "FloatColumnVsQuotedNumericGt",
			sql: `SELECT COUNT(*) AS n FROM lineitem WHERE l_discount > '0.05'`, knownBug: quotedNumericPin, issue: "#536"},
	)

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
	// All five are #526 pins: a QUALIFIED item over a joined inner names a
	// column the semi join's build schema does not carry, exec.HashJoin's key
	// repair swaps the pair on #516's false premise, and the join matches
	// nothing — IN answers 0 and NOT IN answers every row. Measured over this
	// fixture, all five diverge on the base commit too, so nothing here is a
	// regression; they are in the corpus because it had no joined-inner
	// IN-subquery of any kind, which is why this whole family was dark.
	out = append(out,
		pgCase{name: "InSubqueryJoinedInnerLeadQualified",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_nationkey IN
				(SELECT c.n_nationkey FROM nation c JOIN nation b ON b.n_regionkey = c.n_regionkey
				 WHERE c.n_nationkey < 3)`,
			knownBug: pgBugWadjet + " an IN over a JOINED inner with a qualified select item names a " +
				"key the build schema does not carry, so the semi join matches nothing and the " +
				"predicate selects no rows (PostgreSQL: 3, wadjet: 0)", issue: "#526"},
		pgCase{name: "InSubqueryJoinedInnerLeadQualifiedValues",
			sql: `SELECT a.n_nationkey AS k FROM nation a WHERE a.n_nationkey IN
				(SELECT c.n_nationkey FROM nation c JOIN nation b ON b.n_regionkey = c.n_regionkey
				 WHERE c.n_nationkey < 3)
				ORDER BY k`, ordered: true,
			knownBug: pgBugWadjet + " the VALUES of the same shape: PostgreSQL returns 0,1,2 and " +
				"wadjet returns no rows at all", issue: "#526"},
		pgCase{name: "NotInSubqueryJoinedInnerLeadQualified",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_nationkey NOT IN
				(SELECT c.n_nationkey FROM nation c JOIN nation b ON b.n_regionkey = c.n_regionkey
				 WHERE c.n_nationkey < 3)`,
			knownBug: pgBugWadjet + " the NOT IN half of the same defect: the anti join matches " +
				"nothing, so every row survives (PostgreSQL: 22, wadjet: 25)", issue: "#526"},
		// Across two DIFFERENT tables, where no bare name collides. This is
		// the ORDINARY spelling of a joined inner and it is wrong for the
		// same reason, which is what says #526 is a property of the JOIN and
		// not of the name collision the self-join above needs.
		pgCase{name: "InSubqueryJoinedInnerCrossTable",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_regionkey IN
				(SELECT r.r_regionkey FROM region r JOIN nation b ON b.n_regionkey = r.r_regionkey
				 WHERE r.r_regionkey < 2)`,
			knownBug: pgBugWadjet + " a joined inner across two tables, item qualified by the " +
				"relation written first (PostgreSQL: 10, wadjet: 0)", issue: "#526"},
		pgCase{name: "InSubqueryJoinedInnerCrossTableNonLead",
			sql: `SELECT COUNT(*) AS c FROM nation a WHERE a.n_regionkey IN
				(SELECT b.n_regionkey FROM region r JOIN nation b ON b.n_regionkey = r.r_regionkey
				 WHERE r.r_regionkey < 2)`,
			knownBug: pgBugWadjet + " the same across two tables with the item qualified by the " +
				"relation written second (PostgreSQL: 10, wadjet: 0)", issue: "#526"},
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
		pgCase{name: "PolymorphicOverFloatColumns", sql: `SELECT ps_partkey, ps_suppkey,
			COALESCE(ps_supplycost, 0) AS coalesce_float,
			GREATEST(ps_supplycost, ps_availqty) AS greatest_mixed,
			LEAST(ps_supplycost, ps_availqty) AS least_mixed
			FROM partsupp WHERE ps_partkey <= 20 ORDER BY ps_partkey, ps_suppkey`},
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

	return out
}

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
