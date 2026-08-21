package tpch

import (
	"context"
	"fmt"
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
			pgRows, pgCols, pgErr := o.runPostgres(ctx, c.sql)
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
	want := oracle.FingerprintOf(positional(pgRows, pgCols), c.ordered)
	got := oracle.FingerprintOf(positional(wRows, wCols), c.ordered)
	return want.Match(got)
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
	)

	// --- Pagination ------------------------------------------------------
	out = append(out,
		pgCase{name: "OffsetAlone", sql: `SELECT n_nationkey FROM nation ORDER BY n_nationkey OFFSET 5`},
		pgCase{name: "OffsetThenLimit", sql: `SELECT n_nationkey FROM nation ORDER BY n_nationkey OFFSET 5 LIMIT 3`},
		pgCase{name: "LimitThenOffset", sql: `SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3 OFFSET 5`},
		pgCase{name: "OffsetPastEnd", sql: `SELECT n_nationkey FROM nation ORDER BY n_nationkey OFFSET 1000`,
			countOnly: true, why: "an empty result has no rows to fingerprint; at tolerance 0 the count is the entire answer"},
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
		// the same statement is a separate, unrelated gap #374's own pin
		// never named — it only reached REPLACE once POSITION stopped
		// failing first. REPLACE is a reserved lexer keyword that never
		// reaches the generic function-call dispatch, the same shape #371
		// fixed for EVERY; the scalar function itself (fnReplace) is
		// already implemented.
		pgCase{name: "PositionAndReplace", sql: `SELECT POSITION('cd' IN 'abcdef') AS p, POSITION('zz' IN 'abcdef') AS missing,
			REPLACE('abcabc', 'b', 'X') AS r`,
			knownBug: pgBugUnsupported + " REPLACE(...) does not parse as a function call: REPLACE is a reserved " +
				"keyword (presumably for CREATE OR REPLACE) that never reaches the function-call dispatch",
			issue: "#382"},
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

	return out
}
