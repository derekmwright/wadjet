package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// THE WINDOW DECLARED-TYPE CENSUS — #813, on FOUR arms, over every numeric
// width the engine has.
//
// #813 was filed as two claims. Measured at 2e386378 against live
// PostgreSQL 17.11 over rows identical to the `numwidth` fixture:
//
//  1. `CAST(SUM(x) OVER () AS BIGINT)` declares INT64 single-process and
//     FLOAT64 on the stage DAG. NO LONGER REPRODUCES: both spellings declare
//     INT64 on all four arms and answer PostgreSQL's digits. The DAG reaches
//     that answer by REFUSING the plan and routing to the coordinator's local
//     pipeline (`unreachable output +1`), which the two cells below assert —
//     so what closed is the DIVERGENCE, and the DAG still never declares this
//     expression itself.
//
//  2. `SUM(<integer>) OVER ()` declares float8 where the GROUPED spelling is
//     exact. CLOSED 2026-09-07 by #987 (arc K2). The float64 accumulator was
//     worse than a wrong declaration: past 2^53 its total depended on the
//     ORDER the rows arrived in, so this very file's
//     `CAST(SUM(int8) OVER () AS BIGINT)` cell answered 9007201419001864
//     about one run in twenty on the routed DAG arms and 9007201419001868 on
//     the rest. `exec.IntegerAccOutputType` is now the ONE table the grouped
//     declaration, the window declaration and the operator's runtime
//     correction all read, and `exec.windowExactFrames` accumulates an
//     integer input in the same Int128 carrier the grouped spelling uses. The
//     cells below are what the repair moved, and they are the proof.
//
// This is the CENSUS, the full cross of five window aggregates and six
// widths. Every `want` is what wadjet answers on all four arms; every
// DIVERGENT cell names PostgreSQL's own answer in `why`, so the day the
// divergence closes the cell FAILS and deleting it is the fix's proof. A cell
// with no `why` agrees with PostgreSQL and is a control: it fails if a repair
// moves something it should not.
//
// The GROUPED spelling of each aggregate rides beside it. That pairing is the
// point — `SUM(d) GROUP BY g` and `SUM(d) OVER ()` are one question written
// twice, and the census is what says which of the two moved.
func TestH2TheWindowDeclaredTypeCensus(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := f1Arms(t, ctx)

	win := func(fn, col string) string {
		return fmt.Sprintf("SELECT %s(%s) OVER () AS v FROM numwidth ORDER BY 1 LIMIT 1", fn, col)
	}
	grp := func(fn, col string) string {
		return fmt.Sprintf("SELECT %s(%s) AS v FROM numwidth", fn, col)
	}
	// The network types live on the type-matrix table, not on numwidth.
	k2win := func(fn, col string) string {
		return fmt.Sprintf("SELECT %s(%s) OVER () AS v FROM typemx ORDER BY 1 LIMIT 1", fn, col)
	}
	k2grp := func(fn, col string) string {
		return fmt.Sprintf("SELECT %s(%s) AS v FROM typemx", fn, col)
	}

	cases := []f1Case{
		// SUM — the family #813 is about. The two DECIMAL widths already
		// answer PostgreSQL's type and digits (#586); the four others do not.
		{
			// CLOSED by #987: bigint, exactly what PostgreSQL declares, and
			// the same INT64 2164260874 the GROUPED spelling below answers.
			name: "813 SUM(int4) OVER () is bigint",
			sql:  win("SUM", "w_i32"),
			want: "cols=[v:INT64] rows=1 | 2164260874",
		},
		{name: "813 control: SUM(int4) grouped", sql: grp("SUM", "w_i32"),
			want: "cols=[v:INT64] rows=1 | 2164260874"},
		{
			// CLOSED by #987: numeric, and the digits past 2^53 the float64
			// accumulator used to lose — intermittently, since its error
			// depended on the order the DAG's tasks delivered rows in.
			name: "813 SUM(int8) OVER () is numeric and exact",
			sql:  win("SUM", "w_i64"),
			want: "cols=[v:DECIMAL(38,0)] rows=1 | 9007201419001868",
		},
		{name: "813 control: SUM(int8) grouped", sql: grp("SUM", "w_i64"),
			want: "cols=[v:DECIMAL(38,0)] rows=1 | 9007201419001868"},
		{
			// The width the filing does not name and the census found: a
			// FLOAT32 input takes the same float64 accumulator, so the window
			// and the grouped spelling disagree about the TYPE and about the
			// last digit.
			name: "813 SUM(real) OVER ()",
			sql:  win("SUM", "w_f32"),
			want: "cols=[v:FLOAT64] rows=1 | 1.67772251e+07",
			why: "PostgreSQL 17 declares real and answers 1.6777224e+07; wadjet's " +
				"GROUPED spelling declares FLOAT32 and answers 1.6777226e+07. Three " +
				"answers to one question. DEFERRED (ADR-0012).",
		},
		{name: "813 control: SUM(real) grouped", sql: grp("SUM", "w_f32"),
			want: "cols=[v:FLOAT32] rows=1 | 1.6777226e+07"},
		{name: "813 control: SUM(float8) OVER ()", sql: win("SUM", "w_f64"),
			want: "cols=[v:FLOAT64] rows=1 | 9.007199271518218e+15"},
		{name: "813 control: SUM(numeric(9,2)) OVER ()", sql: win("SUM", "w_d2"),
			want: "cols=[v:DECIMAL(38,2)] rows=1 | 9.10"},
		{name: "813 control: SUM(numeric(18,4)) OVER ()", sql: win("SUM", "w_d4"),
			want: "cols=[v:DECIMAL(38,4)] rows=1 | 16777226.1001"},

		// AVG — the same rule, and the width where the DIGITS are visibly
		// gone rather than merely mis-declared.
		{
			// CLOSED by #987. PostgreSQL prints 270532609.25000000 — its
			// numeric division picks a magnitude-dependent scale; wadjet's is
			// the fixed +4 of ADR-0024 item 2. Both are exact to the digits
			// they keep and agree to min(scale), which is ADR-0012 item 9's
			// class, not a value divergence.
			name: "813 AVG(int4) OVER () is numeric",
			sql:  win("AVG", "w_i32"),
			want: "cols=[v:DECIMAL(38,4)] rows=1 | 270532609.2500",
		},
		{
			name: "813 AVG(int8) OVER () — the DIGITS, not only the type",
			sql:  win("AVG", "w_i64"),
			want: "cols=[v:DECIMAL(38,4)] rows=1 | 1000800157666874.2222",
		},
		{name: "813 control: AVG(int8) grouped is exact", sql: grp("AVG", "w_i64"),
			want: "cols=[v:DECIMAL(38,4)] rows=1 | 1000800157666874.2222"},
		{name: "813 control: AVG(float8) OVER ()", sql: win("AVG", "w_f64"),
			want: "cols=[v:FLOAT64] rows=1 | 1.0007999190575798e+15"},
		{name: "813 control: AVG(numeric(9,2)) OVER ()", sql: win("AVG", "w_d2"),
			want: "cols=[v:DECIMAL(38,6)] rows=1 | 1.300000"},
		{name: "813 control: AVG(numeric(18,4)) OVER ()", sql: win("AVG", "w_d4"),
			want: "cols=[v:DECIMAL(38,8)] rows=1 | 2097153.26251250"},

		// MIN/MAX and COUNT are input-independent or copy their input, and
		// #569 already made them declare it. They are the controls that say
		// the SUM/AVG deferral is about the ACCUMULATOR and not about the
		// window's typing in general.
		{name: "813 control: MIN(int4) OVER () declares int4", sql: win("MIN", "w_i32"),
			want: "cols=[v:INT32] rows=1 | -20"},
		{name: "813 control: MAX(int4) OVER () declares int4", sql: win("MAX", "w_i32"),
			want: "cols=[v:INT32] rows=1 | 2147483647"},
		{name: "813 control: MIN(real) OVER () declares real", sql: win("MIN", "w_f32"),
			want: "cols=[v:FLOAT32] rows=1 | -20"},
		{name: "813 control: MAX(numeric(9,2)) OVER () keeps its (p,s)", sql: win("MAX", "w_d2"),
			want: "cols=[v:DECIMAL(9,2)] rows=1 | 12.75"},
		{name: "813 control: COUNT(int4) OVER () is bigint", sql: win("COUNT", "w_i32"),
			want: "cols=[v:INT64] rows=1 | 8"},

		// Issue item 1, now a two-cell ratchet rather than a divergence. The
		// ROUTING is asserted because it is how the DAG reaches the answer:
		// rows alone cannot tell that apart from the DAG declaring it.
		{
			name: "813 item 1: CAST(SUM(numeric) OVER () AS BIGINT) declares its target",
			sql:  "SELECT CAST(SUM(w_d2) OVER () AS BIGINT) AS v FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[v:INT64] rows=1 | 9",
			routed: map[string]string{
				"dag": "unreachable output +1", "dagshuf": "unreachable output +1"},
		},
		{
			name: "813 item 1: CAST(SUM(int8) OVER () AS BIGINT) declares its target",
			sql:  "SELECT CAST(SUM(w_i64) OVER () AS BIGINT) AS v FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[v:INT64] rows=1 | 9007201419001868",
			routed: map[string]string{
				"dag": "unreachable output +1", "dagshuf": "unreachable output +1"},
		},

		// #987 IS NOT `OVER ()`. The float64 accumulator was wrong in every
		// FRAME form, and the sliding one exercises the retract as well as the
		// add — a subtraction above 2^53 loses digits the same way. Every row
		// below is PostgreSQL 17.11's, taken live over these ten rows, and
		// every row is asserted rather than a LIMIT 1 sample: the running
		// frame's error was two ulps at row 4 and none at row 8, so a sample
		// of one row could pass a float accumulator.
		{
			name: "987 the RUNNING frame is exact above 2^53",
			sql: "SELECT w_key, SUM(w_i64) OVER (ORDER BY w_key) AS v " +
				"FROM numwidth ORDER BY w_key",
			want: "cols=[w_key:INT64 v:DECIMAL(38,0)] rows=10 | 0,2 | 1,5 | 2,17 | " +
				"3,16777234 | 4,9007199271518227 | 5,9007199271518207 | " +
				"6,9007199271518207 | 7,9007199271518207 | 8,9007199271518220 | " +
				"9,9007201419001868",
		},
		{
			name: "987 the SLIDING frame's exit subtraction is exact above 2^53",
			sql: "SELECT w_key, SUM(w_i64) OVER (ORDER BY w_key " +
				"ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS v FROM numwidth ORDER BY w_key",
			want: "cols=[w_key:INT64 v:DECIMAL(38,0)] rows=10 | 0,5 | 1,17 | " +
				"2,16777232 | 3,9007199271518222 | 4,9007199271518190 | " +
				"5,9007199254740973 | 6,-20 | 7,13 | 8,2147483661 | 9,2147483661",
		},
		{
			name: "987 a PARTITION BY running frame is exact above 2^53",
			sql: "SELECT w_key, SUM(w_i64) OVER (PARTITION BY w_key % 2 ORDER BY w_key) AS v " +
				"FROM numwidth ORDER BY w_key",
			want: "cols=[w_key:INT64 v:DECIMAL(38,0)] rows=10 | 0,2 | 1,3 | 2,14 | " +
				"3,16777220 | 4,9007199254741007 | 5,16777200 | 6,9007199254741007 | " +
				"7,16777200 | 8,9007199254741020 | 9,2164260848",
		},
		// #953: PORT and PROTOCOL declare int4 on the wire (#834), so
		// `sum(port)` is `sum(int4)`. Both spellings are asserted because
		// before this they were not merely mis-typed: the GROUPED
		// `SUM(c_proto)` had no arm in the aggregate's dispatch at all and
		// answered NULL where the WINDOWED spelling answered 621435.
		{name: "953 SUM(PORT) OVER () is bigint", sql: k2win("SUM", "c_port"),
			want: "cols=[v:INT64] rows=1 | 17376678"},
		{name: "953 control: SUM(PORT) grouped", sql: k2grp("SUM", "c_port"),
			want: "cols=[v:INT64] rows=1 | 17376678"},
		{name: "953 AVG(PORT) OVER () is numeric", sql: k2win("AVG", "c_port"),
			want: "cols=[v:DECIMAL(38,4)] rows=1 | 3523.2518"},
		{name: "953 control: AVG(PORT) grouped", sql: k2grp("AVG", "c_port"),
			want: "cols=[v:DECIMAL(38,4)] rows=1 | 3523.2518"},
		{name: "953 SUM(PROTOCOL) OVER () is bigint", sql: k2win("SUM", "c_proto"),
			want: "cols=[v:INT64] rows=1 | 621435"},
		{name: "953 the GROUPED SUM(PROTOCOL) answers, where it used to be NULL",
			sql:  k2grp("SUM", "c_proto"),
			want: "cols=[v:INT64] rows=1 | 621435"},
		{name: "953 AVG(PROTOCOL) OVER () is numeric", sql: k2win("AVG", "c_proto"),
			want: "cols=[v:DECIMAL(38,4)] rows=1 | 125.8730"},
		{name: "953 control: AVG(PROTOCOL) grouped", sql: k2grp("AVG", "c_proto"),
			want: "cols=[v:DECIMAL(38,4)] rows=1 | 125.8730"},
		// The BOUNDARY of #953, attempted from the other side: DATE,
		// TIMESTAMP and DURATION are int-backed too and stay float8 in BOTH
		// spellings, because `date` and `timestamp` are their own wire types
		// with no PostgreSQL `sum` and an interval's sum is an interval.
		{name: "953 boundary: SUM(DURATION) stays float8 in both spellings",
			sql:  k2win("SUM", "c_dur"),
			want: "cols=[v:FLOAT64] rows=1 | 1.234567e+13"},
		{name: "953 boundary control: SUM(DURATION) grouped", sql: k2grp("SUM", "c_dur"),
			want: "cols=[v:FLOAT64] rows=1 | 1.234567e+13"},
		{name: "953 boundary: SUM(DATE) stays float8 in both spellings",
			sql:  k2win("SUM", "c_date"),
			want: "cols=[v:FLOAT64] rows=1 | 8.583688e+07"},
		{name: "953 boundary control: SUM(DATE) grouped", sql: k2grp("SUM", "c_date"),
			want: "cols=[v:FLOAT64] rows=1 | 8.583688e+07"},

		// #953's grouped half reaches the accumulator through FOUR producers
		// and the whole-table cells above exercise only the ones a scalar
		// aggregate uses. Round 1 found that: reverting the PROTOCOL arm of
		// `kernel.ResolveRowSum` ALONE, or the four `agg_scatter.go` SUM/AVG
		// arms ALONE, left every gate in the branch green while
		// `SUM(c_proto) … GROUP BY` came back NULL on four arms. A gate that
		// passes with the fix reverted is not a gate.
		//
		// A KEYED GROUP BY drives the row updater and the SoA scatter;
		// DISTINCT drives the distinct-set path, which is a third producer
		// again. Each cell below fails on the single-hunk revert of the
		// producer it names.
		// The three group SUMs add to 621435 — the whole-table total the cell
		// above pins — and each group's AVG is that group's SUM over its own
		// COUNT(*) (1660 / 1640 / 1637), so every number here is checkable
		// from PostgreSQL's `sum(int4)` rule and arithmetic, not taken on
		// faith. `c_proto % 3` declares float8, which is the modulo's own
		// pre-existing typing and not this arc's.
		{name: "953 keyed: SUM(PROTOCOL) GROUP BY a key — the row updater and the SoA scatter",
			sql:  "SELECT c_proto % 3 AS g, SUM(c_proto) AS s FROM typemx GROUP BY c_proto % 3 ORDER BY 1",
			want: "cols=[g:FLOAT64 s:INT64] rows=4 | 0,209145 | 1,205430 | 2,206860 | NULL,NULL"},
		{name: "953 keyed: AVG(PROTOCOL) GROUP BY a key — the exact Int128 scatter",
			sql: "SELECT c_proto % 3 AS g, AVG(c_proto) AS a FROM typemx GROUP BY c_proto % 3 ORDER BY 1",
			want: "cols=[g:FLOAT64 a:DECIMAL(38,4)] rows=4 | 0,125.9910 | 1,125.2622 | " +
				"2,126.3653 | NULL,NULL"},
		{name: "953 keyed: SUM(PORT) GROUP BY a key",
			sql: "SELECT c_port % 3 AS g, SUM(c_port) AS s FROM typemx GROUP BY c_port % 3 ORDER BY 1",
			want: "cols=[g:FLOAT64 s:INT64] rows=4 | 0,5792238 | 1,5792226 | " +
				"2,5792214 | NULL,NULL"},
		// PROTOCOL holds 0..255 with every value present, so the DISTINCT
		// total is 0+1+…+255 = 32640 over 256 values — sharply different from
		// the raw 621435, which is what lets this cell tell a working DISTINCT
		// from a dropped one.
		{name: "953 DISTINCT: SUM(DISTINCT PROTOCOL) is the distinct total, not the raw one",
			sql:  "SELECT SUM(DISTINCT c_proto) AS s, COUNT(DISTINCT c_proto) AS n FROM typemx",
			want: "cols=[s:INT64 n:INT64] rows=1 | 32640,256"},
		{name: "953 DISTINCT: AVG(DISTINCT PROTOCOL) is 32640/256",
			sql:  "SELECT AVG(DISTINCT c_proto) AS a FROM typemx",
			want: "cols=[a:DECIMAL(38,4)] rows=1 | 127.5000"},
		// PORT's values are all distinct (1024..6023), so DISTINCT must change
		// NEITHER the total nor the count — asserted as that equality, so the
		// cell carries its own proof rather than a number from elsewhere.
		//
		// It is a CONTROL and not a discriminator, and saying so is the point:
		// because every value is distinct, a DISTINCT that was DROPPED would
		// answer exactly the same four numbers. The cell that can tell those
		// apart is PROTOCOL's above, where the two totals differ by 20x.
		{name: "953 DISTINCT: SUM(DISTINCT PORT) equals SUM(PORT) — every value is distinct",
			sql: "SELECT SUM(DISTINCT c_port) AS s, SUM(c_port) AS t, " +
				"COUNT(DISTINCT c_port) AS n, COUNT(c_port) AS c FROM typemx",
			want: "cols=[s:INT64 t:INT64 n:INT64 c:INT64] rows=1 | 17376678,17376678,4932,4932"},

		// #987 review P4's control: the window WITHOUT DISTINCT still answers.
		// The refusal itself is TestAWindowFunctionRefusesDISTINCT, which
		// asserts the SQLSTATE rather than a rendered message — the four arms
		// wrap the parse error under different prefixes and a prefix match
		// would pin the wrapping instead of the refusal.
		{name: "987 P4 control: a window WITHOUT DISTINCT still answers",
			sql:  k2win("SUM", "c_proto"),
			want: "cols=[v:INT64] rows=1 | 621435"},

		// A COMPUTED window argument. The first REPORT called this a deferral
		// — "a computed window argument keeps float8" — and round 1 measured
		// it FALSE: the pre-window projection materializes the expression as a
		// column with its own INT64 declaration, so windowSpecOutputType's new
		// integer arm reads it and these are exact too. Two of them returned
		// the WRONG NUMBER at bb8635a4 and nothing in the branch held any of
		// it; every `want` is PostgreSQL 17.11's over these ten rows.
		{name: "987 computed argument: SUM(int8 * 1) OVER ()",
			sql:  "SELECT SUM(w_i64 * 1) OVER () AS v FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[v:DECIMAL(38,0)] rows=1 | 9007201419001868"},
		{name: "987 computed argument: SUM(int8 * 2) OVER ()",
			sql:  "SELECT SUM(w_i64 * 2) OVER () AS v FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[v:DECIMAL(38,0)] rows=1 | 18014402838003736"},
		{
			// base answered 9.007201419001876e+15; PostgreSQL says …877.
			name: "987 computed argument: SUM(int8 + 1) OVER () — the digits moved",
			sql:  "SELECT SUM(w_i64 + 1) OVER () AS v FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[v:DECIMAL(38,0)] rows=1 | 9007201419001877",
		},
		{name: "987 computed argument: SUM(int8 + int4) OVER ()",
			sql:  "SELECT SUM(w_i64 + w_i32) OVER () AS v FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[v:DECIMAL(38,0)] rows=1 | 4328521749"},
		{
			// base answered 9.007201402224632e+15; PostgreSQL says …634.
			name: "987 computed argument: SUM(CASE …) OVER () — the digits moved",
			sql: "SELECT SUM(CASE WHEN w_key > 3 THEN w_i64 ELSE 0 END) OVER () AS v " +
				"FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[v:DECIMAL(38,0)] rows=1 | 9007201402224634",
		},
		{
			// #987 review P2, and the arc's own position 3 read in the other
			// direction. ABS answers in its ARGUMENT's numeric domain, so
			// `sum(abs(int8))` is numeric in PostgreSQL — which the WINDOW
			// spelling already said and the GROUPED spelling did not:
			// aggInputIsWideInteger declined every non-polymorphic function
			// and read the expression as int4, declaring bigint. Same digits,
			// two boxes. Both ask expr.NumericDomainScalarFn now.
			name: "987 P2: SUM(ABS(int8)) is numeric in BOTH spellings",
			sql:  "SELECT SUM(ABS(w_i64)) OVER () AS v FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[v:DECIMAL(38,0)] rows=1 | 9007201419001908",
		},
		{name: "987 P2: the GROUPED spelling of the same aggregate",
			sql:  "SELECT SUM(ABS(w_i64)) AS v FROM numwidth",
			want: "cols=[v:DECIMAL(38,0)] rows=1 | 9007201419001908"},
		{
			// The BOUNDARY of P2, attempted from the other side: a computed
			// int4 argument stays bigint in both spellings, which is
			// PostgreSQL's answer and TPC-H Q12's shape.
			name: "987 P2 boundary: SUM(ABS(int4)) stays bigint in both spellings",
			sql:  "SELECT SUM(ABS(w_i32)) OVER () AS v FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[v:INT64] rows=1 | 2164260914",
		},
		{name: "987 P2 boundary control: the GROUPED SUM(ABS(int4))",
			sql:  "SELECT SUM(ABS(w_i32)) AS v FROM numwidth",
			want: "cols=[v:INT64] rows=1 | 2164260914"},
		{name: "987 P2 boundary control: SUM(CASE of ones) stays bigint (TPC-H Q12's shape)",
			sql:  "SELECT SUM(CASE WHEN w_key > 3 THEN 1 ELSE 0 END) AS v FROM numwidth",
			want: "cols=[v:INT64] rows=1 | 6"},

		// #987 review B1: the SAME question, WINDOWED. Round 2's fix read the
		// MATERIALIZED argument column, and every integer expression in this
		// engine computes in int64 (ADR-0024's widening) — so an int4-domain
		// expression came back INT64 and declared DECIMAL(38,0), OID 1700,
		// where its GROUPED twin two lines up declares bigint and where
		// PostgreSQL declares bigint. One question, two spellings, two boxes:
		// exactly the class #813 was, with the spellings' roles swapped.
		//
		// The width survives only in the SYNTAX, which is why the grouped path
		// walks the AST (aggInputIsWideInteger) and why the window now carries
		// the argument's node to ask that same function
		// (physical.windowComputedArgDecl, over aggInputIsWideInteger and
		// physical.integerAccArgWidth). Every shape below is asserted
		// in BOTH spellings, and the OIDs beside them are
		// pgwire.TestAComputedIntegerWindowArgumentDeclaresPostgresOID.
		{name: "987 B1: SUM(CASE of ones) OVER () is bigint (TPC-H Q12's shape, windowed)",
			sql: "SELECT SUM(CASE WHEN w_key > 3 THEN 1 ELSE 0 END) OVER () AS v " +
				"FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[v:INT64] rows=1 | 6"},
		{name: "987 B1: SUM(int4 * 1) OVER () is bigint",
			sql:  "SELECT SUM(w_i32 * 1) OVER () AS v FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[v:INT64] rows=1 | 2164260874"},
		{name: "987 B1 control: SUM(int4 * 1) grouped",
			sql:  "SELECT SUM(w_i32 * 1) AS v FROM numwidth",
			want: "cols=[v:INT64] rows=1 | 2164260874"},
		{name: "987 B1: SUM(-int4) OVER () is bigint",
			sql:  "SELECT SUM(-w_i32) OVER () AS v FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[v:INT64] rows=1 | -2164260874"},
		{name: "987 B1 control: SUM(-int4) grouped",
			sql:  "SELECT SUM(-w_i32) AS v FROM numwidth",
			want: "cols=[v:INT64] rows=1 | -2164260874"},
		{name: "987 B1: SUM(int4 + 1) OVER () is bigint",
			sql:  "SELECT SUM(w_i32 + 1) OVER () AS v FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[v:INT64] rows=1 | 2164260882"},
		{name: "987 B1 control: SUM(int4 + 1) grouped",
			sql:  "SELECT SUM(w_i32 + 1) AS v FROM numwidth",
			want: "cols=[v:INT64] rows=1 | 2164260882"},
		{name: "987 B1: SUM(MOD(int4, 10)) OVER () is bigint",
			sql:  "SELECT SUM(MOD(w_i32, 10)) OVER () AS v FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[v:INT64] rows=1 | 24"},
		{name: "987 B1 control: SUM(MOD(int4, 10)) grouped",
			sql:  "SELECT SUM(MOD(w_i32, 10)) AS v FROM numwidth",
			want: "cols=[v:INT64] rows=1 | 24"},
		// #987 review ROUND 3, B1: a CAST is an operand whose width is its
		// TARGET's, and the walk had no arm for one — so every int8 operand
		// written under a cast read as int4 and `SUM(bigint_col::bigint)`
		// declared bigint in BOTH spellings where PostgreSQL declares
		// numeric. It is not only a declaration: past int64 a total
		// PostgreSQL ANSWERS became 22003, which the cell below the six
		// asserts is answered again.
		{name: "987 R3 B1: SUM(CAST(int8 AS BIGINT)) OVER () is numeric",
			sql:  "SELECT SUM(CAST(w_i64 AS BIGINT)) OVER () AS v FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[v:DECIMAL(38,0)] rows=1 | 9007201419001868"},
		{name: "987 R3 B1 control: SUM(CAST(int8 AS BIGINT)) grouped",
			sql:  "SELECT SUM(CAST(w_i64 AS BIGINT)) AS v FROM numwidth",
			want: "cols=[v:DECIMAL(38,0)] rows=1 | 9007201419001868"},
		{name: "987 R3 B1: SUM(int8::BIGINT) OVER () — the other spelling of the cast",
			sql:  "SELECT SUM(w_i64::BIGINT) OVER () AS v FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[v:DECIMAL(38,0)] rows=1 | 9007201419001868"},
		{
			// A cast WIDENS as well as keeps: `sum(int4_col::bigint)` is
			// numeric in PostgreSQL because the argument is int8 by the time
			// SUM sees it, even though the column is int4.
			name: "987 R3 B1: SUM(CAST(int4 AS BIGINT)) OVER () is numeric, not bigint",
			sql:  "SELECT SUM(CAST(w_i32 AS BIGINT)) OVER () AS v FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[v:DECIMAL(38,0)] rows=1 | 2164260874",
		},
		{name: "987 R3 B1 control: SUM(CAST(int4 AS BIGINT)) grouped",
			sql:  "SELECT SUM(CAST(w_i32 AS BIGINT)) AS v FROM numwidth",
			want: "cols=[v:DECIMAL(38,0)] rows=1 | 2164260874"},
		{
			// The BOUNDARY: a cast NARROWS too. `sum(int8_col::int4)` is
			// bigint in PostgreSQL — the argument is int4 by then — so an
			// arm that answered "wide" for every cast would fail here.
			// w_key, not w_i64: 2^53+1 has no int4 and the CAST itself
			// refuses, which is PostgreSQL's `integer out of range` and a
			// different question from this one.
			name: "987 R3 B1 boundary: SUM(CAST(int8 AS INTEGER)) OVER () is bigint",
			sql:  "SELECT SUM(CAST(w_key AS INTEGER)) OVER () AS v FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[v:INT64] rows=1 | 45",
		},
		{name: "987 R3 B1 boundary control: SUM(CAST(int8 AS INTEGER)) grouped",
			sql:  "SELECT SUM(CAST(w_key AS INTEGER)) AS v FROM numwidth",
			want: "cols=[v:INT64] rows=1 | 45"},
		{name: "987 R3 B1 boundary control: the same column UNCAST is numeric",
			sql:  "SELECT SUM(w_key) OVER () AS v FROM numwidth ORDER BY 1 LIMIT 1",
			want: "cols=[v:DECIMAL(38,0)] rows=1 | 45"},
		{
			// The DISPOSITION half, and the reason B1 was a blocker rather
			// than a mis-declaration: 10^5 rows put the total past int64, so
			// the bigint reading refused 22003 a query PostgreSQL answers —
			// while the identical query one cast away answered it exactly.
			// "PostgreSQL answers and we refuse" is the direction ADR-0012
			// does not allow.
			name: "987 R3 B1: a cast total past int64 ANSWERS, as PostgreSQL does",
			sql: "SELECT SUM(CAST(a.w_i64 AS BIGINT)) OVER () AS v FROM numwidth a, " +
				"numwidth b, numwidth c, numwidth d, numwidth e ORDER BY 1 LIMIT 1",
			want: "cols=[v:DECIMAL(38,0)] rows=1 | 90072014190018680000",
		},
		{name: "987 R3 B1 control: the same total one cast away",
			sql: "SELECT SUM(a.w_i64) OVER () AS v FROM numwidth a, numwidth b, " +
				"numwidth c, numwidth d, numwidth e ORDER BY 1 LIMIT 1",
			want: "cols=[v:DECIMAL(38,0)] rows=1 | 90072014190018680000"},

		// #987 review ROUND 3, P1 — PINNED, fail-on-agree. A bare PORT or
		// PROTOCOL takes int4's result types (the eight #953 cells above);
		// the same column under ARITHMETIC does not, in EITHER spelling,
		// because `c_port * 1` is evaluated on the float path.
		// `expr.operandIsInt` keeps the network types there deliberately
		// ("Timestamps/dates/network types keep the float path — their
		// arithmetic semantics are handled elsewhere"), and
		// `physical.intArithAllInt` mirrors it so a declaration cannot
		// promise an integer the kernel will not produce. Moving the
		// declaration alone would be exactly that promise.
		//
		// The two spellings AGREE with each other and PostgreSQL has neither
		// type, so this is an internal-consistency gap, not a value
		// divergence — but it is one the docs claimed was closed, so it is
		// pinned here and recorded in ADR-0012's #953 entry with its
		// mechanism. The day the expression layer makes network arithmetic
		// integral, these cells FAIL and deleting them is the proof.
		{name: "953 P1 PINNED: SUM(PROTOCOL * 1) OVER () is float8, not bigint",
			sql:  "SELECT SUM(c_proto * 1) OVER () AS v FROM typemx ORDER BY 1 LIMIT 1",
			want: "cols=[v:FLOAT64] rows=1 | 621435",
			why: "the bare SUM(c_proto) is INT64 621435 two dozen cells up. PORT and " +
				"PROTOCOL arithmetic runs on the float path by design (expr.operandIsInt); " +
				"closing it means moving the KERNEL, not this declaration. PINNED."},
		{name: "953 P1 PINNED control: the GROUPED spelling agrees",
			sql:  "SELECT SUM(c_proto * 1) AS v FROM typemx",
			want: "cols=[v:FLOAT64] rows=1 | 621435",
			why:  "same mechanism; the two spellings agree with each other, which is the point"},
		{name: "953 P1 PINNED: SUM(PORT * 1) OVER () is float8",
			sql:  "SELECT SUM(c_port * 1) OVER () AS v FROM typemx ORDER BY 1 LIMIT 1",
			want: "cols=[v:FLOAT64] rows=1 | 1.7376678e+07",
			why: "the bare SUM(c_port) is INT64 17376678 — the same number in a different " +
				"box AND a different rendering. PINNED with SUM(c_proto * 1)."},
		{name: "953 P1 PINNED control: the GROUPED spelling agrees",
			sql:  "SELECT SUM(c_port * 1) AS v FROM typemx",
			want: "cols=[v:FLOAT64] rows=1 | 1.7376678e+07",
			why:  "same mechanism"},
		{name: "953 P1 PINNED: SUM(ABS(PROTOCOL)) OVER () is float8",
			sql:  "SELECT SUM(ABS(c_proto)) OVER () AS v FROM typemx ORDER BY 1 LIMIT 1",
			want: "cols=[v:FLOAT64] rows=1 | 621435",
			why:  "ABS answers in its argument's domain, and that domain is the float path here"},
		{name: "953 P1 PINNED: AVG(PROTOCOL * 1) OVER () is float8, not numeric(38,4)",
			sql:  "SELECT AVG(c_proto * 1) OVER () AS v FROM typemx ORDER BY 1 LIMIT 1",
			want: "cols=[v:FLOAT64] rows=1 | 125.87299979744785",
			why:  "the bare AVG(c_proto) is DECIMAL(38,4). PINNED with the SUM cells."},

		{
			// The OTHER side of the same walk, and the reason it is a walk
			// rather than "a computed argument is int4": one int8 arm makes
			// the CASE int8, so this one is numeric in both spellings. A fix
			// that narrowed every computed argument would pass the six cells
			// above and fail this one.
			name: "987 B1 boundary: a CASE with an int8 arm is numeric, grouped",
			sql: "SELECT SUM(CASE WHEN w_key > 3 THEN w_i64 ELSE 0 END) AS v " +
				"FROM numwidth",
			want: "cols=[v:DECIMAL(38,0)] rows=1 | 9007201402224634",
		},

		{
			// The int4 half of the same shape: bigint, and the sliding frame
			// again, because SUM(int4) writes through a DIFFERENT arm of
			// windowExactFrames (an int64 output vector, not a DECIMAL one).
			name: "987 SUM(int4) over a sliding frame is bigint",
			sql: "SELECT w_key, SUM(w_i32) OVER (ORDER BY w_key " +
				"ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS v FROM numwidth ORDER BY w_key",
			want: "cols=[w_key:INT64 v:INT64] rows=10 | 0,5 | 1,17 | 2,16777232 | " +
				"3,16777229 | 4,16777197 | 5,-20 | 6,-20 | 7,13 | 8,2147483660 | " +
				"9,2147483660",
		},
	}
	f1Run(t, arms, cases)
}
