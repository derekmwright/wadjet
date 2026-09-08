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
