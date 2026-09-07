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
//     exact. REPRODUCES, on every arm including single-process, so it is a
//     wadjet-vs-PostgreSQL divergence and not a two-path one. DEFERRED with
//     its mechanism (ADR-0012's divergence list): `exec.windowAccOutputType`
//     gives every non-DECIMAL input a FLOAT64 accumulator, and declaring the
//     exact type over that carrier would be the #361 silent-write class on
//     top of a wrong declaration. The repair is an exact integer accumulator
//     in the window operator — `decimalFrameAcc` reads `DecimalData` directly
//     and needs a per-type cell reader, `windowOutputColumn` needs the (p,s)
//     for an integer input, and `SUM(int4) -> bigint` needs an INT64 output
//     path neither frame evaluator has — plus the same in
//     `window_global.go`'s three sites and the spilled path.
//
// This is the CENSUS the deferral is recorded as, replacing two sampled pins
// with the full cross of five window aggregates and six widths. Every `want`
// is what wadjet answers on all four arms; every DIVERGENT cell names
// PostgreSQL's own answer in `why`, so the day the accumulator becomes exact
// the cell FAILS and deleting it is the fix's proof. A cell with no `why`
// already agrees with PostgreSQL and is a control: it fails if the repair
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

	cases := []f1Case{
		// SUM — the family #813 is about. The two DECIMAL widths already
		// answer PostgreSQL's type and digits (#586); the four others do not.
		{
			name: "813 SUM(int4) OVER ()",
			sql:  win("SUM", "w_i32"),
			want: "cols=[v:FLOAT64] rows=1 | 2.164260874e+09",
			why: "PostgreSQL 17 declares bigint and answers 2164260874; the GROUPED " +
				"spelling below is INT64 2164260874. exec.windowAccOutputType gives an " +
				"integer input a FLOAT64 accumulator. DEFERRED (ADR-0012).",
		},
		{name: "813 control: SUM(int4) grouped", sql: grp("SUM", "w_i32"),
			want: "cols=[v:INT64] rows=1 | 2164260874"},
		{
			name: "813 SUM(int8) OVER ()",
			sql:  win("SUM", "w_i64"),
			want: "cols=[v:FLOAT64] rows=1 | 9.007201419001868e+15",
			why: "PostgreSQL 17 declares numeric and answers 9007201419001868, which " +
				"the GROUPED spelling below answers exactly. DEFERRED (ADR-0012).",
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
			name: "813 AVG(int4) OVER ()",
			sql:  win("AVG", "w_i32"),
			want: "cols=[v:FLOAT64] rows=1 | 2.7053260925e+08",
			why: "PostgreSQL 17 declares numeric and answers 270532609.25000000. " +
				"DEFERRED (ADR-0012).",
		},
		{
			name: "813 AVG(int8) OVER () — the DIGITS, not only the type",
			sql:  win("AVG", "w_i64"),
			want: "cols=[v:FLOAT64] rows=1 | 1.0008001576668742e+15",
			why: "PostgreSQL 17 answers 1000800157666874.2222 numeric and so does the " +
				"GROUPED spelling below; the window's float64 accumulator has lost the " +
				"fraction. DEFERRED (ADR-0012).",
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
	}
	f1Run(t, arms, cases)
}
