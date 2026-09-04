package expr

import (
	"testing"
)

// #855: DATE_TRUNC, WIDTH_BUCKET, SPLIT_PART and CHR manufactured a value
// where PostgreSQL raises. These cells sit beside
// TestIntegerConversionsRaiseInsteadOfWrapping and are read the same way:
// every code and message is live postgres:17.11 with VERBOSITY verbose,
// recorded in the arc's ROUND0 before the fix was written.

func TestScalarFunctionsRaiseWherePostgresRaises(t *testing.T) {
	b := castRefusalBatch(t)
	ts := &Cast{Operand: &Lit{Val: "2023-05-17 13:24:35"}, DestType: "timestamp"}
	for _, c := range []struct {
		name       string
		expr       Expr
		state, msg string
	}{
		{"date_trunc_unknown_unit",
			&FuncCall{Name: "date_trunc", Args: []Expr{&Lit{Val: "bogus"}, ts}},
			"22023", `unit "bogus" not recognized for type timestamp without time zone`},
		// `epoch`, `doy` and `dow` are EXTRACT's fields, and PostgreSQL
		// refuses every one of them for DATE_TRUNC — measured, because the
		// accept-set is the whole content of this refusal and guessing it
		// would have refused units the server answers.
		{"date_trunc_epoch_is_not_a_truncation_unit",
			&FuncCall{Name: "date_trunc", Args: []Expr{&Lit{Val: "epoch"}, ts}},
			"22023", `unit "epoch" not recognized for type timestamp without time zone`},
		{"date_trunc_doy_is_not_a_truncation_unit",
			&FuncCall{Name: "date_trunc", Args: []Expr{&Lit{Val: "doy"}, ts}},
			"22023", `unit "doy" not recognized for type timestamp without time zone`},
		{"width_bucket_zero_count",
			&FuncCall{Name: "width_bucket", Args: []Expr{
				&Lit{Val: 1.0}, &Lit{Val: 0.0}, &Lit{Val: 10.0}, &Lit{Val: int64(0)}}},
			"2201G", "count must be greater than zero"},
		{"width_bucket_negative_count",
			&FuncCall{Name: "width_bucket", Args: []Expr{
				&Lit{Val: 1.0}, &Lit{Val: 0.0}, &Lit{Val: 10.0}, &Lit{Val: int64(-1)}}},
			"2201G", "count must be greater than zero"},
		{"width_bucket_equal_bounds",
			&FuncCall{Name: "width_bucket", Args: []Expr{
				&Lit{Val: 1.0}, &Lit{Val: 5.0}, &Lit{Val: 5.0}, &Lit{Val: int64(3)}}},
			"2201G", "lower bound cannot equal upper bound"},
		{"split_part_zero_position",
			&FuncCall{Name: "split_part", Args: []Expr{
				&Lit{Val: "a,b,c"}, &Lit{Val: ","}, &Lit{Val: int64(0)}}},
			"22023", "field position must not be zero"},
		{"chr_nul",
			&FuncCall{Name: "chr", Args: []Expr{&Lit{Val: int64(0)}}},
			"54000", "null character not permitted"},
		{"chr_negative",
			&FuncCall{Name: "chr", Args: []Expr{&Lit{Val: int64(-1)}}},
			"22023", "character number must be positive"},
		{"chr_past_the_unicode_range",
			&FuncCall{Name: "chr", Args: []Expr{&Lit{Val: int64(1114112)}}},
			"54000", "requested character too large for encoding: 1114112"},
	} {
		t.Run(c.name, func(t *testing.T) {
			state, msg := recoverFatalEvalForTest(t, func() { c.expr.Eval(b, 0) })
			if state != c.state || msg != c.msg {
				t.Errorf("raised [%s] %s, want [%s] %s (live PostgreSQL 17.11)",
					state, msg, c.state, c.msg)
			}
		})
	}
}

// The BOUNDARY (protocol rule 11): every argument just inside the domain still
// ANSWERS, and answers what the server answers. A refusal PostgreSQL does not
// make is as much a divergence as a value it does not produce, and the
// DATE_TRUNC unit set is where that risk lives — the accepted set on the
// server is thirteen units and this engine knew eight, so refusing the
// unrecognized ones without adding the other five would have turned five right
// (if NULL) answers into five wrong refusals.
func TestScalarFunctionsStillAnswerInsideTheirDomain(t *testing.T) {
	b := castRefusalBatch(t)
	ts := &Cast{Operand: &Lit{Val: "2023-05-17 13:24:35"}, DestType: "timestamp"}
	for _, c := range []struct {
		name string
		expr Expr
		want any
	}{
		// The thirteen units PostgreSQL 17.11 accepts for a timestamp, each
		// measured on the server. This engine's instants are epoch
		// MILLISECONDS, so milliseconds and microseconds are the identity.
		{"microseconds", &FuncCall{Name: "date_trunc", Args: []Expr{&Lit{Val: "microseconds"}, ts}},
			"2023-05-17T13:24:35Z"},
		{"milliseconds", &FuncCall{Name: "date_trunc", Args: []Expr{&Lit{Val: "milliseconds"}, ts}},
			"2023-05-17T13:24:35Z"},
		{"second", &FuncCall{Name: "date_trunc", Args: []Expr{&Lit{Val: "second"}, ts}},
			"2023-05-17T13:24:35Z"},
		{"minute", &FuncCall{Name: "date_trunc", Args: []Expr{&Lit{Val: "minute"}, ts}},
			"2023-05-17T13:24:00Z"},
		{"hour", &FuncCall{Name: "date_trunc", Args: []Expr{&Lit{Val: "hour"}, ts}},
			"2023-05-17T13:00:00Z"},
		{"day", &FuncCall{Name: "date_trunc", Args: []Expr{&Lit{Val: "day"}, ts}},
			"2023-05-17T00:00:00Z"},
		{"week", &FuncCall{Name: "date_trunc", Args: []Expr{&Lit{Val: "week"}, ts}},
			"2023-05-15T00:00:00Z"},
		{"month", &FuncCall{Name: "date_trunc", Args: []Expr{&Lit{Val: "month"}, ts}},
			"2023-05-01T00:00:00Z"},
		{"quarter", &FuncCall{Name: "date_trunc", Args: []Expr{&Lit{Val: "quarter"}, ts}},
			"2023-04-01T00:00:00Z"},
		{"year", &FuncCall{Name: "date_trunc", Args: []Expr{&Lit{Val: "year"}, ts}},
			"2023-01-01T00:00:00Z"},
		{"decade", &FuncCall{Name: "date_trunc", Args: []Expr{&Lit{Val: "decade"}, ts}},
			"2020-01-01T00:00:00Z"},
		// PostgreSQL's centuries and millennia START at year 1, so 2023 falls
		// in the century beginning 2001 and the millennium beginning 2001 —
		// not 2000. Measured; the arithmetic that "looks right" is wrong here.
		{"century", &FuncCall{Name: "date_trunc", Args: []Expr{&Lit{Val: "century"}, ts}},
			"2001-01-01T00:00:00Z"},
		{"millennium", &FuncCall{Name: "date_trunc", Args: []Expr{&Lit{Val: "millennium"}, ts}},
			"2001-01-01T00:00:00Z"},
		// Case-insensitive, as on the server.
		{"upper_case_unit", &FuncCall{Name: "date_trunc", Args: []Expr{&Lit{Val: "MONTH"}, ts}},
			"2023-05-01T00:00:00Z"},
		{"width_bucket_in_range", &FuncCall{Name: "width_bucket", Args: []Expr{
			&Lit{Val: 1.0}, &Lit{Val: 0.0}, &Lit{Val: 10.0}, &Lit{Val: int64(5)}}}, int32(1)},
		{"width_bucket_below_the_low_bound", &FuncCall{Name: "width_bucket", Args: []Expr{
			&Lit{Val: -1.0}, &Lit{Val: 0.0}, &Lit{Val: 10.0}, &Lit{Val: int64(5)}}}, int32(0)},
		{"width_bucket_at_or_above_the_high_bound", &FuncCall{Name: "width_bucket", Args: []Expr{
			&Lit{Val: 10.0}, &Lit{Val: 0.0}, &Lit{Val: 10.0}, &Lit{Val: int64(5)}}}, int32(6)},
		{"width_bucket_count_of_one", &FuncCall{Name: "width_bucket", Args: []Expr{
			&Lit{Val: 5.0}, &Lit{Val: 0.0}, &Lit{Val: 10.0}, &Lit{Val: int64(1)}}}, int32(1)},
		// SPLIT_PART's NEGATIVE positions, which PostgreSQL has counted from
		// the END since version 14 and which this engine answered as the
		// empty string for every one of them.
		{"split_part_last", &FuncCall{Name: "split_part", Args: []Expr{
			&Lit{Val: "a,b,c"}, &Lit{Val: ","}, &Lit{Val: int64(-1)}}}, "c"},
		{"split_part_second_from_last", &FuncCall{Name: "split_part", Args: []Expr{
			&Lit{Val: "a,b,c"}, &Lit{Val: ","}, &Lit{Val: int64(-2)}}}, "b"},
		{"split_part_first_from_the_end", &FuncCall{Name: "split_part", Args: []Expr{
			&Lit{Val: "a,b,c"}, &Lit{Val: ","}, &Lit{Val: int64(-3)}}}, "a"},
		// Past the end in either direction is the EMPTY STRING on the server,
		// not a refusal and not NULL.
		{"split_part_past_the_start", &FuncCall{Name: "split_part", Args: []Expr{
			&Lit{Val: "a,b,c"}, &Lit{Val: ","}, &Lit{Val: int64(-4)}}}, ""},
		{"split_part_past_the_end", &FuncCall{Name: "split_part", Args: []Expr{
			&Lit{Val: "a,b,c"}, &Lit{Val: ","}, &Lit{Val: int64(4)}}}, ""},
		{"split_part_first", &FuncCall{Name: "split_part", Args: []Expr{
			&Lit{Val: "a,b,c"}, &Lit{Val: ","}, &Lit{Val: int64(1)}}}, "a"},
		{"chr_ascii", &FuncCall{Name: "chr", Args: []Expr{&Lit{Val: int64(65)}}}, "A"},
		{"chr_latin1", &FuncCall{Name: "chr", Args: []Expr{&Lit{Val: int64(233)}}}, "é"},
		// The highest code point the encoding takes: one below the refusal.
		{"chr_at_the_top_of_the_range",
			&FuncCall{Name: "chr", Args: []Expr{&Lit{Val: int64(0x10FFFF)}}}, "\U0010FFFF"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := c.expr.Eval(b, 0); got != c.want {
				t.Errorf("= %T(%v), want %T(%v) (live PostgreSQL 17.11). A refusal PostgreSQL "+
					"does not make is as much a divergence as a value it does not produce (#855)",
					got, got, c.want, c.want)
			}
		})
	}
	// A NULL argument stays NULL rather than becoming a refusal: PostgreSQL's
	// strict functions answer NULL for a NULL input, and the refusals above
	// must not have swallowed that.
	for _, c := range []struct {
		name string
		expr Expr
	}{
		{"date_trunc_null_instant", &FuncCall{Name: "date_trunc",
			Args: []Expr{&Lit{Val: "day"}, &Lit{Val: nil}}}},
		{"width_bucket_null_count", &FuncCall{Name: "width_bucket", Args: []Expr{
			&Lit{Val: 1.0}, &Lit{Val: 0.0}, &Lit{Val: 10.0}, &Lit{Val: nil}}}},
		{"split_part_null_position", &FuncCall{Name: "split_part", Args: []Expr{
			&Lit{Val: "a,b"}, &Lit{Val: ","}, &Lit{Val: nil}}}},
		{"chr_null", &FuncCall{Name: "chr", Args: []Expr{&Lit{Val: nil}}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := c.expr.Eval(b, 0); got != nil {
				t.Errorf("= %T(%v), want NULL — a strict function answers NULL for a NULL "+
					"argument on the server, refusals or not", got, got)
			}
		})
	}
}
