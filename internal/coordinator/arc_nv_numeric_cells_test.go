package coordinator

// The coverage table, one row per (family × position × spelling) the arc
// touched, plus the controls that say the fix stopped where it should and the
// pins that record what it deliberately did not close.
//
// EXCLUDED DIMENSIONS, and why each is somewhere else:
//
//   - The AVG digit COUNT over an integer or a DECIMAL. PostgreSQL's
//     avg(numeric) keeps sixteen significant digits and this engine keeps
//     scale+4 (batch.AvgScale); both are exact to min(scale) and agree there.
//     ADR-0024 item 3's recorded divergence, gated by the decimal suite.
//   - The int4 SUPERSET. `2147483647 + 1` is `integer out of range` on the
//     server and 2147483648 here, because every integer this engine computes
//     is carried in an int64 (ADR-0024's recorded widening). Gated by
//     expr.TestIntegerDomain*.
//   - The DECIMAL CARRIER's range. `d3810 * 2` and `AVG(d3810)` are 22003
//     here and answers on the server's unbounded numeric — ADR-0024 item 3's
//     recorded trade, pinned in the PostgreSQL corpus as
//     `WideDecimalSquaredRowCount`.
//   - WIRE declarations. What OID a column goes out under is arc ND's; this
//     gate reads the Go BOX, which is what decides the VALUE.
//   - The REAL ARITHMETIC width. `r + 1.0::real` is 16777216 on the server
//     and 16777217 here, because arithmetic over two reals computes in
//     float64. It is a residual of this arc, recorded with its shape rather
//     than gated here — closing it needs the declaration layer arc ND owns.
//   - The float `%` operator. PostgreSQL has no `double precision % double
//     precision` at all, so wadjet's answer is a superset with no oracle.
func nvCells() []nvCell {
	const (
		ovf   = "22003"
		nvOvf = nvOvfTable
		nvSpc = nvSpecTable
		nvEdg = nvEdgeTable
		nvRl  = nvRealTable
		nvRet = nvRetTable
	)
	return []nvCell{
		// ------------------------------------------------------------------
		// #1082 — the float8 RANGE. Every one of these is `22003 value out of
		// range: overflow` on PostgreSQL 17.11 and answered ±Infinity here,
		// and a CTAS stored it.
		{name: "1082/product_of_a_column_and_a_literal",
			sql: "SELECT f8 * 10 AS v FROM " + nvOvf + " WHERE id=1", wantState: ovf},
		{name: "1082/product_of_two_columns",
			sql: "SELECT f8 * f8 AS v FROM " + nvOvf + " WHERE id=1", wantState: ovf},
		{name: "1082/sum_of_two_columns",
			sql: "SELECT f8 + f8 AS v FROM " + nvOvf + " WHERE id=1", wantState: ovf},
		{name: "1082/quotient_by_a_fraction",
			sql: "SELECT f8 / 0.5 AS v FROM " + nvOvf + " WHERE id=1", wantState: ovf},
		{name: "1082/negated_then_multiplied",
			sql: "SELECT -f8 * 10 AS v FROM " + nvOvf + " WHERE id=1", wantState: ovf},
		{name: "1082/under_a_cast",
			sql:       "SELECT CAST(f8 AS DOUBLE PRECISION) * 10 AS v FROM " + nvOvf + " WHERE id=1",
			wantState: ovf},
		// float8mul and float8div carry an UNDERFLOW rule too: a product that
		// flushes to zero from two non-zero operands is 22003, measured.
		{name: "1082/product_that_flushes_to_zero",
			sql: "SELECT f8 * f8 AS v FROM " + nvOvf + " WHERE id=3", wantState: ovf},
		{name: "1082/quotient_that_flushes_to_zero",
			sql: "SELECT f8 / 1e300 AS v FROM " + nvOvf + " WHERE id=3", wantState: ovf},
		// The POSITIONS: an aggregate over the overflowing expression, a
		// grouped one, a windowed one and a scalar subquery all refuse, which
		// is what makes the rule the EXPRESSION's rather than the
		// projection's.
		{name: "1082/aggregated", sql: "SELECT SUM(f8 * 10) AS v FROM " + nvOvf + " WHERE id=1",
			wantState: ovf},
		{name: "1082/grouped", sql: "SELECT g, MAX(f8 * 10) AS v FROM " + nvOvf + " GROUP BY g",
			wantState: ovf},
		{name: "1082/windowed", sql: "SELECT SUM(f8 * 10) OVER () AS v FROM " + nvOvf + " WHERE id=1",
			wantState: ovf},
		{name: "1082/scalar_subquery",
			sql:       "SELECT (SELECT f8 * 10 FROM " + nvOvf + " WHERE id=1) AS v FROM " + nvOvf + " WHERE id=4",
			wantState: ovf},
		// The float SUM's own range: sum(float8) is float8pl row by row on the
		// server and raises there too, in all three spellings.
		{name: "1082/sum_leaves_the_type",
			sql: "SELECT SUM(f8) AS v FROM " + nvOvf + " WHERE g=1", wantState: ovf},
		{name: "1082/avg_leaves_the_type",
			sql: "SELECT AVG(f8) AS v FROM " + nvOvf + " WHERE g=1", wantState: ovf},
		{name: "1082/grouped_sum_leaves_the_type",
			sql: "SELECT g, SUM(f8) AS v FROM " + nvOvf + " GROUP BY g", wantState: ovf},
		{name: "1082/windowed_sum_leaves_the_type",
			sql: "SELECT SUM(f8) OVER () AS v FROM " + nvOvf + " WHERE g=1", wantState: ovf},
		{name: "1082/windowed_avg_leaves_the_type",
			sql: "SELECT AVG(f8) OVER () AS v FROM " + nvOvf + " WHERE g=1", wantState: ovf},
		// THE CONTROLS. The refusal is per QUERY, not per table: the group
		// that does not overflow still answers, and so does the column itself.
		{name: "1082/control_the_finite_group_answers",
			sql: "SELECT SUM(f8) AS v FROM " + nvOvf + " WHERE g=2", want: "v=2.5<f8>"},
		{name: "1082/control_the_column_reads_back",
			sql: "SELECT f8 AS v FROM " + nvOvf + " WHERE id=1", want: "v=1e+308<f8>"},

		// ------------------------------------------------------------------
		// #1082's operand exemption. An infinity that ARRIVES is a value on
		// both engines — float8pl exempts an infinite input — so none of
		// these refuses. Without the exemption the rule would refuse a column
		// PostgreSQL stores.
		{name: "1082/infinity_times_ten",
			sql: "SELECT f8 * 10 AS v FROM " + nvSpc + " WHERE id=1", want: "v=+Inf<f8>"},
		{name: "1082/infinity_plus_one",
			sql: "SELECT f8 + 1 AS v FROM " + nvSpc + " WHERE id=1", want: "v=+Inf<f8>"},
		{name: "1082/infinity_over_a_fraction",
			sql: "SELECT f8 / 0.5 AS v FROM " + nvSpc + " WHERE id=1", want: "v=+Inf<f8>"},
		{name: "1082/infinity_times_zero_is_NaN",
			sql: "SELECT f8 * 0 AS v FROM " + nvSpc + " WHERE id=1", want: "v=NaN<f8>"},
		{name: "1082/negated_infinity",
			sql: "SELECT -f8 AS v FROM " + nvSpc + " WHERE id=1", want: "v=-Inf<f8>"},
		{name: "1082/NaN_times_ten",
			sql: "SELECT f8 * 10 AS v FROM " + nvSpc + " WHERE id=3", want: "v=NaN<f8>"},
		{name: "1082/infinity_minus_infinity_is_NaN",
			sql: "SELECT f8 + f8 AS v FROM " + nvSpc + " WHERE id=2", want: "v=-Inf<f8>"},
		{name: "1082/a_sum_over_an_infinite_input_is_infinite",
			sql: "SELECT SUM(f8) AS v FROM " + nvSpc + " WHERE id IN (1,4)", want: "v=+Inf<f8>"},
		{name: "1082/a_windowed_sum_over_one_too",
			sql:  "SELECT SUM(f8) OVER () AS v FROM " + nvSpc + " WHERE id IN (1,4)",
			want: "v=+Inf<f8>;v=+Inf<f8>"},
		{name: "1082/a_sum_over_both_infinities_is_NaN",
			sql: "SELECT SUM(f8) AS v FROM " + nvSpc + " WHERE g=1", want: "v=NaN<f8>"},

		// ------------------------------------------------------------------
		// #950 — SUM(real) accumulates at float4's width. 16777216 is 2^24,
		// where a real stops counting by ones, so the rows after it decide
		// whether the total was carried at float4's width or at float8's.
		// Three spellings answered three numbers; the server answers one.
		{name: "950/sum_over_a_real", sql: "SELECT SUM(r) AS v FROM " + nvRl,
			want: "v=1.6777224e+07<f4>",
			pin: map[string]string{
				"dag": "v=1.6777226e+07<f4>", "dagshuf": "v=1.6777226e+07<f4>",
				"morsel": "v=1.6777226e+07<f4>"},
			why: "1.6777226e+07 before the fix, on EVERY arm, because the total was " +
				"carried at float8's width and narrowed once at the store. The DAG " +
				"arms print it again for a different reason: they fold three partial " +
				"REAL totals, and at float4's width the association reaches the " +
				"eighth significant digit. Both are the float4 sum of these nine " +
				"values under different groupings — ADR-0013's nondeterminism class " +
				"9 at a narrower carrier, which PostgreSQL's own parallel aggregate " +
				"has too"},
		{name: "950/grouped", sql: "SELECT g, SUM(r) AS v FROM " + nvRl + " GROUP BY g",
			want: "g=1<i4>|v=1.677721e+07<f4>;g=2<i4>|v=14.25<f4>",
			pin: map[string]string{
				"dag":     "g=1<i4>|v=1.6777211e+07<f4>;g=2<i4>|v=14.25<f4>",
				"dagshuf": "g=1<i4>|v=1.6777211e+07<f4>;g=2<i4>|v=14.25<f4>",
				"morsel":  "g=1<i4>|v=1.6777211e+07<f4>;g=2<i4>|v=14.25<f4>"},
			why: "class 9 again, and note the SECOND group agrees exactly: the " +
				"association only moves a total that crossed 2^24"},
		{name: "950/distinct", sql: "SELECT SUM(DISTINCT r) AS v FROM " + nvRl,
			want: "v=1.6777212e+07<f4>"},
		{name: "950/through_a_derived_table",
			sql:  "SELECT SUM(v) AS v FROM (SELECT r AS v FROM " + nvRl + " WHERE g=1) x",
			want: "v=1.677721e+07<f4>",
			pin: map[string]string{
				"dag": "v=1.6777211e+07<f4>", "dagshuf": "v=1.6777211e+07<f4>",
				"morsel": "v=1.6777211e+07<f4>"},
			why: "class 9; a derived table does not change how the partials fold"},
		// The whole table rather than a filtered slice: a WHERE beside the
		// window restricts the PARTITION on both engines, so the cell would
		// then compare two one-row windows and prove nothing. A filter ABOVE
		// the window (in a derived table or a CTE) keeps the full partition on
		// both engines too — measured, in case this reads like a hedge.
		{name: "950/windowed_over_the_partition",
			sql: "SELECT g, SUM(r) OVER (PARTITION BY g) AS v FROM " + nvRl,
			want: "g=1<i4>|v=1.677721e+07<f4>;g=1<i4>|v=1.677721e+07<f4>;" +
				"g=1<i4>|v=1.677721e+07<f4>;g=1<i4>|v=1.677721e+07<f4>;" +
				"g=1<i4>|v=1.677721e+07<f4>;g=2<i4>|v=14.25<f4>;g=2<i4>|v=14.25<f4>;" +
				"g=2<i4>|v=14.25<f4>;g=2<i4>|v=14.25<f4>",
			why: "the DIGITS are the grouped spelling's and have not moved; the BOX " +
				"is an f4 since #1118 gave the window column the real declaration " +
				"the server gives it. The whole table, no filter: a WHERE beside the " +
				"window would restrict the PARTITION, which is what both engines do " +
				"and what would make the cell vacuous"},
		{name: "950/windowed_running_total",
			sql: "SELECT id, SUM(r) OVER (ORDER BY id) AS v FROM " + nvRl + " ORDER BY id",
			want: "id=1<i8>|v=2<f4>;id=2<i8>|v=2.0999999046325684<f4>;" +
				"id=3<i8>|v=14.850000381469727<f4>;id=4<i8>|v=1.677723e+07<f4>;" +
				"id=5<i8>|v=1.677721e+07<f4>;id=6<i8>|v=1.677721e+07<f4>;" +
				"id=7<i8>|v=1.6777222e+07<f4>;id=8<i8>|v=1.6777224e+07<f4>;" +
				"id=9<i8>|v=1.6777224e+07<f4>",
			why: "psql: 2, 2.1, 14.85, 1.677723e+07, 1.677721e+07, 1.677721e+07, " +
				"1.6777222e+07, 1.6777224e+07, 1.6777224e+07 — the REAL running " +
				"total, in the f4 box #1118 gave the window column " +
				"(2.0999999046325684 IS float64(float32(2.1)), which is what the " +
				"renderer prints for a float32 carrying float32(2.1))"},
		{name: "950/min_and_max_are_untouched",
			sql:  "SELECT MIN(r) AS lo, MAX(r) AS hi FROM " + nvRl,
			want: "lo=-20<f4>|hi=1.6777216e+07<f4>"},
		{name: "950/avg_over_a_real_stays_double_precision",
			sql:    "SELECT AVG(r) AS v FROM " + nvRl,
			digits: 11,
			want:   "v=2097153.1375<f8>",
			why: "psql prints 2097153.1375; avg(real) is double precision on the " +
				"server (#760) and narrowing it here would absorb the 0.1 it keeps"},

		// ------------------------------------------------------------------
		// #1000 — PORT and PROTOCOL arithmetic is int4 arithmetic. The bare
		// column already followed int4's rules; a COMPUTED one did not, so the
		// SUM accumulated in float64 and the DIVISION did not truncate.
		{name: "1000/a_bare_port_is_int4",
			sql: "SELECT pt AS v FROM " + nvEdg + " WHERE id=1", want: "v=65535<i8>"},
		// The BOX is an i4 since #1070: `port * 1` is `integer` on the
		// server, and these cells are about the NUMBER — 65535 rather than
		// the 65535.0 the float path answered.
		{name: "1000/port_times_one",
			sql: "SELECT pt * 1 AS v FROM " + nvEdg + " WHERE id=1", want: "v=65535<i8>"},
		{name: "1000/port_plus_zero",
			sql: "SELECT pt + 0 AS v FROM " + nvEdg + " WHERE id=1", want: "v=65535<i4>"},
		{name: "1000/a_negated_port",
			sql: "SELECT -pt AS v FROM " + nvEdg + " WHERE id=1", want: "v=-65535<i8>"},
		{name: "1000/abs_of_a_port_answers_in_its_own_domain",
			sql: "SELECT ABS(pt) AS v FROM " + nvEdg + " WHERE id=1", want: "v=65535<i4>"},
		{name: "1000/protocol_division_TRUNCATES",
			sql:  "SELECT pr / 2 AS v FROM " + nvEdg + " WHERE id=1",
			want: "v=127<i8>", why: "127.5 before — this was the wrong VALUE"},
		{name: "1000/sum_over_a_computed_port",
			sql: "SELECT SUM(pt * 1) AS v FROM " + nvEdg + " WHERE id=1", want: "v=65535<i8>"},
		{name: "1000/grouped",
			sql:  "SELECT g, SUM(pt * 1) AS v FROM " + nvEdg + " GROUP BY g",
			want: "g=1<i4>|v=65615<i8>;g=2<i4>|v=444<i8>"},
		{name: "1000/windowed",
			sql: "SELECT SUM(pt * 1) OVER () AS v FROM " + nvEdg + " WHERE id=1", want: "v=65535<i8>"},
		{name: "1000/a_negated_port_under_a_window",
			sql: "SELECT SUM(-pt) OVER () AS v FROM " + nvEdg + " WHERE id=1", want: "v=-65535<i8>",
			why: "this answered NULL"},
		{name: "1000/sum_over_abs_of_a_protocol",
			sql: "SELECT SUM(ABS(pr)) AS v FROM " + nvEdg + " WHERE id=1", want: "v=255<i8>"},
		{name: "1000/the_truncating_division_aggregates",
			sql:  "SELECT g, SUM(pr / 2) AS v FROM " + nvEdg + " GROUP BY g",
			want: "g=1<i4>|v=130<i8>;g=2<i4>|v=8<i8>"},
		{name: "1000/the_truncating_division_through_a_subquery",
			sql:  "SELECT (SELECT pr / 2 FROM " + nvEdg + " WHERE id=1) AS v FROM " + nvEdg + " WHERE id=4",
			want: "v=127<i8>"},

		// ------------------------------------------------------------------
		// #1037 — a wide DECIMAL literal under a CAST. 9007199254740993.25 is
		// 2^53+1 plus a quarter: past a double's reach in both halves, so the
		// float64 box the cast was handed had already lost the fraction AND
		// the last integer digit.
		{name: "1037/the_cast_itself",
			sql:  "SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)) AS v FROM " + nvEdg + " WHERE id=1",
			want: "v=9007199254740993.25", why: "9007199254740994.00 before"},
		{name: "1037/through_a_scalar_subquery",
			sql:  "SELECT (SELECT CAST(9007199254740993.25 AS DECIMAL(30,2))) AS v FROM " + nvEdg + " WHERE id=1",
			want: "v=9007199254740993.25"},
		{name: "1037/summed_over_the_scalar_subquery",
			sql: "SELECT SUM((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) AS v FROM " +
				nvEdg + " WHERE id<4",
			want: "v=27021597764222979.75", why: "27021597764222982 before — the filing's own cell"},
		{name: "1037/min_over_it",
			sql: "SELECT MIN((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) AS v FROM " +
				nvEdg + " WHERE id<4", want: "v=9007199254740993.25"},
		{name: "1037/max_over_it",
			sql: "SELECT MAX((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) AS v FROM " +
				nvEdg + " WHERE id<4", want: "v=9007199254740993.25"},
		{name: "1037/windowed",
			sql: "SELECT SUM((SELECT CAST(9007199254740993.25 AS DECIMAL(30,2)))) OVER () AS v FROM " +
				nvEdg + " WHERE id=1", want: "v=9007199254740993.25"},
		{name: "1037/grouped",
			sql: "SELECT g, SUM(CAST(9007199254740993.25 AS DECIMAL(30,2))) AS v FROM " +
				nvEdg + " GROUP BY g",
			want: "g=1<i4>|v=18014398509481986.50;g=2<i4>|v=27021597764222979.75"},
		{name: "1037/a_wider_scale_keeps_the_same_digits",
			sql:  "SELECT CAST(9007199254740993.25 AS DECIMAL(38,10)) AS v FROM " + nvEdg + " WHERE id=1",
			want: "v=9007199254740993.2500000000"},
		{name: "1037/the_bare_destination_takes_the_literal's_own_scale",
			sql:  "SELECT CAST(9007199254740993.25 AS DECIMAL) AS v FROM " + nvEdg + " WHERE id=1",
			want: "v=9007199254740993.25"},
		{name: "1037/summed_under_the_bare_destination",
			sql:  "SELECT SUM(CAST(9007199254740993.25 AS DECIMAL)) AS v FROM " + nvEdg + " WHERE id<4",
			want: "v=27021597764222979.75"},
		{name: "1037/a_negated_literal_is_folded_with_its_text",
			sql:  "SELECT CAST(-9007199254740993.25 AS DECIMAL(30,2)) AS v FROM " + nvEdg + " WHERE id=1",
			want: "v=-9007199254740993.25"},
		{name: "1037/control_arithmetic_over_the_same_literal_was_already_exact",
			sql:  "SELECT 9007199254740993.25 + 0 AS v FROM " + nvEdg + " WHERE id=1",
			want: "v=9007199254740993.25",
			why:  "arithmetic reads Lit.Text (ADR-0012 item 6); the cast now reads it too"},

		// ------------------------------------------------------------------
		// The CONTROLS for the carriers this arc did not move: 2^53+1 in an
		// int8 column and a DECIMAL(30,2) past a double's reach, read and
		// summed in every position.
		{name: "control/an_int8_past_2^53_reads_back",
			sql: "SELECT i8 AS v FROM " + nvEdg + " WHERE id=1", want: "v=9007199254740993<i8>"},
		{name: "control/and_sums_exactly",
			sql: "SELECT SUM(i8) AS v FROM " + nvEdg + " WHERE id=1", want: "v=9007199254740993"},
		{name: "control/through_a_computed_argument",
			sql: "SELECT SUM(i8 * 1) AS v FROM " + nvEdg + " WHERE id=1", want: "v=9007199254740993"},
		{name: "control/negated",
			sql: "SELECT SUM(-i8) AS v FROM " + nvEdg + " WHERE id=1", want: "v=-9007199254740993"},
		{name: "control/a_wide_decimal_column_reads_back",
			sql: "SELECT d302 AS v FROM " + nvEdg + " WHERE id=1", want: "v=9007199254740993.25"},
		{name: "control/and_sums_exactly_too",
			sql: "SELECT SUM(d302) AS v FROM " + nvEdg + " WHERE id=1", want: "v=9007199254740993.25"},
		{name: "control/through_a_computed_argument_as_well",
			sql: "SELECT SUM(d302 * 1) AS v FROM " + nvEdg + " WHERE id=1", want: "v=9007199254740993.25"},

		// ------------------------------------------------------------------
		// REVIEW N1 — a MOVING frame is RECOMPUTED, at float8's width as well
		// as float4's. PostgreSQL has no inverse transition for either float
		// sum, so a frame whose lower end advanced is recomputed there;
		// subtracting the departing row instead loses whatever the addition
		// absorbed. Over 1e16, 1, 1 the one-preceding sum answered 1 where
		// 17.11 answers 2, and its average 0.5 where 17.11 answers 1 — a
		// wrong NUMBER, pre-existing at c34cdbcb, and the half this arc's
		// first cut fixed was the real one only.
		//
		// Every `want` below is psql on 17.11 over these five rows. The REAL
		// cells are here for the same reason the float8 ones are: one rule,
		// two widths, and a fix to either alone would let them disagree.
		{name: "N1/f8_sum_one_preceding",
			sql: "SELECT id, SUM(f8) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM " +
				nvRet + " ORDER BY id",
			want: "id=1<i8>|v=1e+16<f8>;id=2<i8>|v=1e+16<f8>;id=3<i8>|v=2<f8>;" +
				"id=4<i8>|v=1e+16<f8>;id=5<i8>|v=1.0000000000000002e+16<f8>",
			why: "row 3 answered 1 before: 1e16 + 1 + 1 − 1e16 on a float8 carrier"},
		{name: "N1/f8_avg_one_preceding",
			sql: "SELECT id, AVG(f8) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM " +
				nvRet + " ORDER BY id",
			want: "id=1<i8>|v=1e+16<f8>;id=2<i8>|v=5e+15<f8>;id=3<i8>|v=1<f8>;" +
				"id=4<i8>|v=5e+15<f8>;id=5<i8>|v=5.000000000000001e+15<f8>",
			why: "row 3 answered 0.5"},
		{name: "N1/f8_sum_two_preceding",
			sql: "SELECT id, SUM(f8) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS v FROM " +
				nvRet + " ORDER BY id",
			want: "id=1<i8>|v=1e+16<f8>;id=2<i8>|v=1e+16<f8>;id=3<i8>|v=1e+16<f8>;" +
				"id=4<i8>|v=1.0000000000000002e+16<f8>;id=5<i8>|v=1.0000000000000002e+16<f8>",
			why: "row 4 answered 1e+16 — a whole ulp of the total"},
		{name: "N1/f8_sum_current_and_following",
			sql: "SELECT id, SUM(f8) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS v FROM " +
				nvRet + " ORDER BY id",
			want: "id=1<i8>|v=1e+16<f8>;id=2<i8>|v=2<f8>;id=3<i8>|v=1e+16<f8>;" +
				"id=4<i8>|v=1.0000000000000002e+16<f8>;id=5<i8>|v=2<f8>",
			why: "a FOLLOWING frame moves its lower end too"},
		{name: "N1/f8_sum_preceding_and_following",
			sql: "SELECT id, SUM(f8) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS v FROM " +
				nvRet + " ORDER BY id",
			want: "id=1<i8>|v=1e+16<f8>;id=2<i8>|v=1e+16<f8>;" +
				"id=3<i8>|v=1.0000000000000002e+16<f8>;" +
				"id=4<i8>|v=1.0000000000000002e+16<f8>;" +
				"id=5<i8>|v=1.0000000000000002e+16<f8>"},
		// A RANGE frame with a VALUE offset is refused by the parser, loudly
		// and before this arc — so the frame form PostgreSQL answers
		// `1e+16; 1e+16; 2; 1e+16; 1.0000000000000002e+16` for is not a wrong
		// number here, it is a missing feature. The cell is a ratchet: the day
		// the parser accepts it, this fails and gets the server's values.
		{name: "N1/a_range_frame_with_an_offset_is_refused",
			sql: "SELECT id, SUM(f8) OVER (ORDER BY id RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM " +
				nvRet + " ORDER BY id",
			wantState: "42601",
			why: "psql answers 1e+16; 1e+16; 2; 1e+16; 1.0000000000000002e+16 — a " +
				"feature this parser declines, not a value it gets wrong"},
		{name: "N1/control_a_range_frame_without_an_offset_answers",
			sql: "SELECT id, SUM(f8) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS v FROM " +
				nvRet + " ORDER BY id",
			want: "id=1<i8>|v=1e+16<f8>;id=2<i8>|v=1e+16<f8>;id=3<i8>|v=1e+16<f8>;" +
				"id=4<i8>|v=2e+16<f8>;id=5<i8>|v=2e+16<f8>"},
		{name: "N1/f8_sum_within_a_partition",
			sql: "SELECT id, SUM(f8) OVER (PARTITION BY g ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM " +
				nvRet + " ORDER BY id",
			want: "id=1<i8>|v=1e+16<f8>;id=2<i8>|v=1e+16<f8>;id=3<i8>|v=2<f8>;" +
				"id=4<i8>|v=1e+16<f8>;id=5<i8>|v=1.0000000000000002e+16<f8>"},
		{name: "N1/f4_sum_one_preceding",
			sql: "SELECT id, SUM(f4) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM " +
				nvRet + " ORDER BY id",
			want: "id=1<i8>|v=1e+07<f4>;id=2<i8>|v=1.0000001e+07<f4>;id=3<i8>|v=2<f4>;" +
				"id=4<i8>|v=1.0000001e+07<f4>;id=5<i8>|v=1.0000002e+07<f4>",
			why: "the half the arc's first cut already fixed; it must stay fixed. The " +
				"box is an f4 since #1118 — the DIGITS are unchanged"},
		{name: "N1/f4_avg_one_preceding",
			sql: "SELECT id, AVG(f4) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM " +
				nvRet + " ORDER BY id",
			want: "id=1<i8>|v=1e+07<f8>;id=2<i8>|v=5.0000005e+06<f8>;id=3<i8>|v=1<f8>;" +
				"id=4<i8>|v=5.0000005e+06<f8>;id=5<i8>|v=5.000001e+06<f8>",
			why: "psql: 10000000; 5000000.5; 1; 5000000.5; 5000001 — avg(real) totals " +
				"at float8's width on both engines (#760), so the frame does not narrow"},
		{name: "N1/f4_sum_preceding_and_following",
			sql: "SELECT id, SUM(f4) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS v FROM " +
				nvRet + " ORDER BY id",
			want: "id=1<i8>|v=1.0000001e+07<f4>;id=2<i8>|v=1.0000002e+07<f4>;" +
				"id=3<i8>|v=1.0000002e+07<f4>;id=4<i8>|v=1.0000003e+07<f4>;" +
				"id=5<i8>|v=1.0000002e+07<f4>"},
		// The CONTROLS: a frame whose lower end never moves is still the same
		// running total in the same order, and the two aggregates that do not
		// accumulate are untouched by the change.
		{name: "N1/control_unbounded_preceding_is_a_running_total",
			sql: "SELECT id, SUM(f8) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS v FROM " +
				nvRet + " ORDER BY id",
			want: "id=1<i8>|v=1e+16<f8>;id=2<i8>|v=1e+16<f8>;id=3<i8>|v=1e+16<f8>;" +
				"id=4<i8>|v=2e+16<f8>;id=5<i8>|v=2e+16<f8>"},
		{name: "N1/control_min_over_a_moving_frame",
			sql: "SELECT id, MIN(f8) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM " +
				nvRet + " ORDER BY id",
			want: "id=1<i8>|v=1e+16<f8>;id=2<i8>|v=1<f8>;id=3<i8>|v=1<f8>;" +
				"id=4<i8>|v=1<f8>;id=5<i8>|v=2<f8>"},
		{name: "N1/control_count_over_a_moving_frame",
			sql: "SELECT id, COUNT(f8) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS v FROM " +
				nvRet + " ORDER BY id",
			want: "id=1<i8>|v=1<i8>;id=2<i8>|v=2<i8>;id=3<i8>|v=2<i8>;" +
				"id=4<i8>|v=2<i8>;id=5<i8>|v=2<i8>"},

		// ------------------------------------------------------------------
		// #712 and #764 — DEFERRED, re-measured here, and pinned so the
		// deferral is visible rather than remembered.
		//
		// They are ONE item and it is the CARRIER's: a wadjet DECIMAL vector
		// has ONE scale for the whole column (ADR-0018 §4) and PostgreSQL
		// gives a composite over a column and a constant typmod −1, which
		// prints every VALUE at its OWN scale. So every cell below is the
		// SAME NUMBER as the server's with a different count of trailing
		// zeros, and closing it needs a per-value render scale carried
		// through the vector, the spill run, the `.wshf` chunk and the
		// parquet leaf — a carrier arc, not a typing one.
		//
		// #712's own proposal — enforce 10^p at SetValueChecked /
		// SetComputedChecked — was implemented, measured and reverted before
		// this arc (ADR-0024 item 4), and the first cell is why: the value is
		// RIGHT today and the enforcement would make it a 22003 the server
		// answers. Right → loud is not a fix.
		//
		// The AUDIT #712 asks for, done: batch.DecimalColumn is constructed
		// in exactly one place (NewDecimalColumn, from NewVectorWithScale and
		// NewVectorLike), and both take the scale from their caller.
		// There is no second construction site to carry a precision to.
		{name: "712/a_fold_over_a_wide_decimal_and_an_integer_keeps_its_value",
			sql:  "SELECT GREATEST(d30, 100000000) AS v FROM " + nvFoldTable + " WHERE id=1",
			want: "v=100000000.000000000000000000000000000000",
			why: "psql: 100000000, under a bare `numeric` (pg_typeof, measured). The " +
				"same NUMBER with 30 trailing zeros — 39 digits under a " +
				"DECIMAL(38,30) declaration nothing enforces, which is #712's report. " +
				"Enforcing it turns this cell into a 22003 the server answers"},
		{name: "764/coalesce_with_a_finer_literal",
			sql: "SELECT id, COALESCE(d152, '12.3456789012345') AS v FROM " + nvFoldTable +
				" ORDER BY id",
			want: "id=1<i8>|v=12.7500000000000;id=2<i8>|v=12.3456789012345;" +
				"id=3<i8>|v=1.0000000000000;id=4<i8>|v=-3.5000000000000",
			why: "psql: 12.75; 12.3456789012345; 1.00; -3.50 — every value at its own " +
				"scale under typmod −1"},
		{name: "764/coalesce_with_an_integer_literal",
			sql:  "SELECT id, COALESCE(d152, '7') AS v FROM " + nvFoldTable + " ORDER BY id",
			want: "id=1<i8>|v=12.75;id=2<i8>|v=7.00;id=3<i8>|v=1.00;id=4<i8>|v=-3.50",
			why:  "psql: 12.75; 7; 1.00; -3.50 — only the literal's row differs"},
		{name: "764/least_against_a_coarser_literal",
			sql:  "SELECT id, LEAST(d152, '0.5') AS v FROM " + nvFoldTable + " ORDER BY id",
			want: "id=1<i8>|v=0.50;id=2<i8>|v=0.50;id=3<i8>|v=0.50;id=4<i8>|v=-3.50",
			why:  "psql: 0.5; 0.5; 0.5; -3.50"},
		{name: "764/a_case_whose_else_is_a_finer_literal",
			sql: "SELECT id, CASE WHEN g < 2 THEN d152 ELSE 0.125 END AS v FROM " +
				nvFoldTable + " ORDER BY id",
			want: "id=1<i8>|v=12.750;id=2<i8>|v=NULL;id=3<i8>|v=0.125;id=4<i8>|v=0.125",
			why: "psql: 12.75; NULL; 0.125; 0.125. Taking the DECLARED operands' scale " +
				"instead would give the column's rows exactly and leave 0.125 with " +
				"nowhere to go — a 22003 for 200 rows the server answers, which is " +
				"what #724's review round 1 measured and reverted"},
		{name: "764/control_the_column_alone_renders_at_its_own_scale",
			sql:  "SELECT id, d152 AS v FROM " + nvFoldTable + " ORDER BY id",
			want: "id=1<i8>|v=12.75;id=2<i8>|v=NULL;id=3<i8>|v=1.00;id=4<i8>|v=-3.50",
			why:  "identical to psql: no fold, no inflated scale"},
		{name: "764/control_NULLIF_keeps_argument_zero's_typmod",
			sql:  "SELECT id, NULLIF(d152, 0.5) AS v FROM " + nvFoldTable + " ORDER BY id",
			want: "id=1<i8>|v=12.75;id=2<i8>|v=NULL;id=3<i8>|v=1.00;id=4<i8>|v=-3.50",
			why: "NULLIF is PostgreSQL's one exception — it keeps argument 0's typmod, " +
				"not −1 — and this engine already matched it"},
		// The CARRIER's own range, the other half of the same trade: an
		// inflated scale spends the integer digits, so a shape PostgreSQL's
		// unbounded numeric answers is a 22003 here. Recorded in ADR-0024
		// item 3's carrier-limit list.
		{name: "712/arithmetic_over_the_wide_column_leaves_the_carrier",
			sql:       "SELECT d30 + 100000000 AS v FROM " + nvFoldTable + " WHERE id=1",
			wantState: ovf,
			why: "psql: 100000001.500000000000000000000000000000. At scale 30 an " +
				"Int128 holds 8 integer digits and this needs 9"},
		{name: "712/and_so_does_a_sum_over_the_fold",
			sql:       "SELECT SUM(GREATEST(d30, 100000000)) AS v FROM " + nvFoldTable,
			wantState: ovf,
			why:       "psql: 400000000 — four rows whose fold values carry scale 0 there"},
	}
}
