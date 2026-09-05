package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// nfLit is one composite holding a LITERAL finer, wider or smaller than the
// DECIMAL its typed operands agree on, with PostgreSQL 17.11's whole result:
// rows in id order separated by ';', "<NULL>" for a NULL.
//
// render, when set, is what wadjet prints instead — the SAME NUMBERS at the
// fold's scale rather than at each value's own. It is a pin: an entry that
// starts printing PostgreSQL's text fails and gets its render dropped.
//
// refused marks a value the finite carrier cannot hold at all.
type nfLit struct {
	name    string
	expr    string
	want    string
	render  string
	refused bool
}

// TestLiteralScaleInADecimalFold settles what a LITERAL contributes to a
// DECIMAL fold, which #724's review round 1 reopened.
//
// PostgreSQL gives a numeric CONSTANT typmod -1, so select_common_typmod over
// a column and a constant answers -1 — read off pg_attribute here: every
// composite below is bare `numeric` except NULLIF, which keeps argument 0's
// numeric(15,2), and wadjet already agreed with that one. An unconstrained
// numeric prints every VALUE at its own scale: 12.75 for the column's rows and
// 12.3456789012345 for the literal's, in one column.
//
// Wadjet's vector has ONE scale and cannot print two. The two rules available:
//
//	scale = max over the arms, literal included   the literal's rows are exact,
//	                                              the columns' carry trailing
//	                                              zeros (12.7500000000000)
//	scale = max over the DECLARED arms only       the columns' rows are exact,
//	                                              the literal has nowhere to go
//	                                              and the query raises 22003
//
// The first is what this engine does, and it is ADR-0012 item 12's rule for
// every arm of a set operation — "scale = max over the arms, the only choice
// that moves no value; a narrower one DROPS digits the wider arm holds". The
// second was implemented during the review and refuses queries PostgreSQL AND
// this engine's own main branch both answer: `CASE WHEN g < 3 THEN
// numeric(9,2) ELSE 0.125 END` is 200 rows on PostgreSQL and became a 22003
// under it, failing the pg-oracle corpus entry
// DecimalChoiceFractionalLiteralValue and #695's own
// TestDecimalBesideAnIntegerIsNumeric. A trailing zero is the same number; a
// refused query is no answer at all.
//
// So the residual is the RENDERING, it is pinned per entry, and it is #764.
//
// Both SPELLINGS take the identical rule — a quoted literal is `unknown` and
// resolved from its neighbours (#724), an unquoted one carries its own
// declaration, and both then contribute that spelling to the fold. Every shape
// below is written in both spellings for exactly that reason: they are one
// rule, and a fix to either alone would let them disagree, which is what
// #724's first round did.
//
// Three arms: single-process, the stage DAG, and a DAG with
// BroadcastBytesOverride=1. These are single-table projections, so the
// dispatch shape cannot change the answer — asserting that it does not is the
// point, since a declared type that survived one shape and not the other is
// exactly the class #633 was.
func TestLiteralScaleInADecimalFold(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) {
		c.BroadcastBytesOverride = 1
	})

	arms := []struct {
		name string
		run  func(string) (string, error)
	}{
		{"single", func(sql string) (string, error) {
			r, err := tmdRunSingle(ctx, single, sql)
			if err != nil {
				return "", err
			}
			return nfRows(r), nil
		}},
		{"dag", func(sql string) (string, error) {
			r, err := tmdRunDAG(ctx, coord, sql)
			if err != nil {
				return "", err
			}
			return nfRows(r), nil
		}},
		{"dag-shuffle", func(sql string) (string, error) {
			r, err := tmdRunDAG(ctx, coordB, sql)
			if err != nil {
				return "", err
			}
			return nfRows(r), nil
		}},
	}

	// n_d152 is numeric(15,2) holding 12.75, NULL, 1.00, -3.50 in id order;
	// n_d3810 is numeric(38,10) holding 12.7500000001, NULL, 1.0000000000,
	// -3.5000000000.
	const wide = "123456789012345678901234567890123456789.12345"
	for _, c := range []nfLit{
		// A literal FINER than the columns. Its own rows are exact on both
		// engines; the columns' rows are the pinned rendering.
		{"CoalesceFinerLiteral", "COALESCE(n_d152, '12.3456789012345')",
			"12.75;12.3456789012345;1.00;-3.50",
			"12.7500000000000;12.3456789012345;1.0000000000000;-3.5000000000000", false},
		{"CoalesceFinerLiteralUnquoted", "COALESCE(n_d152, 12.3456789012345)",
			"12.75;12.3456789012345;1.00;-3.50",
			"12.7500000000000;12.3456789012345;1.0000000000000;-3.5000000000000", false},
		{"CoalesceTinyLiteral", "COALESCE(n_d152, '0.00000000000001')",
			"12.75;0.00000000000001;1.00;-3.50",
			"12.75000000000000;0.00000000000001;1.00000000000000;-3.50000000000000", false},
		{"CoalesceOverTheWideColumn", "COALESCE(n_d3810, '12.3456789012345')",
			"12.7500000001;12.3456789012345;1.0000000000;-3.5000000000",
			"12.7500000001000;12.3456789012345;1.0000000000000;-3.5000000000000", false},
		{"GreatestFinerLiteral", "GREATEST(n_d152, '12.3456789012345')",
			"12.75;12.3456789012345;12.3456789012345;12.3456789012345",
			"12.7500000000000;12.3456789012345;12.3456789012345;12.3456789012345", false},
		{"GreatestFinerLiteralUnquoted", "GREATEST(n_d152, 12.3456789012345)",
			"12.75;12.3456789012345;12.3456789012345;12.3456789012345",
			"12.7500000000000;12.3456789012345;12.3456789012345;12.3456789012345", false},
		{"CaseFinerLiteral", "CASE WHEN id = 1 THEN n_d152 ELSE '12.3456789012345' END",
			"12.75;12.3456789012345;12.3456789012345;12.3456789012345",
			"12.7500000000000;12.3456789012345;12.3456789012345;12.3456789012345", false},
		{"CaseFinerLiteralUnquoted", "CASE WHEN id = 1 THEN n_d152 ELSE 12.3456789012345 END",
			"12.75;12.3456789012345;12.3456789012345;12.3456789012345",
			"12.7500000000000;12.3456789012345;12.3456789012345;12.3456789012345", false},
		// NULLIF's value is argument 0 and its typmod is argument 0's, on both
		// engines. With the literal in argument 0 the fold is the literal's.
		{"NullifLiteralIsArgumentZero", "NULLIF('12.3456789012345', n_d152)",
			"12.3456789012345;12.3456789012345;12.3456789012345;12.3456789012345", "", false},
		{"NullifKeepsArgumentZerosTypmod", "NULLIF(n_d152, '12.3456789012345')",
			"12.75;<NULL>;1.00;-3.50", "", false},

		// A literal the columns' scale already holds. Nothing moves and
		// nothing refuses — the control for the rule above.
		{"LeastSmallLiteral", "LEAST(n_d152, '0.5')", "0.5;0.5;0.5;-3.50",
			"0.50;0.50;0.50;-3.50", false},
		{"CoalesceIntegerLiteral", "COALESCE(n_d152, '7')", "12.75;7;1.00;-3.50",
			"12.75;7.00;1.00;-3.50", false},
		{"GreatestWidensTheIntegerPart", "GREATEST(n_d152, '1234567890123')",
			"1234567890123;1234567890123;1234567890123;1234567890123",
			"1234567890123.00;1234567890123.00;1234567890123.00;1234567890123.00", false},
		{"GreatestPastTheColumnsIntegerPart", "GREATEST(n_d152, '123456789012345678')",
			"123456789012345678;123456789012345678;123456789012345678;123456789012345678",
			"123456789012345678.00;123456789012345678.00;123456789012345678.00;123456789012345678.00",
			false},

		// A literal past the CARRIER — forty digits and a fraction. It has no
		// exact value at any scale an Int128 can hold, so the row that SELECTS
		// it is 22003 (ADR-0024 item 1). PostgreSQL prints it.
		{"CoalesceLiteralPastTheCarrier", "COALESCE(n_d152, '" + wide + "')",
			"12.75;" + wide + ";1.00;-3.50", "", true},
		{"GreatestLiteralPastTheCarrier", "GREATEST(n_d152, '" + wide + "')",
			wide + ";" + wide + ";" + wide + ";" + wide, "", true},
		{"TwoDecimalsAndALiteralPastTheCarrier",
			"COALESCE(n_d152, n_d3810, '" + wide + "')",
			"12.75;" + wide + ";1.00;-3.50", "", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT (%s) AS v FROM %s ORDER BY id", c.expr, nfTable)
			for _, arm := range arms {
				got, err := arm.run(sql)
				if c.refused {
					if err == nil {
						t.Errorf("%s: %s answered %s — the carrier holds this literal now, "+
							"so clear `refused` and assert PostgreSQL's %s",
							arm.name, sql, got, c.want)
					}
					continue
				}
				if err != nil {
					t.Fatalf("%s: %s refused: %v\n  PostgreSQL 17.11 answers %s",
						arm.name, sql, err, c.want)
				}
				// The NUMBERS must match PostgreSQL exactly — 12.75 against
				// 12.7500 passes, 12.75 against 12.7501 does not.
				if !nfSameRows(c.want, got) {
					t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)",
						arm.name, sql, got, c.want)
				}
				// And the TEXT is pinned where it differs, so the rendering
				// residual cannot drift or be fixed unnoticed (#764).
				//
				// The "this residual has CLOSED" branch is asked FIRST, and
				// the order is load-bearing: with the equality tested first,
				// an entry whose `render` had been edited to equal `want`
				// would go on passing the day the engine started printing
				// PostgreSQL's own text, and the pin would quietly stop being
				// one (round-2 review, N). Keyed on `c.render != ""` rather
				// than on the two strings, it cannot be edited away without
				// deleting the pin outright — which is the fix's proof anyway.
				if c.render != "" && got == c.want {
					t.Errorf("%s: %s now prints PostgreSQL's own text (%s) — the fold "+
						"renders each value at its own scale, so drop this entry's "+
						"`render` pin", arm.name, sql, got)
					continue
				}
				want := c.render
				if want == "" {
					want = c.want
				}
				if got != want {
					t.Errorf("%s: %s\n  printed %s\n  pinned  %s\n  PostgreSQL 17.11: %s",
						arm.name, sql, got, want, c.want)
				}
			}
		})
	}
}

// TestNumericFoldRefusalIsPerRow is the line #724's review drew and the
// impossibility this design asserts: **a literal an arm does not supply never
// costs a query its answer.** The one refusal left — a literal past the
// carrier — lives at the STORE, one row at a time, so it can only reach a row
// that actually selected it.
//
// The fixtures that attempt it (correctness protocol, method 10): the same
// composite over a range that EXCLUDES the literal's row; over an EMPTY range,
// where no row exists to select anything; inside a WHERE, which projects
// nothing and therefore never stores; and — the shape that made this a review
// finding rather than a hypothetical — a fold over TWO decimal columns at
// different scales, where falling back to the FIRST arm's (15,2) would fail on
// the rows the numeric(38,10) column supplies. All four answer.
func TestNumericFoldRefusalIsPerRow(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	const wide = "'123456789012345678901234567890123456789.12345'"
	for _, tc := range []struct{ name, sql, want string }{
		{"the literal's row excluded",
			"SELECT (COALESCE(n_d152, " + wide + ")) AS v FROM " + nfTable +
				" WHERE id <> 2 ORDER BY id", "12.75;1.00;-3.50"},
		{"an empty range",
			"SELECT (COALESCE(n_d152, " + wide + ")) AS v FROM " + nfTable +
				" WHERE id = 99 ORDER BY id", ""},
		{"a predicate over the same composite, which projects nothing",
			"SELECT COUNT(*) AS v FROM " + nfTable +
				" WHERE COALESCE(n_d152, " + wide + ") > 0", "3"},
		{"two decimal columns and an over-carrier literal, its row excluded",
			"SELECT (COALESCE(n_d152, n_d3810, " + wide + ")) AS v FROM " + nfTable +
				" WHERE id <> 2 ORDER BY id", "12.75;1.00;-3.50"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				res, err := nfRun(ctx, single, coord, tc.sql, arm.dag)
				if err != nil {
					t.Fatalf("%s: %s refused: %v — the refusal reached a row that did not "+
						"select the literal, so it is not a per-row store check any more",
						arm.name, tc.sql, err)
				}
				if got := nfRows(res); !nfSameRows(tc.want, got) {
					t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)",
						arm.name, tc.sql, got, tc.want)
				}
			}
		})
	}
}

// TestNestedCompositeBoxMode is the third fold, and the one the other two
// gates cannot see.
//
// Three answers are resolved over a composite: the DECLARATION
// (physical.nodeDeclaredType), the plan-time literal REFUSAL
// (physical.foldArgTypes) and the runtime BOX MODE, which is two questions of
// its own — expr.classifyOperandFold for the TYPE the arms fold to and
// expr.classifyOperand for the KIND a box from them can be. The first two are
// held together by physical.TestDeclaredFoldAgreesWithTheComparisonFold, over
// nested forms included. The last two had their own recursion lists, and
// neither reached NULLIF (#761).
//
// Both halves were needed and neither alone was safe. Without the TYPE arm,
// `COALESCE(NULLIF(numeric, '…'), real)` declared real — PostgreSQL's answer —
// and then refused to produce one, because the inner call's DECIMAL text met a
// FLOAT32 vector and #361's guard stopped the query. Adding only that arm made
// the query answer and made `GREATEST(NULLIF(numeric, '…'), real)` answer
// -3.50 where PostgreSQL answers -0.5: the PAIR was still unclassifiable, so
// compare() ordered two rendered numbers by BYTES, and "-3.50" sorts below
// "-0.5". A loud refusal traded for a silent wrong value is the regression in
// kind method 8 is about, so the KIND arm landed with it.
//
// Every want below is PostgreSQL 17.11's over this fixture, on both arms. The
// entries whose inner call is a CASE, a COALESCE or a GREATEST are the control
// that localises the fix: they were already right, and they must stay right.
func TestNestedCompositeBoxMode(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	// The inner call is on the runtime's list, so these answer. Every want is
	// PostgreSQL 17.11's over this fixture.
	for _, tc := range []struct{ name, expr, want string }{
		{"GreatestOverACoalesce", "GREATEST(COALESCE(n_d152, '1.5'), n_f32)",
			"12.75;1.5;1.6777216e+07;-0.5"},
		{"CoalesceOverAGreatest", "COALESCE(GREATEST(n_d152, '1.5'), n_f64)",
			"12.75;1.5;1.5;1.5"},
		{"LeastNestedInGreatest", "GREATEST(n_d152, LEAST(n_f32, '1.5'))",
			"12.75;1.5;1.5;-0.5"},
		{"CaseInsideCoalesce", "COALESCE(CASE WHEN id = 1 THEN n_d152 ELSE NULL END, n_f64)",
			"12.75;<NULL>;16777217;-0.25"},
		{"NestedOverTwoDecimals", "COALESCE(NULLIF(n_d152, '12.75'), n_d3810)",
			"12.7500000001;<NULL>;1.0000000000;-3.5000000000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT (%s) AS v FROM %s ORDER BY id", tc.expr, nfTable)
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				res, err := nfRun(ctx, single, coord, sql, arm.dag)
				if err != nil {
					t.Fatalf("%s: %s refused: %v\n  PostgreSQL 17.11 answers %s",
						arm.name, sql, err, tc.want)
				}
				if got := nfRows(res); !nfSameRows(tc.want, got) {
					t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)",
						arm.name, sql, got, tc.want)
				}
			}
		})
	}

	// The inner call is NULLIF, which neither recursion reached until #761.
	// The last two are the shapes that a TYPE-only fix answered WRONGLY, so
	// they are the ones worth keeping closest: on the negative row the
	// extremum has to be chosen by value and not by the bytes of two
	// renderings.
	for _, tc := range []struct{ name, expr, want string }{
		{"NullifInsideCoalesceOverReal", "COALESCE(NULLIF(n_d152, '12.75'), n_f32)",
			"0.1;<NULL>;1;-3.5"},
		{"NullifInsideCoalesceKeepsItsValue", "COALESCE(NULLIF(n_d152, '99'), n_f32)",
			"12.75;<NULL>;1;-3.5"},
		{"NullifInsideCoalesceUnquoted", "COALESCE(NULLIF(n_d152, 12.75), n_f32)",
			"0.1;<NULL>;1;-3.5"},
		{"NullifOverAWideDecimal", "COALESCE(NULLIF(n_d3810, '12.7500000001'), n_f64)",
			"0.2;<NULL>;1;-3.5"},
		{"NullifInsideCase", "CASE WHEN id < 9 THEN NULLIF(n_d152, '12.75') ELSE n_f32 END",
			"<NULL>;<NULL>;1;-3.5"},
		{"NullifNestedTwice", "COALESCE(NULLIF(NULLIF(n_d152, '1.00'), '12.75'), n_f32)",
			"0.1;<NULL>;1.6777216e+07;-3.5"},
		{"NullifInsideGreatest", "GREATEST(NULLIF(n_d152, '12.75'), n_f32)",
			"0.1;<NULL>;1.6777216e+07;-0.5"},
		{"NullifInsideLeast", "LEAST(NULLIF(n_d152, '12.75'), n_f64)",
			"0.2;<NULL>;1;-3.5"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT (%s) AS v FROM %s ORDER BY id", tc.expr, nfTable)
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				res, err := nfRun(ctx, single, coord, sql, arm.dag)
				if err != nil {
					t.Fatalf("%s: %s refused: %v\n  PostgreSQL 17.11 answers %s",
						arm.name, sql, err, tc.want)
				}
				if got := nfRows(res); !nfSameRows(tc.want, got) {
					t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)",
						arm.name, sql, got, tc.want)
				}
			}
		})
	}
}
