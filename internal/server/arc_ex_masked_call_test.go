// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// Arc EX's MASKING gate, on all NINE doors.
//
// Two of the arc's five items re-run over a relation's own columns, which is
// what puts them here. #1137 RE-TYPES a literal beside a column at the
// predicate — the exact site a mask has to be read instead of the stored value
// — so `c_proto = 'udp'` over a PROTOCOL column masked to TCP must select
// NOTHING however many rows really hold UDP. And #1053/#1056/#583 add a
// plan-time REFUSAL over argument types, which a policed relation reaches with
// the MASK's type rather than the column's: a refusal that reads the stored
// declaration, or an error message that quotes a policed value, is a
// disclosure through the error channel.
//
// The gate asserts the mask's ANSWER and a NON-VACUOUS (cell, door) count: a
// census whose cells all refuse proves nothing, and the count says how many
// really answered.
func TestArcEXAMaskedColumnAnswersItsMaskAtEveryDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: embedded cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	rig := pmRigUp(t, ctx)
	leaks := pmTrueValues()

	// want is the canonical row list under the ANALYST identity. A cell with
	// no want is asserted only for non-disclosure and for its refusal class.
	cells := []struct {
		name, sql string
		want      []string
		// wantRefused says the analyst must be REFUSED rather than answered;
		// the message is then checked for policed values like any answer.
		wantRefused bool
	}{
		// #1137's own shape. The mask is TCP (6) and every stored row is UDP
		// or ICMP, so a predicate reading the MASK selects all six rows for
		// 'tcp' and none for 'udp'. A predicate reading the STORED value
		// answers the other way round, and that row COUNT is the disclosure.
		{name: "masked_protocol_equals_the_mask_name",
			sql:  `SELECT COUNT(*) AS n FROM e7net WHERE c_proto = 'tcp'`,
			want: []string{"n=6"}},
		{name: "masked_protocol_equals_a_stored_name",
			sql:  `SELECT COUNT(*) AS n FROM e7net WHERE c_proto = 'udp'`,
			want: []string{"n=0"}},
		{name: "masked_protocol_in_a_list_of_names",
			sql:  `SELECT COUNT(*) AS n FROM e7net WHERE c_proto IN ('udp','icmp')`,
			want: []string{"n=0"}},
		{name: "masked_protocol_is_not_the_mask",
			sql:  `SELECT COUNT(*) AS n FROM e7net WHERE c_proto <> 'tcp'`,
			want: []string{"n=0"}},
		{name: "masked_protocol_inside_a_case",
			sql:  `SELECT COUNT(*) AS n FROM e7net WHERE CASE WHEN c_proto = 'udp' THEN true ELSE false END`,
			want: []string{"n=0"}},
		{name: "masked_protocol_in_having",
			sql:  `SELECT c_proto AS p FROM e7net GROUP BY c_proto HAVING c_proto = 'tcp'`,
			want: []string{"p=6"}},
		{name: "masked_port_equals_the_mask",
			sql:  `SELECT COUNT(*) AS n FROM e7net WHERE c_port = '443'`,
			want: []string{"n=6"}},
		{name: "masked_port_equals_a_stored_value",
			sql:  `SELECT COUNT(*) AS n FROM e7net WHERE c_port = '9001'`,
			want: []string{"n=0"}},
		{name: "masked_protocol_projected",
			sql:  `SELECT DISTINCT c_proto AS p FROM e7net`,
			want: []string{"p=6"}},
		// A literal the TYPE refuses is refused for the masked column too, and
		// the refusal must name the literal rather than any stored value.
		{name: "masked_protocol_against_a_malformed_literal",
			sql: `SELECT COUNT(*) AS n FROM e7net WHERE c_proto = 'nosuchproto'`, wantRefused: true},
		{name: "masked_port_against_int4s_grammar",
			sql: `SELECT COUNT(*) AS n FROM e7net WHERE c_port = '0x1bb'`, wantRefused: true},

		// #1053 / #1056 / #583 over a POLICED relation. A text function over
		// the masked ssn reads the MASK, and a mis-called one is refused
		// without publishing anything.
		{name: "text_function_over_a_masked_column",
			sql:  `SELECT DISTINCT UPPER(ssn) AS v FROM e7emp`,
			want: []string{"v=***"}},
		{name: "length_over_a_masked_column",
			sql:  `SELECT DISTINCT LENGTH(ssn) AS v FROM e7emp`,
			want: []string{"v=3"}},
		{name: "concat_over_a_masked_column",
			sql:  `SELECT DISTINCT CONCAT(ssn, '-', 1) AS v FROM e7emp`,
			want: []string{"v=***-1"}},
		{name: "cast_of_a_masked_numeric_column",
			sql:  `SELECT DISTINCT CAST(acct AS BIGINT) AS v FROM e7emp`,
			want: []string{"v=0"}},
		{name: "wrong_arity_over_a_masked_column",
			sql: `SELECT UPPER(ssn, 'x') AS v FROM e7emp`, wantRefused: true},
		{name: "numeric_literal_in_a_text_position_over_a_masked_column",
			sql: `SELECT STARTS_WITH(ssn, 1) AS v FROM e7emp`, wantRefused: true},
		{name: "wrong_arity_over_a_denied_column",
			sql: `SELECT UPPER(salary, 'x') AS v FROM e7emp`, wantRefused: true},
		{name: "text_function_over_a_denied_column",
			sql: `SELECT UPPER(salary) AS v FROM e7emp`, wantRefused: true},
	}

	answered, refused := 0, 0
	for _, cell := range cells {
		for _, door := range rig.doors {
			got, err := door.run(t, "analyst-key", cell.sql)
			if err != nil {
				refused++
				// A REFUSAL is a disposition, and its MESSAGE is a channel
				// like any other: an error that quotes a policed value
				// discloses it just as a row would.
				for _, bad := range leaks {
					if strings.Contains(err.Error(), bad) {
						t.Errorf("%s / %s: the refusal quotes the policed value %q: %v\n  %s",
							cell.name, door.name, bad, err, cell.sql)
					}
				}
				if !cell.wantRefused {
					t.Logf("%s / %s: refused (%v)", cell.name, door.name, err)
				}
				continue
			}
			if cell.wantRefused {
				t.Errorf("%s / %s ANSWERED %v; this statement names arguments no overload "+
					"takes and must be refused on every door\n  %s",
					cell.name, door.name, got.canon(), cell.sql)
				continue
			}
			answered++
			for _, c := range got.cols {
				if strings.EqualFold(c, "salary") {
					t.Errorf("%s / %s: the DENIED column %q is in the published list\n  %s",
						cell.name, door.name, c, cell.sql)
				}
			}
			for _, row := range got.rows {
				for c, v := range row {
					for _, bad := range leaks {
						if strings.Contains(v, bad) {
							t.Errorf("%s / %s: %s=%s is a policed value reaching the client\n  %s",
								cell.name, door.name, c, v, cell.sql)
						}
					}
				}
			}
			if cell.want == nil {
				continue
			}
			if g, w := strings.Join(got.canon(), "|"), strings.Join(cell.want, "|"); g != w {
				t.Errorf("%s / %s: %s, want %s — the answer must be a statement about the "+
					"MASK, and a different row set here is arithmetic on the value the "+
					"policy hides\n  %s", cell.name, door.name, g, w, cell.sql)
			}
		}
	}
	// A GATE WHOSE CELLS ALL REFUSE PROVES NOTHING, and one whose cells all
	// answer proves nothing about the refusals.
	if answered == 0 {
		t.Fatalf("no (cell, door) pair answered: this gate's leak test cannot fail, and %d "+
			"shapes over policed relations were refused on every door", len(cells))
	}
	if refused == 0 {
		t.Fatalf("no (cell, door) pair was refused: the wrong-arity and wrong-domain cells " +
			"are not reaching a refusal at all")
	}
	t.Logf("%d of %d (cell, door) pairs answered and %d were refused",
		answered, len(cells)*len(rig.doors), refused)
	if want := len(cells) * len(rig.doors); answered+refused != want {
		t.Errorf("%d (cell, door) pairs accounted for, want %d", answered+refused, want)
	}
	if len(rig.doors) != 9 {
		t.Errorf("the rig stood up %d doors, want 9 — a door added or lost silently changes "+
			"what this gate covers: %s", len(rig.doors), pmDoorNames(rig))
	}
}

func pmDoorNames(rig pmRig) string {
	names := make([]string, 0, len(rig.doors))
	for _, d := range rig.doors {
		names = append(names, d.name)
	}
	return fmt.Sprint(names)
}
