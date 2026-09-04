package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// An UNKNOWN-typed literal arm — a quoted string or a bare NULL — takes the
// OTHER arm's type, and the result DECLARES that type on every path (#648,
// ADR-0012 item 12, PostgreSQL's resolution algorithm step 3).
//
// The rule reached the stage DAG first: reconcileSetOpArmTypes stamps the
// resolved type on the literal arm's projection spec and
// setOpDeclaredOutputSchema skips unknown arms in its fold. The
// single-process path publishes its ColumnMetas from unifySetOpSchemas, which
// had no notion of an unknown arm, so it folded STRING against the typed arm,
// DECLINED (STRING is not on the numeric ladder), and kept the LEFTMOST arm's
// STRING. Measured at ee778066, with the literal in the first arm:
//
//	SELECT '1.5' AS v FROM decpair WHERE id = 1 UNION ALL SELECT a FROM decpair
//	  single : declared STRING       (wire OID 25)   rendering 1.5
//	  dag    : declared DECIMAL(9,2) (wire OID 1700) rendering 1.50
//	  PostgreSQL 17.11: numeric
//
// Two defects in one shape: a right value under a wrong OID on the door the
// wire oracle drives, and two different renderings of the same number
// depending on which door answered — which is the fast-path byte threshold,
// not anything the query says.
//
// This gate asserts the DECLARED type and the RENDERED bytes, not the row
// count: a row count cannot see either half, which is why
// `an_unknown_literal_arm_first` (wantRows: 10) stayed green throughout.
//
// The declared TYPMOD is the typed arm's, deliberately: PostgreSQL declares
// numeric with typmod -1 here and renders the literal `1.5` beside the
// column's `2.00`, where wadjet declares DECIMAL(9,2) and renders `1.50`. A
// wadjet DECIMAL vector has ONE scale, so "unconstrained" is not a carrier it
// has; keeping the typed arm's (p,s) is the same answer `c_dec ∪ '0'` already
// gives when the typed arm is on the LEFT, and the alternative — resolving one
// arm's scale away — is #532's truncation. Recorded in ADR-0012 item 12.
type setOpUnkDeclCell struct {
	issue, name, sql string
	// wantDecl is the result's declared first column, `name:TYPE(p,s)`.
	wantDecl string
	// wantRows is the rendered value of column 0 on every row, SORTED, with a
	// NULL written as <nil>. It is what a client reads.
	wantRows []string
}

func setOpUnkDeclCells() []setOpUnkDeclCell {
	return []setOpUnkDeclCell{
		// The pair at the heart of the split, in BOTH arm orders. The typed
		// arm on the left was always right; the literal on the left is the
		// half that published OID 25.
		{issue: "#648", name: "a_numeric_column_then_an_unknown_literal",
			sql: `SELECT a AS v FROM decpair WHERE id IN (4,5,6) UNION ALL ` +
				`SELECT '1.5' FROM decpair WHERE id = 1`,
			wantDecl: "v:DECIMAL(9,2)",
			wantRows: []string{"-0.01", "0.00", "1.50", "2.00"}},
		{issue: "#648", name: "an_unknown_literal_then_a_numeric_column",
			sql: `SELECT '1.5' AS v FROM decpair WHERE id = 1 UNION ALL ` +
				`SELECT a FROM decpair WHERE id IN (4,5,6)`,
			wantDecl: "v:DECIMAL(9,2)",
			wantRows: []string{"-0.01", "0.00", "1.50", "2.00"}},
		// A bare NULL is the other unknown spelling and has the same answer:
		// PostgreSQL types `NULL ∪ numeric` numeric.
		{issue: "#648", name: "a_null_literal_then_a_numeric_column",
			sql: `SELECT NULL AS v FROM decpair WHERE id = 1 UNION ALL ` +
				`SELECT a FROM decpair WHERE id IN (4,5,6)`,
			wantDecl: "v:DECIMAL(9,2)",
			wantRows: []string{"-0.01", "0.00", "2.00", "<nil>"}},
		// Outside the numeric family the split is the whole answer: STRING
		// against IPV4 is not a rendering difference, it is a different type
		// on the wire for a column of addresses.
		{issue: "#648", name: "an_unknown_literal_then_an_inet_column",
			sql: `SELECT '10.0.0.9' AS v FROM typemx WHERE id = 0 UNION ALL ` +
				`SELECT c_ipv4 FROM typemx WHERE id < 3`,
			wantDecl: "v:IPV4",
			wantRows: []string{"10.0.0.0", "10.0.0.1", "10.0.0.2", "10.0.0.9"}},
		{issue: "#648", name: "an_unknown_literal_then_a_date_column",
			sql: `SELECT '2010-01-01' AS v FROM typemx WHERE id = 0 UNION ALL ` +
				`SELECT c_date FROM typemx WHERE id < 3`,
			wantDecl: "v:DATE",
			wantRows: []string{"2010-01-01", "2010-01-01", "2011-02-02", "2012-03-03"}},
		{issue: "#648", name: "an_unknown_literal_then_a_mac_column",
			sql: `SELECT 'aa:bb:cc:00:00:09' AS v FROM typemx WHERE id = 0 UNION ALL ` +
				`SELECT c_mac FROM typemx WHERE id < 3`,
			wantDecl: "v:MAC",
			wantRows: []string{"aa:bb:cc:00:00:00", "aa:bb:cc:00:00:01",
				"aa:bb:cc:00:00:02", "aa:bb:cc:00:00:09"}},
		// THREE arms with the literal FIRST. These nest as (literal ∪ a) ∪ a
		// on BOTH doors, so the rule has to reach the NESTED node: the
		// single-process path resolves it in the inner adapter and declares it
		// in the outer one, and the DAG types the nested arm through
		// setOpNodeResultTypes — which counted the literal's STRING as a type
		// of its own, returned "unknown" for the whole inner result, and
		// REFUSED the query ("result column \"v\" is DECIMAL in one arm and
		// its type cannot be resolved in arm 1") where the single-process path
		// answered. PostgreSQL answers it numeric.
		{issue: "#648", name: "an_unknown_literal_first_of_three_arms",
			sql: `SELECT '1.5' AS v FROM decpair WHERE id = 1 UNION ALL ` +
				`SELECT a FROM decpair WHERE id IN (4,5) UNION ALL ` +
				`SELECT a FROM decpair WHERE id = 6`,
			wantDecl: "v:DECIMAL(9,2)",
			wantRows: []string{"-0.01", "0.00", "1.50", "2.00"}},
		// Controls. Two unknown arms resolve to text in PostgreSQL, which is
		// what they already declare — the mask must not drag them anywhere.
		{issue: "#648", name: "ctl_two_unknown_literal_arms_stay_text",
			sql: `SELECT 'a' AS v FROM decpair WHERE id = 1 UNION ALL ` +
				`SELECT 'b' FROM decpair WHERE id = 1`,
			wantDecl: "v:STRING",
			wantRows: []string{"a", "b"}},
		// An unknown literal does not widen a typmod either: the typed arm's
		// DECIMAL(18,4) stands and the literal renders at that scale.
		{issue: "#648", name: "ctl_an_unknown_literal_keeps_the_typed_arms_typmod",
			sql: `SELECT c_dec AS v FROM typemx WHERE id < 3 UNION ALL ` +
				`SELECT '0' FROM typemx WHERE id = 0`,
			wantDecl: "v:DECIMAL(18,4)",
			wantRows: []string{"0.0000", "0.0000", "1.0001", "2.0002"}},
		// And a pair with NO unknown arm is untouched by any of this.
		{issue: "#648", name: "ctl_two_typed_numeric_arms",
			sql: `SELECT a AS v FROM decpair WHERE id IN (4,5,6) UNION ALL ` +
				`SELECT b FROM decpair WHERE id = 4`,
			wantDecl: "v:DECIMAL(18,4)",
			wantRows: []string{"-0.0100", "-0.0100", "0.0000", "2.0000"}},
	}
}

func TestAnUnknownLiteralArmDeclaresTheOtherArmsType(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	for _, tc := range setOpUnkDeclCells() {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			check := func(arm, decl string, rows []string, err error) {
				t.Helper()
				if err != nil {
					t.Fatalf("%s arm: %v\n  SQL: %s", arm, err, tc.sql)
				}
				if decl != tc.wantDecl {
					t.Errorf("%s arm DECLARES %s, want %s — a set operation's column type is "+
						"a wire fact, and the two paths must publish the same one\n  SQL: %s",
						arm, decl, tc.wantDecl, tc.sql)
				}
				if got := strings.Join(rows, "|"); got != strings.Join(tc.wantRows, "|") {
					t.Errorf("%s arm RENDERS\n  %s\nwant\n  %s\n  SQL: %s",
						arm, got, strings.Join(tc.wantRows, "|"), tc.sql)
				}
			}
			decl, rows, err := sudSingle(ctx, single, tc.sql)
			check("single", decl, rows, err)
			for _, arm := range []struct {
				name string
				c    *Coordinator
			}{{"dag", coord}, {"dag-shuffled", coordB}} {
				decl, rows, err := sudDAG(ctx, arm.c, tc.sql)
				check(arm.name, decl, rows, err)
			}
		})
	}
}

// sudSingle runs one statement on the embedded single-process engine and
// returns its DECLARED first column and that column's rendered values.
func sudSingle(ctx context.Context, db *wadjet.DB, sql string) (string, []string, error) {
	out, err := db.Query(ctx, sql)
	if err != nil {
		return "", nil, err
	}
	decl := ""
	if len(out.ColumnMetas) > 0 {
		m := out.ColumnMetas[0]
		decl = sudDecl(m.Name, m.TypeID, m.Precision, m.Scale)
	}
	vals := make([]string, 0, len(out.Rows))
	for i := range out.Rows {
		cells := out.Cells(i)
		if len(cells) == 0 {
			continue
		}
		vals = append(vals, fmt.Sprintf("%v", cells[0]))
	}
	sort.Strings(vals)
	return decl, vals, nil
}

// sudDAG is sudSingle for the stage DAG. OutputSchema is read BEFORE Rows:
// materializing the rows detaches the batches the schema would come from.
func sudDAG(ctx context.Context, coord *Coordinator, sql string) (string, []string, error) {
	out, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		return "", nil, err
	}
	if out.Error != "" {
		return "", nil, fmt.Errorf("%s", out.Error)
	}
	decl := ""
	if schema := out.OutputSchema(); len(schema) > 0 {
		decl = sudDecl(schema[0].Name, schema[0].Type, schema[0].Precision, schema[0].Scale)
	}
	rows, rerr := out.Rows()
	if rerr != nil {
		return "", nil, fmt.Errorf("materializing distributed rows: %w", rerr)
	}
	name := ""
	if len(out.Columns) > 0 {
		name = out.Columns[0]
	}
	vals := make([]string, 0, len(rows))
	for _, r := range rows {
		vals = append(vals, fmt.Sprintf("%v", r[name]))
	}
	sort.Strings(vals)
	return decl, vals, nil
}

func sudDecl(name string, t parquet.TypeID, prec, scale int) string {
	if t == parquet.TypeDecimal {
		return fmt.Sprintf("%s:%s(%d,%d)", name, t, prec, scale)
	}
	return fmt.Sprintf("%s:%s", name, t)
}
