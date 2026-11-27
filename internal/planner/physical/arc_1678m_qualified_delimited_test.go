// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A QUALIFIED DELIMITED REFERENCE IS HELD TO ITS OWN RELATION'S SPELLING
// (#1678 line m, cn/caseCols). A quoted identifier is case-exact in
// PostgreSQL; the binder already refused `x."VAL"` over a column `Val`, but it
// asked the BLOCK whether anything published the bytes `Val`, so in
//
//	SELECT q."Val", m."Val" FROM qm_t q JOIN qm m ON q.id = m.id
//
// q's "Val" vouched for m, and m."Val" read m's val: a silent wrong answer on
// every arm. PostgreSQL 17.11 (--locale=C), measured:
//
//	ERROR:  42703: column m.Val does not exist
//
// Unquoted references are untouched: the lexer folds them, and they keep
// resolving a stored mixed-case name case-insensitively (the ClickBench
// columns), which is the engine's documented rule and not changed here.
func TestArc1678mQualifiedDelimitedReferenceAsksItsOwnRelation(t *testing.T) {
	cat := brSchemaCatalog{
		"qm_t": {Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}, {Name: "Val", Type: parquet.TypeInt64}}},
		"qm":   {Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}, {Name: "val", Type: parquet.TypeInt64}}},
	}
	cells := []struct{ name, sql, state, sentence string }{
		{"select", `SELECT q.id, q."Val", m."Val" FROM qm_t q JOIN qm m ON q.id = m.id`, "42703", "column m.Val does not exist"},
		{"selectReversed", `SELECT m."Val", q.id FROM qm m JOIN qm_t q ON q.id = m.id`, "42703", "column m.Val does not exist"},
		{"where", `SELECT q.id FROM qm_t q JOIN qm m ON q.id = m.id WHERE m."Val" > 0`, "42703", "column m.Val does not exist"},
		{"on", `SELECT q.id FROM qm_t q JOIN qm m ON q.id = m.id AND m."Val" > 0`, "42703", "column m.Val does not exist"},
		{"groupBy", `SELECT m."Val", COUNT(*) FROM qm_t q JOIN qm m ON q.id = m.id GROUP BY m."Val"`, "42703", "column m.Val does not exist"},
		{"orderBy", `SELECT q.id FROM qm_t q JOIN qm m ON q.id = m.id ORDER BY m."Val"`, "42703", "column m.Val does not exist"},
		{"aggArg", `SELECT SUM(m."Val") FROM qm_t q JOIN qm m ON q.id = m.id`, "42703", "column m.Val does not exist"},
		{"correlated", `SELECT q.id, (SELECT MAX(m."Val") FROM qm m WHERE m.id = q.id) FROM qm_t q`, "42703", "column m.Val does not exist"},
		{"wrongCaseOwnRelation", `SELECT q."VAL" FROM qm_t q JOIN qm m ON q.id = m.id`, "42703", "column q.VAL does not exist"},
		// Controls PostgreSQL answers, each one edit from a refusing cell.
		{"ok/exactOnItsRelation", `SELECT q.id, q."Val", m.val FROM qm_t q JOIN qm m ON q.id = m.id`, "", ""},
		{"ok/quotedLowerOnLowerRelation", `SELECT q."Val", m."val" FROM qm_t q JOIN qm m ON q.id = m.id`, "", ""},
		{"ok/tableNameQualifier", `SELECT qm_t."Val", qm.val FROM qm_t JOIN qm ON qm_t.id = qm.id`, "", ""},
		// Unquoted references keep today's case-insensitive resolution of a
		// stored mixed-case name (PostgreSQL would refuse this one: the
		// compatibility class this line does not change).
		{"ok/unquotedFindsMixedCase", `SELECT q.Val FROM qm_t q JOIN qm m ON q.id = m.id`, "", ""},
	}
	for _, tc := range cells {
		t.Run(tc.name, func(t *testing.T) {
			err := validateColumns(context.Background(), cat, mustExtract(t, tc.sql))
			if tc.state == "" {
				if err != nil {
					t.Fatalf("the binder refused a statement it answers today\n  %s\n  %v", tc.sql, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("PostgreSQL 17.11 raises %s %s; the binder accepted the statement\n  %s", tc.state, tc.sentence, tc.sql)
			}
			if got := sqlerr.StateOf(err); got != tc.state {
				t.Errorf("SQLSTATE %s, want %s (PostgreSQL 17.11)\n  %s\n  %v", got, tc.state, tc.sql, err)
			}
			if !strings.Contains(err.Error(), tc.sentence) {
				t.Errorf("got %v, want PostgreSQL's sentence %q\n  %s", err, tc.sentence, tc.sql)
			}
		})
	}
}
