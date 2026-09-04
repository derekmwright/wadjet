package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// #628: PostgreSQL's boolean input grammar belongs to the OPERAND'S TYPE, not
// to how the operand was produced. #574 gave it to a BOOL *column* — the rule
// was keyed on classifyOperand tagging a `*ColRef` whose type is TypeBool —
// so a DERIVED boolean fell to compare()'s toString rendering and matched the
// two exact spellings "true"/"false" alone:
//
//	SELECT count(*) WHERE (NOT c_bool) = 'yes'   wadjet 0, PostgreSQL the FALSE count
//	SELECT count(*) WHERE (NOT c_bool) = 'bogus' wadjet 0, PostgreSQL 22P02
//
// Every expectation below is live PostgreSQL 17.11 over the same shape
// (`f3_bool(k bigint, b boolean)` holding true, false, NULL), and the COUNTS
// are computed from this fixture rather than copied, so a wrong expectation
// cannot be inherited from a wrong engine.
func TestComputedBooleanTakesThePostgresBooleanGrammar(t *testing.T) {
	ctx := context.Background()
	db := tmOpen(t)
	tbl := typematrix.Table

	trueN := int64(tmBoolCount(t, true))
	falseN := int64(tmBoolCount(t, false))
	nullN := int64(typematrix.Rows) - trueN - falseN
	if trueN == 0 || falseN == 0 || nullN == 0 {
		t.Fatalf("fixture cannot separate the rules: true=%d false=%d null=%d", trueN, falseN, nullN)
	}
	// c_str is NULL on a stride of its own, and a LIKE over a NULL is NULL —
	// which is neither TRUE nor a boolean-grammar question.
	var strN int64
	for _, r := range typematrix.Data(typematrix.Rows) {
		if r["c_str"] != nil {
			strN++
		}
	}

	for _, c := range []struct {
		name, pred string
		want       int64
	}{
		// The producer is a NOT, an AND, an OR — none of them a column.
		{"not_yes", `(NOT c_bool) = 'yes'`, falseN},
		{"not_n", `(NOT c_bool) = 'n'`, trueN},
		{"not_off", `(NOT c_bool) = 'off'`, trueN},
		{"not_one", `(NOT c_bool) = '1'`, falseN},
		{"not_zero", `(NOT c_bool) = '0'`, trueN},
		{"not_prefix", `(NOT c_bool) = 'tr'`, falseN},
		{"not_padded_uppercase", `(NOT c_bool) = '  TRUE  '`, falseN},
		{"and_no", `(c_bool AND c_bool) = 'no'`, falseN},
		{"or_on", `(c_bool OR c_bool) = 'on'`, trueN},
		// The two spellings that already worked stay working — this is the
		// half a fix could break by routing everything through the grammar.
		{"not_true", `(NOT c_bool) = 'true'`, falseN},
		{"not_false", `(NOT c_bool) = 'false'`, trueN},
		{"not_ne_yes", `(NOT c_bool) <> 'yes'`, trueN},
		// Ordering, not only equality.
		{"not_lt_true", `(NOT c_bool) < 'true'`, trueN},
		// A comparison, an IS NULL, a LIKE, a BETWEEN and an IN are boolean
		// producers too, and each reaches classifyOperand as its own node.
		{"cmp_yes", `(id > 1) = 'yes'`, int64(typematrix.Rows) - 2},
		{"isnull_y", `(c_bool IS NULL) = 'y'`, nullN},
		{"isnotnull_y", `(c_bool IS NOT NULL) = 'y'`, trueN + falseN},
		{"like_t", `(c_str LIKE 's-%') = 't'`, strN},
		{"between_true", `(id BETWEEN 0 AND 9) = 'true'`, 10},
		{"in_yes", `(id IN (0, 1, 2)) = 'yes'`, 3},
		// The composites: a CASE, a COALESCE and a CAST over booleans.
		{"case_yes", `(CASE WHEN id = 0 THEN true ELSE false END) = 'yes'`, 1},
		{"coalesce_f", `COALESCE(c_bool, false) = 'f'`, falseN + nullN},
		{"coalesce_true_literal", `COALESCE(c_bool, true) = 'y'`, trueN + nullN},
		{"cast_yes", `CAST(c_bool AS BOOLEAN) = 'yes'`, trueN},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := tmScalarInt(t, ctx, db, fmt.Sprintf(
				`SELECT COUNT(*) AS n FROM %s WHERE %s`, tbl, c.pred))
			if got != c.want {
				t.Errorf("WHERE %s matched %d rows, live PostgreSQL 17.11 matches %d",
					c.pred, got, c.want)
			}
			// The SELECT list is the second site, and it was the one that
			// answered differently from the WHERE clause before #574 — the
			// projection has no scan kernel to reach.
			sel := tmScalarInt(t, ctx, db, fmt.Sprintf(
				`SELECT COUNT(*) AS n FROM (SELECT (%s) AS v FROM %s) q WHERE v`, c.pred, tbl))
			if sel != c.want {
				t.Errorf("projected %s is TRUE on %d rows and the WHERE clause matches %d "+
					"— one rule, two sites", c.pred, sel, c.want)
			}
		})
	}

	// A string that is not a boolean at all is 22P02 on the server, and was a
	// silent `false` here. Loud beats plausible.
	for _, sql := range []string{
		`SELECT COUNT(*) AS n FROM ` + tbl + ` WHERE (NOT c_bool) = 'bogus'`,
		`SELECT COUNT(*) AS n FROM ` + tbl + ` WHERE (c_bool AND c_bool) = 'o'`,
	} {
		_, err := tmRun(ctx, db, sql)
		if err == nil {
			t.Errorf("%s answered; PostgreSQL 17.11 raises 22P02 "+
				`invalid input syntax for type boolean`, sql)
			continue
		}
		if !strings.Contains(err.Error(), "invalid input syntax for type boolean") {
			t.Errorf("%s: %v, want PostgreSQL's boolean input refusal", sql, err)
		}
	}
}
