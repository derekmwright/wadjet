// SPDX-License-Identifier: MIT

package physical

import (
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestTemporalArithmeticDeclaresWhatItProduces is arc VL round 3's
// declared == produced gate for the OPERATOR forms: for every spelling, the
// declaration (DeclaredTypeOfNode) and the value the compiled expression
// produces agree — a DATE declaration comes with a DATE box (int64 epoch
// days) holding PostgreSQL's value, a TIMESTAMP one with a TIMESTAMP box.
//
// The spellings are the round-2 review's second spellings of round 1's
// finding: the operand of `date ± n` judged by its DECLARED type — a cast, a
// nested sum, an integer column, a clock function — never by whether it is a
// bare number literal; and date arithmetic's own result reaching a parent
// operator, a string function or a date-part function in its unit. Values are
// PostgreSQL 17.11's (measured), CURRENT_DATE's taken as today.
func TestTemporalArithmeticDeclaresWhatItProduces(t *testing.T) {
	schema := []parquet.Column{
		{Name: "d", Type: parquet.TypeDate}, {Name: "ts", Type: parquet.TypeTimestamp},
		{Name: "i", Type: parquet.TypeInt32}, {Name: "s", Type: parquet.TypeString},
	}
	b := batch.NewRecordBatch(schema, 1)
	b.Len = 1
	b.Columns[0].SetValue(0, "2026-03-03")
	b.Columns[1].SetValue(0, int64(1772533230000)) // 2026-03-03 10:20:30
	b.Columns[2].SetValue(0, int32(2))
	b.Columns[3].SetValue(0, "2026-03-03")
	today := time.Now().UTC().Format("2006-01-02")
	for _, tc := range []struct {
		sql  string
		typ  parquet.TypeID
		want string
	}{
		{"DATE '2026-01-01' + 1", parquet.TypeDate, "2026-01-02"},
		{"DATE '2026-01-01' + CAST(1 AS INTEGER)", parquet.TypeDate, "2026-01-02"},
		{"DATE '2026-01-01' + 1 * 1", parquet.TypeDate, "2026-01-02"},
		{"(DATE '2026-01-01' + 1) + 1", parquet.TypeDate, "2026-01-03"},
		{"1 + DATE '2026-01-01'", parquet.TypeDate, "2026-01-02"},
		{"d + i", parquet.TypeDate, "2026-03-05"},
		{"(d + 1) + 1", parquet.TypeDate, "2026-03-05"},
		{"d - i", parquet.TypeDate, "2026-03-01"},
		{"CURRENT_DATE + 1 - 1", parquet.TypeDate, today},
		{"CURRENT_DATE + 1 - 1 - CURRENT_DATE", parquet.TypeInt64, "0"},
		{"(d + 1) - d", parquet.TypeInt64, "1"},
		{"d - DATE '2026-01-01'", parquet.TypeInt64, "61"},
		{"CAST(ts AS DATE) + 1", parquet.TypeDate, "2026-03-04"},
		{"d + INTERVAL '1 day'", parquet.TypeTimestamp, "2026-03-04 00:00:00"},
		{"(d + 1) + INTERVAL '1 day'", parquet.TypeTimestamp, "2026-03-05 00:00:00"},
		{"ts + INTERVAL '1 hour'", parquet.TypeTimestamp, "2026-03-03 11:20:30"},
		{"INTERVAL '1 day' + ts", parquet.TypeTimestamp, "2026-03-04 10:20:30"},
		{"'2026-03-03 10:20:30' + INTERVAL '1 hour'", parquet.TypeTimestamp, "2026-03-03 11:20:30"},
		{"date_add(d, 1)", parquet.TypeDate, "2026-03-04"},
		{"date_sub(d + 1, 2)", parquet.TypeDate, "2026-03-02"},
		{"date_add(ts, 1)", parquet.TypeTimestamp, "2026-03-04 10:20:30"},
		{"date_add(d, INTERVAL '1 day')", parquet.TypeTimestamp, "2026-03-04 00:00:00"},
		{"to_date('2026-04-05')", parquet.TypeDate, "2026-04-05"},
		{"to_date('2026-04-05') + 1", parquet.TypeDate, "2026-04-06"},
		{"last_day_of_month(d)", parquet.TypeDate, "2026-03-31"},
		{"date_trunc('month', d + 1)", parquet.TypeTimestamp, "2026-03-01 00:00:00"},
		{"CAST(d + 1 AS TIMESTAMP)", parquet.TypeTimestamp, "2026-03-04 00:00:00"},
		{"COALESCE(d + 1, d)", parquet.TypeDate, "2026-03-04"},
		// The value reaching a consumer that reads it as text or as an
		// instant, in its own unit.
		{"(d + 1) || 'x'", parquet.TypeString, "2026-03-04x"},
		{"CAST(d + 1 AS TEXT)", parquet.TypeString, "2026-03-04"},
		{"EXTRACT(YEAR FROM CURRENT_DATE + 1) > 2000", parquet.TypeBool, "true"},
		{"(d + 1) > TIMESTAMP '2026-03-03 23:00:00'", parquet.TypeBool, "true"},
		{"DATE '2026-01-02' > TIMESTAMP '2026-01-01 10:00:00'", parquet.TypeBool, "true"},
		{"CURRENT_DATE + 1 > now()", parquet.TypeBool, "true"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			node, err := plansql.ParseExpressionComplete(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			decl, conf := DeclaredTypeOfNode(node, schema)
			if conf != expr.Decided || decl.ID != tc.typ {
				t.Errorf("declares %s (%v), want %s", decl.ID, conf, tc.typ)
			}
			compiled, err := expr.Compile(node)
			if err != nil {
				t.Fatal(err)
			}
			v := compiled.Eval(b, 0)
			var got string
			switch tc.typ {
			case parquet.TypeDate:
				days, ok := v.(int64)
				if !ok {
					t.Fatalf("declares DATE, produced %T %v", v, v)
				}
				got = batch.FormatDate(int32(days))
			case parquet.TypeTimestamp:
				ms, ok := v.(int64)
				if !ok {
					t.Fatalf("declares TIMESTAMP, produced %T %v", v, v)
				}
				got = batch.FormatTimestamp(ms)
			default:
				got = fmt.Sprint(v)
			}
			if got != tc.want {
				t.Errorf("= %s, want %s (PostgreSQL 17.11)", got, tc.want)
			}
		})
	}
}
