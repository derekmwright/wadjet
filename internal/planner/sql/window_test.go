package sql

import (
	"strings"
	"testing"
)

func TestRewriteWindowFunctions_Basic(t *testing.T) {
	sql := "SELECT user_id, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM employees"
	rewritten, specs, err := rewriteWindowFunctions(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 1 {
		t.Fatalf("expected 1 window spec, got %d", len(specs))
	}

	ws := specs[0]
	if ws.FuncName != "row_number" {
		t.Errorf("func: got %q, want 'row_number'", ws.FuncName)
	}
	if ws.Args != "" {
		t.Errorf("args: got %q, want empty", ws.Args)
	}
	if ws.Alias != "rn" {
		t.Errorf("alias: got %q, want 'rn'", ws.Alias)
	}
	if len(ws.PartitionBy) != 1 || ws.PartitionBy[0] != "dept" {
		t.Errorf("partition by: got %v, want [dept]", ws.PartitionBy)
	}
	if len(ws.OrderBy) != 1 || ws.OrderBy[0].Column != "salary" || !ws.OrderBy[0].Desc {
		t.Errorf("order by: got %v, want [{salary true}]", ws.OrderBy)
	}

	// Verify the rewritten SQL is valid for vitess
	if !strings.Contains(rewritten, "0 as rn") {
		t.Errorf("rewritten SQL should contain '0 as rn': %s", rewritten)
	}
	if strings.Contains(rewritten, "OVER") {
		t.Errorf("rewritten SQL should not contain OVER: %s", rewritten)
	}
}

func TestRewriteWindowFunctions_NoAlias(t *testing.T) {
	sql := "SELECT user_id, RANK() OVER (ORDER BY score DESC) FROM employees"
	rewritten, specs, err := rewriteWindowFunctions(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 1 {
		t.Fatalf("expected 1 window spec, got %d", len(specs))
	}
	if specs[0].Alias != "__w0" {
		t.Errorf("alias: got %q, want '__w0'", specs[0].Alias)
	}
	if !strings.Contains(rewritten, "0 as __w0") {
		t.Errorf("rewritten SQL should contain '0 as __w0': %s", rewritten)
	}
}

func TestRewriteWindowFunctions_Multiple(t *testing.T) {
	sql := "SELECT user_id, ROW_NUMBER() OVER (ORDER BY ts) as rn, SUM(amount) OVER (PARTITION BY dept) as total FROM events"
	_, specs, err := rewriteWindowFunctions(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 2 {
		t.Fatalf("expected 2 window specs, got %d", len(specs))
	}

	if specs[0].FuncName != "row_number" || specs[0].Alias != "rn" {
		t.Errorf("spec[0]: got func=%q alias=%q", specs[0].FuncName, specs[0].Alias)
	}
	if specs[1].FuncName != "sum" || specs[1].Alias != "total" {
		t.Errorf("spec[1]: got func=%q alias=%q", specs[1].FuncName, specs[1].Alias)
	}
	if specs[1].Args != "amount" {
		t.Errorf("spec[1] args: got %q, want 'amount'", specs[1].Args)
	}
	if len(specs[1].PartitionBy) != 1 || specs[1].PartitionBy[0] != "dept" {
		t.Errorf("spec[1] partition: got %v, want [dept]", specs[1].PartitionBy)
	}
}

func TestRewriteWindowFunctions_NoWindowFunctions(t *testing.T) {
	sql := "SELECT user_id, amount FROM events WHERE id > 5"
	rewritten, specs, err := rewriteWindowFunctions(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 0 {
		t.Fatalf("expected no window specs, got %d", len(specs))
	}
	if rewritten != sql {
		t.Errorf("SQL should be unchanged: got %q", rewritten)
	}
}

func TestRewriteWindowFunctions_AggWithOver(t *testing.T) {
	sql := "SELECT dept, SUM(salary) OVER (PARTITION BY dept ORDER BY hire_date) as running_total FROM employees"
	_, specs, err := rewriteWindowFunctions(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 1 {
		t.Fatalf("expected 1 spec, got %d", len(specs))
	}
	if specs[0].FuncName != "sum" {
		t.Errorf("func: got %q, want 'sum'", specs[0].FuncName)
	}
	if specs[0].Args != "salary" {
		t.Errorf("args: got %q, want 'salary'", specs[0].Args)
	}
	if len(specs[0].OrderBy) != 1 || specs[0].OrderBy[0].Column != "hire_date" {
		t.Errorf("order by: got %v", specs[0].OrderBy)
	}
}

func TestRewriteWindowFunctions_DenseRank(t *testing.T) {
	sql := "SELECT name, DENSE_RANK() OVER (PARTITION BY dept, team ORDER BY score DESC) as dr FROM employees"
	_, specs, err := rewriteWindowFunctions(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 1 {
		t.Fatalf("expected 1 spec, got %d", len(specs))
	}
	ws := specs[0]
	if ws.FuncName != "dense_rank" {
		t.Errorf("func: got %q, want 'dense_rank'", ws.FuncName)
	}
	if len(ws.PartitionBy) != 2 || ws.PartitionBy[0] != "dept" || ws.PartitionBy[1] != "team" {
		t.Errorf("partition by: got %v, want [dept team]", ws.PartitionBy)
	}
}

func TestRewriteWindowFunctions_StringLiteral(t *testing.T) {
	// OVER inside a string literal should not be treated as a window spec
	sql := "SELECT 'OVER' as label, name FROM employees"
	rewritten, specs, err := rewriteWindowFunctions(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 0 {
		t.Errorf("expected 0 specs, got %d", len(specs))
	}
	if rewritten != sql {
		t.Errorf("SQL should be unchanged")
	}
}

func TestRewriteWindowFunctions_CaseInsensitive(t *testing.T) {
	sql := "SELECT user_id, row_number() over (partition by dept order by salary desc) as rn FROM employees"
	_, specs, err := rewriteWindowFunctions(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 1 {
		t.Fatalf("expected 1 spec, got %d", len(specs))
	}
	if specs[0].FuncName != "row_number" {
		t.Errorf("func: got %q", specs[0].FuncName)
	}
}

func TestRewriteWindowFunctions_PartitionByOnly(t *testing.T) {
	sql := "SELECT dept, COUNT(*) OVER (PARTITION BY dept) as dept_count FROM employees"
	_, specs, err := rewriteWindowFunctions(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 1 {
		t.Fatalf("expected 1 spec, got %d", len(specs))
	}
	if specs[0].Args != "*" {
		t.Errorf("args: got %q, want '*'", specs[0].Args)
	}
	if len(specs[0].OrderBy) != 0 {
		t.Errorf("expected no order by, got %v", specs[0].OrderBy)
	}
}

func TestParseWindowSelect(t *testing.T) {
	sql := "SELECT user_id, ROW_NUMBER() OVER (ORDER BY ts DESC) as rn FROM events"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Type != QuerySelect {
		t.Fatalf("expected QuerySelect, got %v", parsed.Type)
	}
	if len(parsed.Windows) != 1 {
		t.Fatalf("expected 1 window spec, got %d", len(parsed.Windows))
	}

	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Windows) != 1 {
		t.Fatalf("expected 1 window in SelectInfo, got %d", len(info.Windows))
	}

	// Find the window column
	found := false
	for _, col := range info.Columns {
		if col.Alias == "rn" && col.IsWindow {
			found = true
			if col.WindowSpec == nil {
				t.Error("window column has nil WindowSpec")
			}
		}
	}
	if !found {
		t.Error("expected to find a window column with alias 'rn'")
	}
}
