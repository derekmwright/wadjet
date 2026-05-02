package wadjet

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/auth"
	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

func TestDescribeCaseInsensitive(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "name", Type: parquet.TypeString},
		},
	}
	if err := db.CreateTable(ctx, "events", schema, nil); err != nil {
		t.Fatal(err)
	}

	// DESCRIBE with mixed case should still find the table
	result, err := db.Query(ctx, "DESCRIBE Events")
	if err != nil {
		t.Fatalf("DESCRIBE Events (mixed case) failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}

	// SHOW COLUMNS FROM with uppercase should also work
	result, err = db.Query(ctx, "SHOW COLUMNS FROM EVENTS")
	if err != nil {
		t.Fatalf("SHOW COLUMNS FROM EVENTS (uppercase) failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(result.Rows))
	}
}

func TestDescribeParquetFallback(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	// Create table schema and write some data
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "name", Type: parquet.TypeString, Nullable: true},
		},
	}
	if err := db.CreateTable(ctx, "findings", schema, nil); err != nil {
		t.Fatal(err)
	}

	ing := db.NewIngester("findings", schema, nil, ingest.Config{
		MaxBufferRows: 100,
		RowGroupSize:  100,
	})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "name": "xss"},
		{"id": int64(2), "name": "sqli"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Verify normal DESCRIBE works
	result, err := db.Query(ctx, "DESCRIBE findings")
	if err != nil {
		t.Fatalf("DESCRIBE findings failed: %v", err)
	}
	if len(result.Rows) < 2 {
		t.Fatalf("expected at least 2 rows, got %d", len(result.Rows))
	}

	// Now create a second DB with the SAME store but fresh catalog (simulates restart)
	db2, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	// DESCRIBE should fall back to Parquet file schema discovery
	result, err = db2.Query(ctx, "DESCRIBE findings")
	if err != nil {
		t.Fatalf("DESCRIBE findings (parquet fallback) failed: %v", err)
	}
	if len(result.Rows) < 2 {
		t.Fatalf("expected at least 2 rows from parquet fallback, got %d", len(result.Rows))
	}

	// Verify column names are correct
	foundID, foundName := false, false
	for _, row := range result.Rows {
		if row["column_name"] == "id" {
			foundID = true
		}
		if row["column_name"] == "name" {
			foundName = true
		}
	}
	if !foundID || !foundName {
		t.Errorf("expected id and name columns, got %v", result.Rows)
	}
}

// Regression test for GitHub issue #9: SELECT col1, col2 returns NULL values
// when using column projection instead of SELECT *.
func TestColumnProjectionNotNull(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "severity", Type: parquet.TypeString, Nullable: true},
			{Name: "title", Type: parquet.TypeString, Nullable: true},
			{Name: "status", Type: parquet.TypeString, Nullable: true},
		},
	}
	if err := db.CreateTable(ctx, "findings", schema, nil); err != nil {
		t.Fatal(err)
	}

	ing := db.NewIngester("findings", schema, nil, ingest.Config{
		MaxBufferRows: 100,
		RowGroupSize:  100,
	})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "severity": "high", "title": "Public bucket", "status": "open"},
		{"id": int64(2), "severity": "low", "title": "Missing tag", "status": "closed"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// SELECT * should work
	star, err := db.Query(ctx, "SELECT * FROM findings")
	if err != nil {
		t.Fatalf("SELECT * failed: %v", err)
	}
	if len(star.Rows) != 2 {
		t.Fatalf("SELECT * expected 2 rows, got %d", len(star.Rows))
	}
	for i, row := range star.Rows {
		if row["severity"] == nil {
			t.Errorf("SELECT * row %d: severity is nil", i)
		}
		if row["title"] == nil {
			t.Errorf("SELECT * row %d: title is nil", i)
		}
	}

	// SELECT col1, col2 should also work — this is the regression
	proj, err := db.Query(ctx, "SELECT severity, title FROM findings")
	if err != nil {
		t.Fatalf("SELECT severity, title failed: %v", err)
	}
	if len(proj.Rows) != 2 {
		t.Fatalf("SELECT severity, title expected 2 rows, got %d", len(proj.Rows))
	}
	if len(proj.Columns) != 2 {
		t.Errorf("expected 2 columns, got %d: %v", len(proj.Columns), proj.Columns)
	}
	for i, row := range proj.Rows {
		sev := row["severity"]
		title := row["title"]
		if sev == nil {
			t.Errorf("row %d: severity is nil (expected non-nil)", i)
		}
		if title == nil {
			t.Errorf("row %d: title is nil (expected non-nil)", i)
		}
		t.Logf("row %d: severity=%v title=%v (keys: %v)", i, sev, title, mapKeys(row))
	}

	// Column projection with WHERE clause
	filtered, err := db.Query(ctx, "SELECT severity, title FROM findings WHERE id = 1")
	if err != nil {
		t.Fatalf("SELECT with WHERE failed: %v", err)
	}
	if len(filtered.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(filtered.Rows))
	}
	if filtered.Rows[0]["severity"] == nil {
		t.Errorf("filtered row: severity is nil")
	}
	if filtered.Rows[0]["title"] == nil {
		t.Errorf("filtered row: title is nil")
	}

	// Table-qualified column reference
	qualified, err := db.Query(ctx, "SELECT findings.severity, findings.title FROM findings")
	if err != nil {
		t.Fatalf("SELECT findings.col failed: %v", err)
	}
	if len(qualified.Rows) != 2 {
		t.Fatalf("qualified: expected 2 rows, got %d", len(qualified.Rows))
	}
	for i, row := range qualified.Rows {
		t.Logf("qualified row %d: keys=%v", i, mapKeys(row))
		// Check both qualified and unqualified keys
		sev := row["severity"]
		if sev == nil {
			sev = row["findings.severity"]
		}
		if sev == nil {
			t.Errorf("qualified row %d: severity is nil (keys: %v)", i, mapKeys(row))
		}
	}

	// Single column projection
	single, err := db.Query(ctx, "SELECT title FROM findings")
	if err != nil {
		t.Fatalf("SELECT title failed: %v", err)
	}
	if len(single.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(single.Rows))
	}
	for i, row := range single.Rows {
		if row["title"] == nil {
			t.Errorf("single col row %d: title is nil", i)
		}
	}
}

// Same test but with a partitioned table (matches issue #9 exactly)
func TestColumnProjectionPartitioned(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "tenant_id", Type: parquet.TypeString},
			{Name: "scan_date", Type: parquet.TypeString},
			{Name: "severity", Type: parquet.TypeString, Nullable: true},
			{Name: "title", Type: parquet.TypeString, Nullable: true},
			{Name: "status", Type: parquet.TypeString, Nullable: true},
		},
	}
	if err := db.CreateTable(ctx, "findings", schema, []string{"tenant_id", "scan_date"}); err != nil {
		t.Fatal(err)
	}

	ing := db.NewIngester("findings", schema, []string{"tenant_id", "scan_date"}, ingest.Config{
		MaxBufferRows: 100,
		RowGroupSize:  100,
	})
	if err := ing.Ingest(ctx, []map[string]any{
		{"tenant_id": "acme", "scan_date": "2026-03-01", "severity": "high", "title": "Public bucket", "status": "open"},
		{"tenant_id": "acme", "scan_date": "2026-03-01", "severity": "low", "title": "Missing tag", "status": "closed"},
		{"tenant_id": "globex", "scan_date": "2026-03-02", "severity": "critical", "title": "SQL injection", "status": "open"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Column projection with partition filter
	result, err := db.Query(ctx, "SELECT severity, title FROM findings WHERE tenant_id = 'acme'")
	if err != nil {
		t.Fatalf("partitioned SELECT failed: %v", err)
	}
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	for i, row := range result.Rows {
		t.Logf("part row %d: severity=%v title=%v keys=%v", i, row["severity"], row["title"], mapKeys(row))
		if row["severity"] == nil {
			t.Errorf("part row %d: severity is nil", i)
		}
		if row["title"] == nil {
			t.Errorf("part row %d: title is nil", i)
		}
	}

	// Column projection without partition filter
	all, err := db.Query(ctx, "SELECT severity, title FROM findings")
	if err != nil {
		t.Fatalf("unfiltered SELECT failed: %v", err)
	}
	if len(all.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(all.Rows))
	}
	for i, row := range all.Rows {
		if row["severity"] == nil {
			t.Errorf("all row %d: severity is nil", i)
		}
		if row["title"] == nil {
			t.Errorf("all row %d: title is nil", i)
		}
	}

	// Verify column list is correct and matches row keys
	if len(result.Columns) != 2 || result.Columns[0] != "severity" || result.Columns[1] != "title" {
		t.Errorf("unexpected columns: %v", result.Columns)
	}
}

func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Regression test for GitHub issue #7: CURRENT_DATE returns NULL.
// Table-less SELECT must work and return correct values.
func TestCurrentDateNotNull(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	today := time.Now().Format("2006-01-02")

	tests := []struct {
		sql    string
		colKey string // column name in result
		check  func(val any) bool
	}{
		{
			sql:    "SELECT CURRENT_DATE",
			colKey: "current_date()",
			check:  func(val any) bool { return val == today },
		},
		{
			sql:    "SELECT CURRENT_TIMESTAMP",
			colKey: "current_timestamp()",
			check: func(val any) bool {
				s, ok := val.(string)
				return ok && strings.HasPrefix(s, today[:10])
			},
		},
		{
			sql:    "SELECT NOW()",
			colKey: "now()",
			check: func(val any) bool {
				s, ok := val.(string)
				return ok && strings.HasPrefix(s, today[:10])
			},
		},
		{
			sql:    "SELECT 1 + 1 AS result",
			colKey: "result",
			check:  func(val any) bool { return val == float64(2) || val == int64(2) || val == 2 },
		},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			result, err := db.Query(ctx, tt.sql)
			if err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if len(result.Rows) != 1 {
				t.Fatalf("expected 1 row, got %d", len(result.Rows))
			}
			val, ok := result.Rows[0][tt.colKey]
			if !ok {
				t.Fatalf("column %q not found in result: %v", tt.colKey, result.Rows[0])
			}
			if val == nil {
				t.Fatalf("%s returned NULL", tt.sql)
			}
			if !tt.check(val) {
				t.Errorf("%s returned unexpected value: %v", tt.sql, val)
			}
		})
	}
}

// TestWithRecursiveCTE verifies end-to-end WITH RECURSIVE execution.
func TestWithRecursiveCTE(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "parent_id", Type: parquet.TypeInt64, Nullable: true},
			{Name: "name", Type: parquet.TypeString},
		},
	}
	if err := db.CreateTable(ctx, "org", schema, nil); err != nil {
		t.Fatal(err)
	}

	ing := db.NewIngester("org", schema, nil, ingest.Config{MaxBufferRows: 100, RowGroupSize: 100})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "parent_id": nil, "name": "CEO"},
		{"id": int64(2), "parent_id": int64(1), "name": "VP Eng"},
		{"id": int64(3), "parent_id": int64(1), "name": "VP Sales"},
		{"id": int64(4), "parent_id": int64(2), "name": "Tech Lead"},
		{"id": int64(5), "parent_id": int64(4), "name": "Developer"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	t.Run("generate_series", func(t *testing.T) {
		// Pure recursive CTE without table dependency — generates 1..10
		result, err := db.Query(ctx, `
			WITH RECURSIVE cnt(n) AS (
				SELECT 1
				UNION ALL
				SELECT n + 1 FROM cnt WHERE n < 10
			)
			SELECT n FROM cnt ORDER BY n
		`)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(result.Rows) != 10 {
			t.Fatalf("expected 10 rows, got %d", len(result.Rows))
		}
		for i, row := range result.Rows {
			n := row["n"]
			expected := float64(i + 1)
			if fmt.Sprintf("%v", n) != fmt.Sprintf("%v", expected) {
				t.Errorf("row %d: expected n=%v, got %v", i, expected, n)
			}
		}
	})

	t.Run("non_recursive_cte", func(t *testing.T) {
		// Standard CTE (not recursive) referencing table data
		result, err := db.Query(ctx, `
			WITH managers AS (
				SELECT id, name FROM org WHERE parent_id IS NULL
			)
			SELECT name FROM managers
		`)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}
		if len(result.Rows) != 1 {
			t.Fatalf("expected 1 row, got %d", len(result.Rows))
		}
		if result.Rows[0]["name"] != "CEO" {
			t.Errorf("expected CEO, got %v", result.Rows[0]["name"])
		}
	})
}

func TestExplainAnalyze(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "severity", Type: parquet.TypeString},
		},
	}
	if err := db.CreateTable(ctx, "findings", schema, nil); err != nil {
		t.Fatal(err)
	}

	ing := db.NewIngester("findings", schema, nil, ingest.Config{
		MaxBufferRows: 100,
		RowGroupSize:  100,
	})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "severity": "high"},
		{"id": int64(2), "severity": "low"},
		{"id": int64(3), "severity": "high"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	t.Run("basic", func(t *testing.T) {
		result, err := db.Query(ctx, "EXPLAIN ANALYZE SELECT * FROM findings WHERE severity = 'high'")
		if err != nil {
			t.Fatalf("EXPLAIN ANALYZE failed: %v", err)
		}

		if len(result.Columns) != 1 || result.Columns[0] != "plan" {
			t.Fatalf("expected single 'plan' column, got %v", result.Columns)
		}

		plan := result.Plan

		// Must contain logical plan section
		if !strings.Contains(plan, "-- Logical Plan --") {
			t.Error("missing logical plan section")
		}

		// Must contain execution stats section
		if !strings.Contains(plan, "-- Execution Stats --") {
			t.Error("missing execution stats section")
		}

		// Must contain operator stats with timing
		if !strings.Contains(plan, "rows_in=") || !strings.Contains(plan, "rows_out=") {
			t.Error("missing row count stats")
		}
		if !strings.Contains(plan, "time=") {
			t.Error("missing timing stats")
		}

		// Must report total rows
		if !strings.Contains(plan, "Total rows returned:") {
			t.Error("missing total rows")
		}

		t.Logf("EXPLAIN ANALYZE output:\n%s", plan)
	})

	t.Run("verbose", func(t *testing.T) {
		result, err := db.Query(ctx, "EXPLAIN ANALYZE VERBOSE SELECT * FROM findings")
		if err != nil {
			t.Fatalf("EXPLAIN ANALYZE VERBOSE failed: %v", err)
		}

		plan := result.Plan

		// Must contain both logical and physical plan sections
		if !strings.Contains(plan, "-- Logical Plan --") {
			t.Error("missing logical plan section")
		}
		if !strings.Contains(plan, "-- Physical Plan --") {
			t.Error("missing physical plan section")
		}
		if !strings.Contains(plan, "-- Execution Stats --") {
			t.Error("missing execution stats section")
		}

		t.Logf("EXPLAIN ANALYZE VERBOSE output:\n%s", plan)
	})

	t.Run("plain_explain_unchanged", func(t *testing.T) {
		// Verify that plain EXPLAIN (no ANALYZE) still works and does NOT execute
		result, err := db.Query(ctx, "EXPLAIN SELECT * FROM findings")
		if err != nil {
			t.Fatalf("EXPLAIN failed: %v", err)
		}

		plan := result.Plan

		// Should NOT contain execution stats
		if strings.Contains(plan, "-- Execution Stats --") {
			t.Error("plain EXPLAIN should not contain execution stats")
		}
		if strings.Contains(plan, "Total rows returned:") {
			t.Error("plain EXPLAIN should not contain total rows")
		}
	})
}

func TestExplainAnalyzeABACEnforcement(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "severity", Type: parquet.TypeString},
			{Name: "secret_col", Type: parquet.TypeString},
		},
	}
	if err := db.CreateTable(ctx, "findings", schema, nil); err != nil {
		t.Fatal(err)
	}

	ing := db.NewIngester("findings", schema, nil, ingest.Config{
		MaxBufferRows: 100,
		RowGroupSize:  100,
	})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "severity": "high", "secret_col": "classified-a"},
		{"id": int64(2), "severity": "low", "secret_col": "classified-b"},
		{"id": int64(3), "severity": "high", "secret_col": "classified-c"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Set up ABAC: deny secret_col and inject row filter severity='high'
	evaluator := auth.NewPolicyEvaluator([]auth.AccessControlPolicy{
		{
			Name:    "test-policy",
			Version: 1,
			Enabled: true,
			Rules: []auth.PolicyRule{
				{
					ID:        "restrict-findings",
					EffectStr: "allow",
					Priority:  10,
					Subjects: []auth.Condition{
						{Attribute: "subject.role", Op: "eq", Value: "analyst"},
					},
					Resources: []auth.Condition{
						{Attribute: "resource.name", Op: "eq", Value: "findings"},
					},
					Actions: []auth.Action{auth.ActionRead},
					Obligations: []auth.Obligation{
						{Type: "deny_column", Target: "secret_col"},
						{Type: "row_filter", Value: "severity = 'high'"},
					},
				},
			},
		},
	})

	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "test-key", Name: "analyst", Role: "analyst"},
		},
		Roles: []auth.RoleConfig{
			{Name: "analyst", Tables: []string{"*"}, Allow: []string{"read"}},
		},
	})
	provider := auth.NewProvider(authn, authz, nil, nil)
	provider.UpdateWithEvaluator(authn, authz, nil, evaluator)
	db.SetAuthProvider(provider)

	identity := &auth.Identity{
		Name:   "analyst",
		Role:   "analyst",
		Method: "apikey",
	}
	authCtx := auth.ContextWithIdentity(ctx, identity)

	t.Run("explain_analyze_respects_deny_column", func(t *testing.T) {
		// EXPLAIN ANALYZE with SELECT * -- secret_col should be stripped by ABAC
		result, err := db.Query(authCtx, "EXPLAIN ANALYZE SELECT * FROM findings")
		if err != nil {
			t.Fatalf("EXPLAIN ANALYZE failed: %v", err)
		}
		plan := result.Plan
		t.Logf("EXPLAIN ANALYZE deny_column plan:\n%s", plan)

		// The denied column must not appear in plan output
		if strings.Contains(plan, "secret_col") {
			t.Errorf("denied column 'secret_col' should not appear in EXPLAIN ANALYZE output, plan:\n%s", plan)
		}
	})

	t.Run("explain_respects_deny_column", func(t *testing.T) {
		// EXPLAIN with SELECT * -- secret_col should be stripped by ABAC
		result, err := db.Query(authCtx, "EXPLAIN SELECT * FROM findings")
		if err != nil {
			t.Fatalf("EXPLAIN failed: %v", err)
		}
		plan := result.Plan
		t.Logf("EXPLAIN deny_column plan:\n%s", plan)

		// The denied column must not appear in plan output
		if strings.Contains(plan, "secret_col") {
			t.Errorf("denied column 'secret_col' should not appear in EXPLAIN output, plan:\n%s", plan)
		}
	})

	t.Run("explain_analyze_respects_row_filter", func(t *testing.T) {
		result, err := db.Query(authCtx, "EXPLAIN ANALYZE SELECT id, severity FROM findings")
		if err != nil {
			t.Fatalf("EXPLAIN ANALYZE failed: %v", err)
		}

		plan := result.Plan
		t.Logf("EXPLAIN ANALYZE with ABAC:\n%s", plan)

		// Row filter severity='high' should limit results to 2 rows, not 3
		if !strings.Contains(plan, "Total rows returned: 2") {
			t.Errorf("expected exactly 2 rows (row filter severity='high'), plan:\n%s", plan)
		}
	})

	t.Run("explain_shows_row_filter_in_plan", func(t *testing.T) {
		result, err := db.Query(authCtx, "EXPLAIN SELECT id, severity FROM findings")
		if err != nil {
			t.Fatalf("EXPLAIN failed: %v", err)
		}

		plan := result.Plan
		t.Logf("EXPLAIN with ABAC:\n%s", plan)

		// The row filter should appear in the logical plan
		if !strings.Contains(plan, "severity") {
			t.Errorf("expected row filter predicate in plan, got:\n%s", plan)
		}
	})
}

// TestConcurrentQueryNoPanic is a regression test for the concurrent map write
// panic in buildScan. The Planner held mutable per-query state (scanCounter,
// scanCache, planCtx) on the DB struct, causing fatal races when multiple
// pgwire connections called Query concurrently. Each call must now use an
// independent Planner instance.
func TestConcurrentQueryNoPanic(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "val", Type: parquet.TypeString},
		},
	}
	if err := db.CreateTable(ctx, "concurrent_test", schema, nil); err != nil {
		t.Fatal(err)
	}

	ing := db.NewIngester("concurrent_test", schema, nil, ingest.Config{
		MaxBufferRows: 100,
		RowGroupSize:  100,
	})
	rows := make([]map[string]any, 10)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i), "val": fmt.Sprintf("v%d", i)}
	}
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	const goroutines = 20
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = db.Query(ctx, "SELECT id, val FROM concurrent_test WHERE id > 0")
		}(i)
	}
	wg.Wait()

	for i, e := range errs {
		if e != nil {
			t.Errorf("goroutine %d: %v", i, e)
		}
	}
}
// TestLeftJoinNonEquiOnFilter is a regression test for the Q13/Q16 family
// of bugs. The logical optimizer's extractJoinCondPredicates only pushed
// non-equi ON-clause predicates for INNER joins, so a query like:
//
//	customer LEFT JOIN orders
//	  ON c_custkey = o_custkey
//	  AND o_comment NOT LIKE '%special%requests%'
//
// silently dropped the non-equi part — parseJoinKeys keeps only "=" parts,
// and there is no post-join filter for inner/left joins. The result was
// extra orders pulled into the LEFT JOIN output.
//
// Outer-join correctness: pushing single-side predicates is safe only on
// the *inner* side (rows that don't survive aren't padded with NULLs),
// which is the right side of LEFT JOIN.
func TestLeftJoinNonEquiOnFilter(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	custSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "c_id", Type: parquet.TypeInt32},
	}}
	ordSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "o_id", Type: parquet.TypeInt32},
		{Name: "o_cust", Type: parquet.TypeInt32},
		{Name: "o_kind", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "cust", custSchema, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.CreateTable(ctx, "ord", ordSchema, nil); err != nil {
		t.Fatal(err)
	}
	custIng := db.NewIngester("cust", custSchema, nil, ingest.Config{MaxBufferRows: 8})
	if err := custIng.Ingest(ctx, []map[string]any{
		{"c_id": int32(1)}, {"c_id": int32(2)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := custIng.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	ordIng := db.NewIngester("ord", ordSchema, nil, ingest.Config{MaxBufferRows: 8})
	if err := ordIng.Ingest(ctx, []map[string]any{
		// cust 1: 2 normal orders + 1 special — ON filter should drop the special
		{"o_id": int32(101), "o_cust": int32(1), "o_kind": "normal"},
		{"o_id": int32(102), "o_cust": int32(1), "o_kind": "normal"},
		{"o_id": int32(103), "o_cust": int32(1), "o_kind": "special"},
		// cust 2: only special orders — LEFT JOIN must still emit cust 2 with NULL right
		{"o_id": int32(201), "o_cust": int32(2), "o_kind": "special"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ordIng.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// LEFT JOIN with non-equi ON filter on the right side. Expected per row:
	//   cust 1 → 2 matches (101, 102) — special order excluded
	//   cust 2 → 0 matches → 1 NULL-padded row
	res, err := db.Query(ctx,
		`SELECT c_id, COUNT(o_id) AS n FROM cust
		 LEFT JOIN ord ON c_id = o_cust AND o_kind != 'special'
		 GROUP BY c_id ORDER BY c_id`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d: %v", len(res.Rows), res.Rows)
	}
	want := map[int32]int64{1: 2, 2: 0}
	for _, r := range res.Rows {
		c := r["c_id"].(int32)
		n := r["n"].(int64)
		if got := want[c]; got != n {
			t.Errorf("c_id=%d: got n=%d, want %d", c, n, got)
		}
	}
}

// TestLiteralProjectionType is a regression test for the Q20 bug where
// `WHERE col IN (SELECT 13)` returned no rows. The physical planner's
// inferProjectionType ignored *plansql.Lit nodes, so a numeric-literal
// projection got typed as String. The runtime then stored the int64 13 in a
// String column (rendered as "13"), and the IN-subquery hash lookup against
// an int column never matched. Adds explicit type inference for literal
// projections so SELECT <int> projects through an int64 column.
func TestLiteralProjectionType(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := Open(ctx, Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "k", Type: parquet.TypeInt32},
			{Name: "name", Type: parquet.TypeString},
		},
	}
	if err := db.CreateTable(ctx, "t", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("t", schema, nil, ingest.Config{MaxBufferRows: 8})
	if err := ing.Ingest(ctx, []map[string]any{
		{"k": int32(13), "name": "thirteen"},
		{"k": int32(20), "name": "twenty"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	// Projection of a numeric literal must keep its int64 type — `IN
	// (SELECT 13)` against an int column has to match.
	res, err := db.Query(ctx, "SELECT name FROM t WHERE k IN (SELECT 13)")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if got := res.Rows[0]["name"]; got != "thirteen" {
		t.Errorf("name: got %v, want thirteen", got)
	}

	// The standalone literal projection must also produce an int64-typed cell.
	res, err = db.Query(ctx, "SELECT 13 AS x")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(res.Rows))
	}
	if got, want := res.Rows[0]["x"], int64(13); got != want {
		t.Errorf("x: got %v (%T), want %v (int64)", got, got, want)
	}
}


