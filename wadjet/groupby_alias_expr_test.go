package wadjet

import (
	"context"
	"testing"

	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// Regression: GROUP BY <alias> (and GROUP BY <position>) where the alias
// names a computed SELECT expression (CASE, function call) must group by
// that expression. Previously only the alias STRING was kept: the aggregate
// grouped by a nonexistent column (collapsing to one group) and the
// projection re-evaluated the expression over the aggregate output — where
// the source columns no longer exist — so every row took the ELSE/NULL
// branch. Surfaced by ClickBench Q19/Q40 (extract-alias and CASE-alias
// grouping over hits).
func TestGroupByAliasOfComputedExpr(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "eng", Type: parquet.TypeInt64},
		{Name: "ref", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "t", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 10)
	for i := range rows {
		// eng: 0 for rows 0-6, 1 for rows 7-9.
		eng := int64(0)
		if i >= 7 {
			eng = 1
		}
		rows[i] = map[string]any{"eng": eng, "ref": "r"}
	}
	ing := db.NewIngester("t", schema, nil, ingest.Config{MaxBufferRows: 100})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"alias", "SELECT CASE WHEN eng = 0 THEN 'a' ELSE 'b' END AS s, COUNT(*) AS c FROM t GROUP BY s ORDER BY c DESC"},
		{"positional", "SELECT CASE WHEN eng = 0 THEN 'a' ELSE 'b' END AS s, COUNT(*) AS c FROM t GROUP BY 1 ORDER BY c DESC"},
		{"func_alias", "SELECT abs(eng - 1) AS s, COUNT(*) AS c FROM t GROUP BY s ORDER BY c DESC"},
		{"plain_alias", "SELECT CASE WHEN eng = 0 THEN 'a' ELSE 'b' END AS s2, eng AS s, COUNT(*) AS c FROM t GROUP BY s2, s ORDER BY c DESC"},
		{"literal_positional", "SELECT 1 AS s2, CASE WHEN eng = 0 THEN 'a' ELSE 'b' END AS s, COUNT(*) AS c FROM t GROUP BY 1, 2 ORDER BY c DESC"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := db.Query(ctx, tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Rows) != 2 {
				t.Fatalf("got %d groups, want 2: %v", len(res.Rows), res.Rows)
			}
			counts := map[string]int64{}
			for _, r := range res.Rows {
				var key string
				var c int64
				for k, v := range r {
					switch k {
					case "c":
						if n, ok := v.(int64); ok {
							c = n
						}
					case "s":
						key = canonKey(v)
					}
				}
				counts[key] = c
			}
			want := map[string]map[string]int64{
				"alias":              {"a": 7, "b": 3},
				"positional":         {"a": 7, "b": 3},
				"func_alias":         {"1": 7, "0": 3},
				"plain_alias":        {"0": 7, "1": 3},
				"literal_positional": {"a": 7, "b": 3},
			}[tc.name]
			for k, wc := range want {
				if counts[k] != wc {
					t.Errorf("group %q: count %d, want %d (all: %v)", k, counts[k], wc, counts)
				}
			}
		})
	}
}

func canonKey(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case int64:
		return itoa64(x)
	case float64:
		return itoa64(int64(x))
	}
	if v == nil {
		return "<nil>"
	}
	return "?"
}

func itoa64(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
