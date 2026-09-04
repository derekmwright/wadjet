package logical

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// shapeOnlyFor builds and optimizes a plan without the TPC-H scan annotator
// (these queries name their own columns) and returns the single scan's
// ShapeOnlyColumns.
func shapeOnlyFor(t *testing.T, sql string) []string {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract %q: %v", sql, err)
	}
	plan, err := BuildFromSelectWithCTEs(info, info.CTEs)
	if err != nil {
		t.Fatalf("build %q: %v", sql, err)
	}
	opt := Optimize(plan)
	scans := collectScans(opt)
	if len(scans) == 0 {
		return nil
	}
	return scans[0].ShapeOnlyColumns
}

func hasCol(cols []string, want string) bool {
	for _, c := range cols {
		if strings.EqualFold(c, want) {
			return true
		}
	}
	return false
}

func TestShapeOnlyColumnsMarked(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want string
	}{
		{
			// The motivating shape: ClickBench Q28, spelled with the
			// BYTE-counting length. url is used twice and neither use reads a
			// byte.
			//
			// The published Q28 spells it `LENGTH(URL)`, and that spelling no
			// longer takes this path: #856 made LENGTH count CHARACTERS, as
			// PostgreSQL and this engine's own CHARACTER_LENGTH do, and a
			// character count is a scan of the continuation bytes rather than
			// a subtraction of two offsets. The not-marked list below carries
			// the LENGTH spelling as the boundary this states.
			name: "clickbench-q28",
			sql:  "SELECT counterid, AVG(OCTET_LENGTH(url)) AS l, COUNT(*) AS c FROM hits WHERE url <> '' GROUP BY counterid HAVING COUNT(*) > 100 ORDER BY l DESC LIMIT 25",
			want: "url",
		},
		{
			name: "octet-length-only-projection",
			sql:  "SELECT k, OCTET_LENGTH(url) AS l FROM hits",
			want: "url",
		},
		{
			name: "octet-length",
			sql:  "SELECT k, octet_length(url) AS l FROM hits",
			want: "url",
		},
		{
			name: "bit-length",
			sql:  "SELECT k, bit_length(url) AS l FROM hits",
			want: "url",
		},
		{
			name: "is-null-filter",
			sql:  "SELECT COUNT(*) AS c FROM hits WHERE url IS NOT NULL",
			want: "url",
		},
		{
			name: "count-of-column",
			sql:  "SELECT k, COUNT(url) AS c FROM hits GROUP BY k",
			want: "url",
		},
		{
			name: "empty-string-equality",
			sql:  "SELECT k, COUNT(*) AS c FROM hits WHERE url = '' GROUP BY k",
			want: "url",
		},
		{
			name: "sum-of-lengths",
			sql:  "SELECT SUM(OCTET_LENGTH(url)) AS s FROM hits",
			want: "url",
		},
		{
			// DISTINCT is rewritten to GROUP BY the projected alias, so the
			// column itself never reaches the aggregate.
			name: "distinct-over-length",
			sql:  "SELECT DISTINCT OCTET_LENGTH(url) AS l FROM hits",
			want: "url",
		},
		{
			// Filter-only under a projection of another column: the value
			// never reaches the output.
			name: "filter-only-under-projection",
			sql:  "SELECT k FROM hits WHERE url <> '' LIMIT 10",
			want: "url",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := shapeOnlyFor(t, tc.sql)
			if !hasCol(got, tc.want) {
				t.Errorf("ShapeOnlyColumns = %v, want to contain %q", got, tc.want)
			}
		})
	}
}

func TestShapeOnlyColumnsNotMarked(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		col  string
	}{
		{
			// One value use anywhere disqualifies the column.
			name: "mixed-shape-and-value",
			sql:  "SELECT url, OCTET_LENGTH(url) AS l FROM hits WHERE url <> ''",
			col:  "url",
		},
		{
			name: "grouped-by-the-column",
			sql:  "SELECT url, COUNT(*) AS c FROM hits WHERE url <> '' GROUP BY url",
			col:  "url",
		},
		{
			name: "ordered-by-the-column",
			sql:  "SELECT k, OCTET_LENGTH(url) AS l FROM hits ORDER BY url",
			col:  "url",
		},
		{
			name: "like-filter",
			sql:  "SELECT k, OCTET_LENGTH(url) AS l FROM hits WHERE url LIKE '%x%'",
			col:  "url",
		},
		{
			name: "non-empty-literal-comparison",
			sql:  "SELECT k, OCTET_LENGTH(url) AS l FROM hits WHERE url = 'x'",
			col:  "url",
		},
		{
			// Rune counting must read the bytes.
			name: "char-length",
			sql:  "SELECT k, char_length(url) AS l FROM hits",
			col:  "url",
		},
		{
			// #856's boundary: LENGTH is char_length's synonym now, so the
			// spelling ClickBench Q28 uses no longer takes the shape-only
			// decode. This entry is the claim the marked list makes, attempted
			// from the outside — without it, restoring `length` to
			// shapeLenFuncs would silently pass every test in this file.
			name: "length-is-a-character-count",
			sql:  "SELECT k, LENGTH(url) AS l FROM hits",
			col:  "url",
		},
		{
			name: "length-inside-an-aggregate",
			sql:  "SELECT SUM(LENGTH(url)) AS s FROM hits",
			col:  "url",
		},
		{
			name: "substr",
			sql:  "SELECT k, OCTET_LENGTH(url) AS l, substr(url, 1, 3) AS p FROM hits",
			col:  "url",
		},
		{
			name: "count-distinct",
			sql:  "SELECT k, COUNT(DISTINCT url) AS c FROM hits GROUP BY k",
			col:  "url",
		},
		{
			name: "join",
			sql:  "SELECT h.k, OCTET_LENGTH(h.url) AS l FROM hits h JOIN other o ON h.k = o.k",
			col:  "url",
		},
		{
			name: "union",
			sql:  "SELECT OCTET_LENGTH(url) AS l FROM hits UNION ALL SELECT OCTET_LENGTH(url) AS l FROM hits",
			col:  "url",
		},
		{
			name: "window",
			sql:  "SELECT k, OCTET_LENGTH(url) AS l, ROW_NUMBER() OVER (PARTITION BY url) AS rn FROM hits",
			col:  "url",
		},
		{
			name: "in-subquery",
			sql:  "SELECT k, OCTET_LENGTH(url) AS l FROM hits WHERE url IN (SELECT url FROM other)",
			col:  "url",
		},
		{
			name: "wildcard-projection",
			sql:  "SELECT * FROM hits WHERE url <> ''",
			col:  "url",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shapeOnlyFor(t, tc.sql); hasCol(got, tc.col) {
				t.Errorf("ShapeOnlyColumns = %v, must not contain %q", got, tc.col)
			}
		})
	}
}

// TestShapeOnlyColumnsOutputBound pins the structural guarantee that keeps a
// distributed leaf fragment (scan + filter, rows shipped onward) from
// marking anything.
func TestShapeOnlyColumnsOutputBound(t *testing.T) {
	scan := NewScan("hits", "")
	scan.RequiredColumns = []string{"url"}
	filter := NewFilter(scan, []Predicate{{Column: "url", Op: "!=", Value: ""}})
	if outputBoundedByProjection(filter) {
		t.Fatal("a Filter root must not be treated as output-bounded")
	}
	computeShapeOnlyColumns(filter)
	if len(scan.ShapeOnlyColumns) != 0 {
		t.Fatalf("filter-rooted fragment marked %v", scan.ShapeOnlyColumns)
	}
	if outputBoundedByProjection(scan) {
		t.Fatal("a Scan root must not be treated as output-bounded")
	}
}

// TestShapeOnlyColumnsRequiresPrunedList: an unpruned scan means "read every
// column", so nothing was proven about how the columns are used.
func TestShapeOnlyColumnsRequiresPrunedList(t *testing.T) {
	scan := NewScan("hits", "")
	agg := NewAggregate(scan, nil, []AggExpr{{Func: "count", InputCol: "url", OutputCol: "c"}})
	computeShapeOnlyColumns(agg)
	if len(scan.ShapeOnlyColumns) != 0 {
		t.Fatalf("unpruned scan marked %v", scan.ShapeOnlyColumns)
	}
}
