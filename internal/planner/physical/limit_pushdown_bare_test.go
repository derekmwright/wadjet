package physical

import (
	"strings"
	"testing"
)

// A LIMIT with no ORDER BY must reach the stages that read rows, so a task
// stops pulling once satisfied instead of reading its whole input for rows the
// coordinator will discard (#311).
//
// Before this, `SELECT t.*, CTID FROM public.customer t LIMIT 501` against
// SF100 read all 15M rows — the gather stage reported total_rows=15000000 and
// a 23 GB peak heap — to return 501.
func TestBareLimitReachesReadingStages(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	stages := sqlToStages(t, cat, ctx, "SELECT l_orderkey FROM lineitem LIMIT 501", 3)
	if len(stages) == 0 {
		t.Fatal("no stages")
	}
	var carriers, readers int
	for _, s := range stages {
		if stageReadsRows(s) {
			readers++
			if s.RowLimit == 501 {
				carriers++
			} else {
				t.Errorf("stage %s (%s) reads rows but carries RowLimit=%d, want 501",
					s.ID, s.Type, s.RowLimit)
			}
		}
	}
	if readers == 0 {
		t.Fatalf("no row-reading stage found among %s", stageSummary(stages))
	}
	if carriers == 0 {
		t.Fatalf("no stage carries the limit: %s", stageSummary(stages))
	}
}

// A LIMIT whose subtree can change cardinality must NOT be pushed down:
// bounding the input of a join, aggregate, distinct or sort changes its
// output, which would be wrong answers rather than fewer rows. ORDER BY LIMIT
// keeps its existing sort/TopN path, where Stage.Limit applies.
func TestLimitPushdownDeclinesUnsafeShapes(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	for _, tt := range []struct{ name, sql string }{
		{"order by", "SELECT l_orderkey FROM lineitem ORDER BY l_orderkey LIMIT 10"},
		{"aggregate", "SELECT COUNT(*) AS c FROM lineitem LIMIT 10"},
		{"group by", "SELECT l_orderkey, COUNT(*) AS c FROM lineitem GROUP BY l_orderkey LIMIT 10"},
		{"distinct", "SELECT DISTINCT l_orderkey FROM lineitem LIMIT 10"},
		{"join", "SELECT l_orderkey FROM lineitem JOIN orders ON l_orderkey = o_orderkey LIMIT 10"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, s := range sqlToStages(t, cat, ctx, tt.sql, 3) {
				if s.RowLimit != 0 {
					t.Errorf("stage %s (%s) carries RowLimit=%d; %s may not push its limit down",
						s.ID, s.Type, s.RowLimit, tt.name)
				}
			}
		})
	}
}

// A filter is cardinality-reducing, never row-producing, so a limit may still
// be pushed through it — the task simply reaches n later, or never.
func TestBareLimitPushesThroughFilterAndProject(t *testing.T) {
	cat, ctx := setupTPCHCatalog(t)

	stages := sqlToStages(t, cat, ctx,
		"SELECT l_orderkey, l_quantity * 2 AS q2 FROM lineitem WHERE l_quantity > 5 LIMIT 7", 3)
	found := false
	for _, s := range stages {
		if s.RowLimit == 7 {
			found = true
		}
	}
	if !found {
		t.Fatalf("limit did not survive filter+project: %s", stageSummary(stages))
	}
}

// stageReadsRows reports whether a stage produces rows from storage, as
// opposed to moving or combining rows other stages produced.
func stageReadsRows(s Stage) bool {
	switch s.Type {
	case "scan", "pipeline":
		return true
	}
	// Exchange stages that absorbed a scan still do the reading.
	return strings.Contains(s.Type, "exchange") && s.TableName != ""
}

func stageSummary(stages []Stage) string {
	var b strings.Builder
	for i, s := range stages {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(s.ID)
		b.WriteString("(")
		b.WriteString(s.Type)
		if s.TableName != "" {
			b.WriteString(" tbl=")
			b.WriteString(s.TableName)
		}
		b.WriteString(" rowlimit=")
		b.WriteString(itoa(s.RowLimit))
		b.WriteString(")")
	}
	return b.String()
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}
