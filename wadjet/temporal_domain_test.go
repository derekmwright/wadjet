package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

const (
	// One second past the epoch: 1000 ms, deep inside the ±500,000 band the
	// magnitude heuristic reads as a DAY count.
	nearEpochText = "1970-01-01T00:00:01Z"
	// 3500-01-01 is 558,821 epoch days — OUTSIDE the ±500,000 band, so the
	// heuristic reads the day count as MILLISECONDS. The first draft of this
	// gate used 1970-01-02 (day 1), which is inside the band where the
	// heuristic is accidentally right: three of its five cases passed on the
	// parent commit, and the DATE half of #826 shipped with no base-FAIL
	// evidence at all. The fixture has to sit where the bug is visible.
	farDateText = "3500-01-01"
)

// One predicate, one answer, whatever the spelling and whatever the arm
// (#826).
//
// The temporal DOMAIN — epoch days for a DATE, epoch milliseconds for a
// TIMESTAMP — was inferred from the MAGNITUDE of the numeric operand:
// parseTemporalInt64OK reads |ref| < 500_000 as "this is days". So a
// TIMESTAMP inside ±500,000 ms of the epoch (the sentinel band log data is
// full of) was compared as a day count, and a DATE beyond 500,000 epoch
// days as a millisecond count.
//
// It is invisible in the obvious spelling because compileCmp specializes a
// bare column against a string LITERAL into CmpTemporalLit, which
// pre-parses the literal into BOTH units and picks by the column's resolved
// type. Only the column-against-column spelling reached the guess, so the
// two spellings of one predicate disagreed — which is a defect regardless
// of which answer PostgreSQL would give (it refuses `timestamp = text`
// outright).
//
// Every COMPARISON case below fails on the parent commit — seven of seven,
// on both arms. (The NULL companion at the end passes there and says so.)
// The fixture puts the SAME text in the string column on every row, so
// `ts = s` and `ts = '<that text>'` are two spellings of ONE predicate and
// any disagreement is the defect.
func TestOneTemporalPredicateAnswersTheSameInEverySpelling(t *testing.T) {
	for _, arm := range []struct {
		name   string
		budget int64
	}{
		{"in_memory", 0},
		// The spilled arm. The comparison happens in the expression layer,
		// below any spill decision, but a budget changes which vector a
		// value arrives in and the arc's own #791 pin is a reminder that
		// "below the decision" is a claim, not a guarantee.
		{"spilled", 512 << 10},
	} {
		t.Run(arm.name, func(t *testing.T) {
			if arm.budget > 0 {
				defer exec.ForceSmallSpillRuns(4096)()
			}
			runTemporalDomainCases(t, temporalFixture(t, arm.budget))
		})
	}
}

func runTemporalDomainCases(t *testing.T, db *DB) {
	t.Helper()
	ctx := context.Background()
	count := func(sql string) int64 {
		t.Helper()
		res, err := db.Query(ctx, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		if len(res.Rows) != 1 {
			t.Fatalf("%s returned %d rows, want 1", sql, len(res.Rows))
		}
		switch v := res.Rows[0]["n"].(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case float64:
			return int64(v)
		default:
			t.Fatalf("%s: n is %T", sql, v)
			return 0
		}
	}

	cases := []struct {
		name             string
		literal, colPair string
		want             int64
		why              string
	}{
		{
			"near_epoch_ts_equality",
			`SELECT COUNT(*) AS n FROM temporal WHERE ts = '` + nearEpochText + `'`,
			`SELECT COUNT(*) AS n FROM temporal WHERE ts = s`,
			2,
			"1000 ms is inside the ±500,000 band the heuristic reads as DAYS",
		},
		{
			"near_epoch_ts_greater_than",
			`SELECT COUNT(*) AS n FROM temporal WHERE ts > '` + nearEpochText + `'`,
			`SELECT COUNT(*) AS n FROM temporal WHERE ts > s`,
			2,
			"the same band through an ordering operator: 1999 and 2024 are after it",
		},
		{
			"near_epoch_ts_less_than",
			`SELECT COUNT(*) AS n FROM temporal WHERE ts < '` + nearEpochText + `'`,
			`SELECT COUNT(*) AS n FROM temporal WHERE ts < s`,
			1,
			"row 6 is 500 ms past the epoch: read as a DAY count it is 500 > 0, read as " +
				"milliseconds it is 500 < 1000 — opposite answers from one predicate",
		},
		{
			"far_date_equality",
			`SELECT COUNT(*) AS n FROM temporal WHERE d = '` + farDateText + `'`,
			`SELECT COUNT(*) AS n FROM temporal WHERE d = ds`,
			2,
			"558,821 epoch days is OUTSIDE the band, so the heuristic reads days as ms",
		},
		{
			"far_date_less_than",
			`SELECT COUNT(*) AS n FROM temporal WHERE d < '` + farDateText + `'`,
			`SELECT COUNT(*) AS n FROM temporal WHERE d < ds`,
			2,
			"the DATE twin through an ordering operator",
		},
		{
			"far_date_not_equal",
			`SELECT COUNT(*) AS n FROM temporal WHERE d <> '` + farDateText + `'`,
			`SELECT COUNT(*) AS n FROM temporal WHERE d <> ds`,
			3,
			"<> over the whole table, which also pins that the NULL row is excluded " +
				"from a negated comparison rather than swept in by it",
		},
		{
			"far_date_greater_than",
			`SELECT COUNT(*) AS n FROM temporal WHERE d > '` + farDateText + `'`,
			`SELECT COUNT(*) AS n FROM temporal WHERE d > ds`,
			1,
			"and the other direction, so a fix cannot satisfy one side by inverting",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lit := count(tc.literal)
			col := count(tc.colPair)
			if lit != col {
				t.Fatalf("one predicate, two answers: the LITERAL spelling says %d and the "+
					"COLUMN spelling says %d.\n  %s\n  literal: %s\n  column:  %s\n"+
					"  The temporal domain comes from the operand's DECLARED type, never "+
					"from the value's magnitude (#826, ADR-0012 item 8).",
					lit, col, tc.why, tc.literal, tc.colPair)
			}
			if lit != tc.want {
				t.Fatalf("both spellings answer %d, want %d — they agree on the wrong "+
					"number.\n  %s", lit, tc.want, tc.why)
			}
		})
	}

	// A NULL operand is UNKNOWN, not a coincidence of the domain. Row 5
	// carries NULL in both string columns; it must match nothing for every
	// operator, `<>` included, in both spellings.
	//
	// DISCLOSED: unlike every case above, this one PASSES on the parent
	// commit. NULL handling was never part of the defect — the new arm
	// short-circuits on a nil box before it reaches the domain question — so
	// this is a companion property, not regression evidence. It is here
	// because the fix inserts a rule ahead of compare() for a pair that can
	// carry NULL, and a rule that swallowed a NULL into a comparison would
	// be a new defect this file must catch.
	t.Run("null_operand_is_unknown", func(t *testing.T) {
		for _, op := range []string{"=", "<>", "<", ">"} {
			gotTS := count(`SELECT COUNT(*) AS n FROM temporal WHERE id = 5 AND ts ` + op + ` s`)
			gotD := count(`SELECT COUNT(*) AS n FROM temporal WHERE id = 5 AND d ` + op + ` ds`)
			if gotTS != 0 || gotD != 0 {
				t.Fatalf("a NULL operand matched %s: ts%s s -> %d, d%s ds -> %d; a comparison "+
					"against NULL is UNKNOWN and matches nothing, `<>` included",
					op, op, gotTS, op, gotD)
			}
		}
	})
}

// temporalFixture builds the two-spelling table. s and ds hold the SAME
// text on every non-NULL row — the literal each column arm is paired with.
func temporalFixture(t *testing.T, budget int64) *DB {
	t.Helper()
	ctx := context.Background()
	cfg := Config{Store: objstore.NewMemStore(), Bucket: "test", MemoryBudget: budget}
	if budget > 0 {
		cfg.SpillDir = t.TempDir()
	}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "ds", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "temporal", schema, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	rows := []map[string]any{
		{"id": int64(1), "ts": nearEpochText, "s": nearEpochText, "d": farDateText, "ds": farDateText},
		{"id": int64(2), "ts": nearEpochText, "s": nearEpochText, "d": farDateText, "ds": farDateText},
		{"id": int64(3), "ts": "2024-06-01T12:00:00Z", "s": nearEpochText, "d": "2024-06-01", "ds": farDateText},
		{"id": int64(4), "ts": "1999-01-01T00:00:00Z", "s": nearEpochText, "d": "1999-01-01", "ds": farDateText},
		{"id": int64(5), "ts": nil, "s": nil, "d": nil, "ds": nil},
		{"id": int64(6), "ts": "1970-01-01T00:00:00.500Z", "s": nearEpochText, "d": "4000-01-01", "ds": farDateText},
	}
	ing := db.NewIngester("temporal", schema, nil, ingest.Config{MaxBufferRows: 64, RowGroupSize: 2})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return db
}
