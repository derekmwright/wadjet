package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// THE TCP FLAG FAMILY THROUGH THE WHOLE ENGINE (#966).
//
// The unit gates in internal/engine/expr hold the functions to PostgreSQL's
// bit arithmetic; these hold the QUERY to it — parser, planner, pushdown,
// scan, exec — over a flow table with an INT32 and an INT64 flags column
// holding the same values.
//
// Every expectation is the answer PostgreSQL 17.11 printed on the shared
// oracle server for the equivalent bit spelling over the same rows:
//
//	 f  | (f&18)=18 | (f&18)<>0 | (f&18)=0
//	----+-----------+-----------+----------
//	  0 |     f     |     f     |    t
//	  2 |     f     |     t     |    f
//	 18 |     t     |     t     |    f
//	 16 |     f     |     t     |    f
//	  4 |     f     |     f     |    t
//	511 |     t     |     t     |    f
//	 24 |     f     |     t     |    f
//	 20 |     f     |     t     |    f
//	256 |     f     |     f     |    t
//	NULL|   NULL    |   NULL    |   NULL

// a2fValues8 / a2fValues4 are the fourteen flag values, in the order the rows
// are ingested, at each column's own width; the fifteenth row of every repeat
// is NULL. Row ids are 1-based, so id i of the first repeat holds index i-1.
//
// The last four are the round-2 addition (#966 P3): a flags value is a BIT
// PATTERN, not a magnitude — `(-1) & 18 = 18` on PostgreSQL — and the first
// draft of this fixture masked the INT32 column with `v & 0x1FF`, which turned
// every negative into a positive before any predicate saw it. Both int64
// extremes are here because a mask over them is where a float carrier and a
// sign-extension mistake both show.
var (
	a2fValues8 = []int64{0, 2, 18, 16, 4, 511, 24, 20, 256, 1<<62 | 18,
		-1, -18, -9223372036854775808, 9223372036854775807}
	a2fValues4 = []int32{0, 2, 18, 16, 4, 511, 24, 20, 256, 18,
		-1, -18, -2147483648, 2147483647}
)

// flagFixture registers a flow table whose two flag columns hold identical
// values at INT32 and INT64 width, repeated so the table spans several row
// groups — a single-row-group fixture cannot see a prune decision at all
// (#965 round 2's lesson).
func flagFixture(t *testing.T) (*DB, context.Context) {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "f4", Type: parquet.TypeInt32, Nullable: true},
		{Name: "f8", Type: parquet.TypeInt64, Nullable: true},
		{Name: "note", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "a2flow", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 0, 1500)
	for rep := 0; rep < 100; rep++ {
		for i := range a2fValues8 {
			rows = append(rows, map[string]any{
				"id":   int32(rep*15 + i + 1),
				"f4":   a2fValues4[i],
				"f8":   a2fValues8[i],
				"note": fmt.Sprintf("r%03d", rep),
			})
		}
		rows = append(rows, map[string]any{
			"id": int32(rep*15 + 15), "f4": nil, "f8": nil, "note": nil,
		})
	}
	ing := db.NewIngester("a2flow", schema, nil, ingest.Config{
		MaxBufferRows: len(rows) + 1, RowGroupSize: 220,
	})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db, ctx
}

func flagCount(t *testing.T, db *DB, ctx context.Context, sql string) int64 {
	t.Helper()
	res, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("%v\n  SQL: %s", err, sql)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("want one row, got %d\n  SQL: %s", len(res.Rows), sql)
	}
	n, ok := tmAsInt64(res.Rows[0][res.Columns[0]])
	if !ok {
		t.Fatalf("COUNT(*) came back as %#v\n  SQL: %s", res.Rows[0][res.Columns[0]], sql)
	}
	return n
}

// The values, end to end, on both integer widths — against the PostgreSQL
// transcript and against the engine's own BITWISE_AND spelling of it.
func TestTheTCPFlagFamilyAnswersPostgresBitArithmeticEndToEnd(t *testing.T) {
	db, ctx := flagFixture(t)

	// PostgreSQL's counts over the 1500 rows: each of the fourteen values
	// appears 100 times, plus 100 NULL rows that match nothing. Measured on
	// the shared oracle server over `a2_tcpflow2`, the same fourteen values,
	// and multiplied by the repeat count.
	//
	// f8 holds 2^62|18 where f4 holds 18, and the int64 extremes where f4
	// holds the int32 ones, so the two columns answer the SAME counts for a
	// nine-bit mask — which is the point of carrying the wide values: a
	// float64 carrier answers 100 fewer for the first, and a magnitude reading
	// of a negative answers differently for the others.
	for _, tc := range []struct {
		sql  string
		want int64
	}{
		{"SELECT COUNT(*) AS n FROM a2flow WHERE tcp_flags_has_all(f8, 'SYN', 'ACK')", 500},
		{"SELECT COUNT(*) AS n FROM a2flow WHERE tcp_flags_has_all(f4, 'SYN', 'ACK')", 500},
		{"SELECT COUNT(*) AS n FROM a2flow WHERE tcp_flags_has_any(f8, 'SYN', 'ACK')", 1000},
		{"SELECT COUNT(*) AS n FROM a2flow WHERE tcp_flags_has_any(f4, 'SYN', 'ACK')", 1000},
		{"SELECT COUNT(*) AS n FROM a2flow WHERE tcp_flags_has_none(f8, 'SYN', 'ACK')", 400},
		{"SELECT COUNT(*) AS n FROM a2flow WHERE tcp_flags_has_none(f4, 'SYN', 'ACK')", 400},
		// The bit spelling of the same three, which is the value oracle.
		{"SELECT COUNT(*) AS n FROM a2flow WHERE BITWISE_AND(f8, 18) = 18", 500},
		{"SELECT COUNT(*) AS n FROM a2flow WHERE BITWISE_AND(f8, 18) <> 0", 1000},
		{"SELECT COUNT(*) AS n FROM a2flow WHERE BITWISE_AND(f8, 18) = 0", 400},
		{"SELECT COUNT(*) AS n FROM a2flow WHERE BITWISE_AND(f4, 18) = 18", 500},
		// One bit each.
		{"SELECT COUNT(*) AS n FROM a2flow WHERE tcp_flags_has_all(f8, 'SYN')", 700},
		{"SELECT COUNT(*) AS n FROM a2flow WHERE tcp_flags_has_all(f8, 'RST')", 600},
		{"SELECT COUNT(*) AS n FROM a2flow WHERE tcp_flags_has_all(f8, 'AE')", 500},
		// All nine bits: only the 511 rows.
		{`SELECT COUNT(*) AS n FROM a2flow WHERE tcp_flags_has_all(f8,
		    'FIN','SYN','RST','PSH','ACK','URG','ECE','CWR','AE')`, 300},
		// NULL matches none of the three, has_none included.
		{"SELECT COUNT(*) AS n FROM a2flow WHERE f8 IS NULL AND tcp_flags_has_none(f8, 'ACK')", 0},
		// A NOT over the predicate is the three-valued complement: the NULL
		// rows are excluded from both sides, exactly as PostgreSQL's
		// `NOT ((f & 18) = 18)` is.
		{"SELECT COUNT(*) AS n FROM a2flow WHERE NOT tcp_flags_has_all(f8, 'SYN', 'ACK')", 900},
	} {
		t.Run(tc.sql[:min(len(tc.sql), 76)], func(t *testing.T) {
			if got := flagCount(t, db, ctx, tc.sql); got != tc.want {
				t.Errorf("got %d rows, PostgreSQL's bit arithmetic says %d\n  SQL: %s",
					got, tc.want, tc.sql)
			}
		})
	}

	// The NEGATIVE and EXTREME rows, which is where a magnitude reading of a
	// flags value shows (#966 round 2, P3). PostgreSQL: `(-1) & 18 = 18`, and
	// int64 min has only its sign bit set so it matches nothing in the nine.
	for _, tc := range []struct {
		sql  string
		want int64
	}{
		{"SELECT COUNT(*) AS n FROM a2flow WHERE id % 15 = 11 AND tcp_flags_has_all(f8,'SYN','ACK')", 100},
		{"SELECT COUNT(*) AS n FROM a2flow WHERE id % 15 = 11 AND tcp_flags_has_all(f4,'SYN','ACK')", 100},
		{"SELECT COUNT(*) AS n FROM a2flow WHERE id % 15 = 13 AND tcp_flags_has_any(f8,'FIN','SYN','RST','PSH','ACK','URG','ECE','CWR','AE')", 0},
		{"SELECT COUNT(*) AS n FROM a2flow WHERE id % 15 = 13 AND tcp_flags_has_none(f8,'ACK')", 100},
		{"SELECT COUNT(*) AS n FROM a2flow WHERE id % 15 = 14 AND tcp_flags_has_all(f8,'SYN','ACK')", 100},
	} {
		if got := flagCount(t, db, ctx, tc.sql); got != tc.want {
			t.Errorf("got %d rows, PostgreSQL's bit arithmetic says %d\n  SQL: %s",
				got, tc.want, tc.sql)
		}
	}

	// The renderers, projected.
	res, err := db.Query(ctx, `SELECT tcp_flags_text(f8) AS s, tcp_flag_mask('SYN','ACK') AS m
	                           FROM a2flow WHERE id = 3`)
	if err != nil {
		t.Fatal(err)
	}
	if got := fmt.Sprint(res.Rows[0]["s"]); got != "SYN|ACK" {
		t.Errorf("tcp_flags_text(18) = %q, want \"SYN|ACK\"", got)
	}
	if got, _ := tmAsInt64(res.Rows[0]["m"]); got != 18 {
		t.Errorf("tcp_flag_mask('SYN','ACK') = %v, want 18", res.Rows[0]["m"])
	}

	// The DECLARED types, which a value comparison cannot see: a predicate is
	// BOOL, the mask is an int4-width integer, the text renderer is TEXT.
	res, err = db.Query(ctx, `SELECT tcp_flags_has_all(f8,'SYN') AS b, tcp_flag_mask('SYN') AS m,
	                                 tcp_flags_text(f8) AS s
	                          FROM a2flow WHERE id = 3`)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]parquet.TypeID{
		"b": parquet.TypeBool, "m": parquet.TypeInt32, "s": parquet.TypeString,
	}
	for _, cm := range res.ColumnMetas {
		if w, ok := want[cm.Name]; ok && cm.TypeID != w {
			t.Errorf("column %s declares %v, want %v", cm.Name, cm.TypeID, w)
		}
	}
}

// `tcp_flags` REALLY RETURNS AN ARRAY, and a top-level projection of it is
// TEXT — a pre-existing engine limitation, pinned here so it fails when fixed.
//
// The registry declares RetArray and the value is a real []any: element_at and
// array_length read it as one. But `scalarFnDeclaredType`
// (internal/planner/physical/plan.go) DECLINES every ARRAY/MAP/ROW-returning
// function, in as many words — "a projection has no element type to size the
// child vector with and an ARRAY column built without one reads back empty" —
// so the projection falls back to text and the client is handed Go's rendering
// of the slice rather than a slice or PostgreSQL's `{SYN,ACK}`. `map_keys`,
// `map_values` and `map_entries` have answered that way since they were added;
// this function inherits it and does not cause it.
//
// It is asserted rather than glossed because when a projection CAN carry a
// nested type, this test fails, and that failure is the reminder to move the
// expectation to ARRAY (and to check the wire, where ARRAY declares OID 25 —
// ADR-0012, #992). Fixing it is a change to every container-returning function
// at once, not to this one.
func TestATopLevelTCPFlagsProjectionIsTextToday(t *testing.T) {
	db, ctx := flagFixture(t)
	res, err := db.Query(ctx, `SELECT tcp_flags(f8) AS a,
	                                  element_at(tcp_flags(f8), 1) AS e,
	                                  array_length(tcp_flags(f8)) AS n
	                           FROM a2flow WHERE id = 3`)
	if err != nil {
		t.Fatal(err)
	}
	// The ARRAY is real where a consumer reads it as one.
	if got := fmt.Sprint(res.Rows[0]["e"]); got != "SYN" {
		t.Errorf("element_at(tcp_flags(18), 1) = %q, want \"SYN\"", got)
	}
	if got, _ := tmAsInt64(res.Rows[0]["n"]); got != 2 {
		t.Errorf("array_length(tcp_flags(18)) = %v, want 2", res.Rows[0]["n"])
	}
	for _, cm := range res.ColumnMetas {
		if cm.Name != "a" {
			continue
		}
		if cm.TypeID != parquet.TypeString {
			t.Errorf("a top-level tcp_flags() projection now declares %v rather than "+
				"STRING. If a projection can carry a nested type, that is the fix — "+
				"move this expectation to ARRAY, and check the wire OID with it "+
				"(ADR-0012, #992).", cm.TypeID)
		}
	}
}

// An unknown flag name is a REFUSAL that reaches the client, with PostgreSQL's
// invalid_parameter_value SQLSTATE and the name in the message — not a query
// that answers a larger row set.
func TestAnUnknownFlagNameRefusesTheQuery(t *testing.T) {
	db, ctx := flagFixture(t)
	for _, tc := range []struct{ sql, msg string }{
		{"SELECT COUNT(*) AS n FROM a2flow WHERE tcp_flags_has_all(f8, 'SYN', 'ACKK')",
			`TCP flag name "ACKK" not recognized`},
		{"SELECT tcp_flag_mask('SYNN') AS m FROM a2flow WHERE id = 1",
			`TCP flag name "SYNN" not recognized`},
		{"SELECT COUNT(*) AS n FROM a2flow WHERE tcp_flags_has_any(f8)",
			"tcp_flags_has_any requires at least one TCP flag name"},
		{"SELECT tcp_flags_text(note) AS s FROM a2flow WHERE id = 1",
			"the flags argument must be an integer"},
	} {
		t.Run(tc.msg, func(t *testing.T) {
			_, err := db.Query(ctx, tc.sql)
			if err == nil {
				t.Fatalf("answered where 22023 is due\n  SQL: %s", tc.sql)
			}
			if state := sqlerr.StateOf(err); state != "22023" {
				t.Errorf("SQLSTATE %s, want 22023\n  err: %v", state, err)
			}
			if !strings.Contains(err.Error(), tc.msg) {
				t.Errorf("%q does not carry %q", err, tc.msg)
			}
		})
	}
}

// THE PUSHDOWN IS MEASURED, NOT INFERRED.
//
// A pushdown that silently stopped firing answers exactly what a full scan
// answers, so rows prove nothing about it. These read the scan's own counters
// across one query, and assert the row count is IDENTICAL with the pushdown on
// and off — which is what a kill switch is for.
func TestAFlagPredicateReachesTheScanAndTheAnswerDoesNotDependOnIt(t *testing.T) {
	db, ctx := flagFixture(t)

	for _, tc := range []struct {
		name string
		sql  string
		want int64
	}{
		{"has_all", "SELECT COUNT(*) AS n FROM a2flow WHERE tcp_flags_has_all(f8, 'SYN', 'ACK')", 500},
		{"has_any", "SELECT COUNT(*) AS n FROM a2flow WHERE tcp_flags_has_any(f4, 'SYN', 'ACK')", 1000},
		{"has_none", "SELECT COUNT(*) AS n FROM a2flow WHERE tcp_flags_has_none(f8, 'ACK')", 600},
		{"bitwise_all", "SELECT COUNT(*) AS n FROM a2flow WHERE BITWISE_AND(f8, 18) = 18", 500},
		{"bitwise_any", "SELECT COUNT(*) AS n FROM a2flow WHERE BITWISE_AND(f4, 4) <> 0", 600},
		{"bitwise_none", "SELECT COUNT(*) AS n FROM a2flow WHERE BITWISE_AND(f8, 16) = 0", 600},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prev := scan.FlagDictPushdown.Set(true)
			defer scan.FlagDictPushdown.Set(prev)

			scan.ResetFlagPushdownStats()
			on := flagCount(t, db, ctx, tc.sql)
			entries, masks, plain := scan.FlagPushdownStatsSnapshot()
			if entries+masks+plain == 0 {
				t.Fatalf("the flag predicate never reached the scan filter — no counter "+
					"moved for\n  SQL: %s", tc.sql)
			}
			// Which arm ran is a property of the FILE, not of the predicate:
			// this engine's writer emits PLAIN pages, so an ingested fixture
			// exercises andPlainPage here and the per-dictionary-entry arm is
			// measured against the pure-dictionary fixture in
			// scan.TestAFlagPredicateOnADictionaryPageIsEvaluatedPerEntry.
			t.Logf("scan filter: %d dictionary entries, %d dictionary masks, %d plain values",
				entries, masks, plain)

			scan.FlagDictPushdown.Set(false)
			scan.ResetFlagPushdownStats()
			off := flagCount(t, db, ctx, tc.sql)
			e2, m2, p2 := scan.FlagPushdownStatsSnapshot()
			if e2+m2+p2 != 0 {
				t.Errorf("WADJET_FLAG_DICT_PUSHDOWN=0 still evaluated %d/%d/%d in the scan; "+
					"the switch is not a kill switch", e2, m2, p2)
			}
			if on != off {
				t.Errorf("PUSHDOWN CHANGED THE ANSWER\n  SQL: %s\n  pushdown on  = %d\n"+
					"  pushdown off = %d", tc.sql, on, off)
			}
			if on != tc.want {
				t.Errorf("got %d rows, PostgreSQL's bit arithmetic says %d\n  SQL: %s",
					on, tc.want, tc.sql)
			}
		})
	}
}

// A FLAG PREDICATE NEVER PRUNES A ROW GROUP.
//
// A min/max range cannot prove a bit, so nothing here may skip a row group on
// a flag conjunct alone. The COUNTER is the observable: two answers that agree
// prove nothing, because a prune that skipped a group holding matching rows
// changes the answer only when such a group exists.
//
// The control is beside it: the same fixture with a RANGE predicate on `id`
// does prune, so a zero delta on the flag query is a decision rather than a
// prune layer that was never armed.
func TestAFlagPredicateNeverPrunesARowGroup(t *testing.T) {
	db, ctx := flagFixture(t)
	prevStats := scan.StatsPrune.Set(true)
	prevDict := scan.DictPrune.Set(true)
	t.Cleanup(func() {
		scan.StatsPrune.Set(prevStats)
		scan.DictPrune.Set(prevDict)
	})

	prune := func(sql string) (int64, int64) {
		before := scan.StatsPrunedRowGroupsSnapshot()
		n := flagCount(t, db, ctx, sql)
		return n, scan.StatsPrunedRowGroupsSnapshot() - before
	}

	// The control: a range on the monotonic id column prunes, so the layer is
	// armed over this fixture.
	if _, pruned := prune("SELECT COUNT(*) AS n FROM a2flow WHERE id >= 900"); pruned == 0 {
		t.Fatal("the control pruned nothing — the prune layer is not armed over this " +
			"fixture, so the flag cells below would pass vacuously")
	}
	for _, sql := range []string{
		"SELECT COUNT(*) AS n FROM a2flow WHERE tcp_flags_has_all(f8, 'SYN', 'ACK')",
		"SELECT COUNT(*) AS n FROM a2flow WHERE tcp_flags_has_none(f4, 'ACK')",
		"SELECT COUNT(*) AS n FROM a2flow WHERE BITWISE_AND(f8, 18) = 18",
	} {
		if _, pruned := prune(sql); pruned != 0 {
			t.Errorf("a flag predicate pruned %d row groups; a min/max range cannot "+
				"prove a bit\n  SQL: %s", pruned, sql)
		}
	}

	// And the answer is the same with both prunes off, which is the property
	// the counter cannot state on its own.
	for _, sql := range []string{
		"SELECT COUNT(*) AS n FROM a2flow WHERE tcp_flags_has_all(f8, 'SYN', 'ACK')",
		"SELECT COUNT(*) AS n FROM a2flow WHERE tcp_flags_has_any(f4, 'RST', 'FIN')",
	} {
		scan.StatsPrune.Set(true)
		scan.DictPrune.Set(true)
		on := flagCount(t, db, ctx, sql)
		scan.StatsPrune.Set(false)
		scan.DictPrune.Set(false)
		off := flagCount(t, db, ctx, sql)
		if on != off {
			t.Errorf("PRUNING CHANGED THE ANSWER\n  SQL: %s\n  on = %d\n  off = %d", sql, on, off)
		}
	}
}
