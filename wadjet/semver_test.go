package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/oracle/semvergen"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// THE SEMVER FAMILY THROUGH THE PUBLIC API (#967).
//
// The five-arm census lives in internal/coordinator; what belongs here is the
// pair of claims only this door can make: that a predicate over
// `semver_sort_key` prunes NOTHING before it is evaluated, and that the key's
// order survives a memory budget small enough to spill the sort.

const svtTable = "a3pkg"

// svtRows is large enough for several row groups, which is what makes the
// prune counters mean anything.
const (
	svtRows     = 1500
	svtRowGroup = 200
)

// svtFixture builds a table whose version column is MONOTONIC BY PRECEDENCE
// and emphatically NOT by text: `1.0.0, 1.1.0, … 1.1499.0` rises as a version
// and scrambles as bytes, since `1.10.0` sorts between `1.1.0` and `1.2.0`
// there. That is the whole reason a min/max prune on the stored column cannot
// decide a predicate over the KEY: a row group whose text bounds are
// [1.100.0, 1.99.0] holds versions from 1.100.0 to 1.99.0 in neither order.
func svtFixture(t *testing.T) (*DB, context.Context) {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "v", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, svtTable, schema, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	rows := make([]map[string]any, 0, svtRows)
	for i := 0; i < svtRows; i++ {
		rows = append(rows, map[string]any{
			"id": int32(i), "v": fmt.Sprintf("1.%d.0", i),
		})
	}
	ing := db.NewIngester(svtTable, schema, nil, ingest.Config{
		MaxBufferRows: svtRows + 1, RowGroupSize: svtRowGroup,
	})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return db, ctx
}

func svtCount(t *testing.T, db *DB, ctx context.Context, sql string) int64 {
	t.Helper()
	res, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("%s: %d rows, want 1", sql, len(res.Rows))
	}
	n, ok := res.Rows[0][res.Columns[0]].(int64)
	if !ok {
		t.Fatalf("%s: %T, not an int64", sql, res.Rows[0][res.Columns[0]])
	}
	return n
}

// A PREDICATE OVER semver_sort_key PRUNES NOTHING BEFORE IT IS EVALUATED.
//
// A row group's recorded minimum and maximum are the version column's OWN TEXT
// bounds, and the key's order is not the text's — `1.9.0` is above `1.10.0` as
// bytes and below it as a version — so a bound test on the stored column
// cannot decide a predicate over the key. The counter is the observable: two
// answers that agree prove nothing, because a wrong prune changes the answer
// only when a pruned group held a matching row.
//
// The control is beside it: a range on the monotonic `id` column DOES prune
// over this fixture, so a zero delta on the key queries is a decision rather
// than a prune layer that was never armed.
func TestASemverKeyPredicateDoesNoPruningBeforeItIsEvaluated(t *testing.T) {
	db, ctx := svtFixture(t)
	prevStats := scan.StatsPrune.Set(true)
	prevDict := scan.DictPrune.Set(true)
	t.Cleanup(func() {
		scan.StatsPrune.Set(prevStats)
		scan.DictPrune.Set(prevDict)
	})

	prune := func(sql string) (int64, int64) {
		before := scan.StatsPrunedRowGroupsSnapshot()
		n := svtCount(t, db, ctx, sql)
		return n, scan.StatsPrunedRowGroupsSnapshot() - before
	}

	if _, pruned := prune(
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE id >= 1200", svtTable)); pruned == 0 {
		t.Fatal("the control pruned nothing — the prune layer is not armed over this " +
			"fixture, so the cells below would pass vacuously")
	}

	// The literal key for 1.500.0, spelled the way the family builds one. It
	// is written out rather than computed so the gate states what a user would
	// actually have to write beside the column, and it is the one place in the
	// tree that names the key's bytes.
	const key1500 = "0000000000000000001" + "0000000000000000500" + "0000000000000000000" + "~"
	for _, tc := range []struct {
		sql  string
		want int64
	}{
		// Versions strictly above 1.500.0: 1.501.0 … 1.1499.0.
		{fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE semver_sort_key(v) > '%s'",
			svtTable, key1500), svtRows - 501},
		{fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE semver_sort_key(v) > semver_sort_key('1.500.0')",
			svtTable), svtRows - 501},
		// And the same band expressed as a range, which is the spelling a user
		// writes: 1.500.0 through 1.599.0 is a hundred versions.
		{fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE semver_satisfies(v, '~1.500')", svtTable), 1},
		{fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE semver_satisfies(v, '>=1.500.0 <1.600.0')",
			svtTable), 100},
	} {
		n, pruned := prune(tc.sql)
		if pruned != 0 {
			t.Errorf("a predicate over the semver key pruned %d row groups BEFORE it was "+
				"evaluated; a row group's TEXT bounds say nothing about the key's\n  SQL: %s",
				pruned, tc.sql)
		}
		if n != tc.want {
			t.Errorf("got %d rows, want %d\n  SQL: %s", n, tc.want, tc.sql)
		}
	}

	// And the answer does not depend on the prune layer being on.
	for _, sql := range []string{
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE semver_sort_key(v) > '%s'", svtTable, key1500),
		fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE semver_satisfies(v, '>=1.500.0 <1.600.0')", svtTable),
	} {
		on := svtCount(t, db, ctx, sql)
		scan.StatsPrune.Set(false)
		scan.DictPrune.Set(false)
		off := svtCount(t, db, ctx, sql)
		scan.StatsPrune.Set(true)
		scan.DictPrune.Set(true)
		if on != off {
			t.Errorf("PRUNING CHANGED THE ANSWER\n  SQL: %s\n  prune on  = %d\n  prune off = %d",
				sql, on, off)
		}
	}
}

// THE KEY'S ORDER SURVIVES A SPILL, over the generated corpus.
//
// The claim `semver_sort_key` makes is that a consumer which orders BYTES
// orders versions, and an external sort is the consumer that reads those bytes
// back off disk in runs and merges them. A 512 KiB budget over 5000 versions
// engages it; the in-memory arm beside it is the control, and the two orders
// must be identical.
func TestTheSemverKeysOrderSurvivesASpilledSort(t *testing.T) {
	ctx := context.Background()
	corpus := semvergen.Corpus(967, 5000)
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "v", Type: parquet.TypeString},
	}}
	rows := make([]map[string]any, 0, len(corpus))
	for i, v := range corpus {
		rows = append(rows, map[string]any{"id": int32(i), "v": v})
	}

	// The budgeted arm alone is ARMED — ADR-0027 §6: arming both sides cancels
	// a defect that lives in the merge (#790). A 512 KiB budget over 5000
	// short strings does not reach the spill threshold on its own (measured:
	// zero runs written), so the knob is what makes this a spilled arm rather
	// than a second in-memory one.
	order := func(budget int64, force bool) ([]string, bool) {
		if force {
			restoreEvery := exec.ForceSortSpillEvery(1)
			restoreRuns := exec.ForceSmallSpillRuns(512)
			defer func() {
				exec.ForceSortSpillEvery(restoreEvery)
				restoreRuns()
			}()
		}
		beforeRuns := exec.SortRunsWritten.Load()
		cfg := Config{Store: objstore.NewMemStore(), Bucket: "test"}
		if budget > 0 {
			cfg.MemoryBudget = budget
		}
		db, err := Open(ctx, cfg)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		defer db.Close()
		if err := db.CreateTable(ctx, "a3sort", schema, nil); err != nil {
			t.Fatalf("create: %v", err)
		}
		ing := db.NewIngester("a3sort", schema, nil, ingest.Config{
			MaxBufferRows: len(rows) + 1, RowGroupSize: 512,
		})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatalf("ingest: %v", err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush: %v", err)
		}
		res, err := db.Query(ctx,
			`SELECT v FROM a3sort ORDER BY semver_sort_key(v), v`)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		out := make([]string, 0, len(res.Rows))
		for _, r := range res.Rows {
			out = append(out, r["v"].(string))
		}
		return out, exec.SortRunsWritten.Load() > beforeRuns
	}

	inMemory, memSpilled := order(0, false)
	if memSpilled {
		t.Fatal("the UNBUDGETED arm spilled, so the two arms are the same arm and the " +
			"comparison below proves nothing")
	}
	if len(inMemory) != len(corpus) {
		t.Fatalf("the in-memory arm returned %d rows, want %d", len(inMemory), len(corpus))
	}
	spilledOrder, didSpill := order(512*1024, true)
	if !didSpill {
		t.Fatal("the budgeted arm wrote NO sorted run, so it never merged runs off disk " +
			"and this gate compared two in-memory sorts (ADR-0027)")
	}
	if len(spilledOrder) != len(corpus) {
		t.Fatalf("the budgeted arm returned %d rows, want %d", len(spilledOrder), len(corpus))
	}
	for i := range inMemory {
		if inMemory[i] != spilledOrder[i] {
			t.Fatalf("position %d: the in-memory order has %q and the budgeted order has %q",
				i, inMemory[i], spilledOrder[i])
		}
	}

	// And the order really is the specification's, asked of semver_cmp through
	// SQL so the check is the engine's own comparison rather than a second one
	// written here.
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateTable(ctx, "a3sorted", parquet.Schema{Columns: []parquet.Column{
		{Name: "rn", Type: parquet.TypeInt32},
		{Name: "v", Type: parquet.TypeString},
	}}, nil); err != nil {
		t.Fatal(err)
	}
	ranked := make([]map[string]any, 0, len(spilledOrder))
	for i, v := range spilledOrder {
		ranked = append(ranked, map[string]any{"rn": int32(i), "v": v})
	}
	ing := db.NewIngester("a3sorted", parquet.Schema{Columns: []parquet.Column{
		{Name: "rn", Type: parquet.TypeInt32},
		{Name: "v", Type: parquet.TypeString},
	}}, nil, ingest.Config{MaxBufferRows: len(ranked) + 1, RowGroupSize: 512})
	if err := ing.Ingest(ctx, ranked); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := db.Query(ctx, `SELECT COUNT(*) AS n FROM a3sorted a JOIN a3sorted b
	                            ON b.rn = a.rn + 1
	                           WHERE semver_cmp(a.v, b.v) > 0`)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if n := res.Rows[0]["n"].(int64); n != 0 {
		t.Errorf("the spilled order left %d adjacent pairs out of specification order", n)
	}
}

// THE LENIENT/LOUD SPLIT END TO END, through the public API.
func TestTheSemverFamilyIsLenientOnDataAndLoudOnTheQuery(t *testing.T) {
	db, ctx := svtFixture(t)

	// Junk is NULL, not an error, even in a WHERE.
	if n := svtCount(t, db, ctx,
		`SELECT COUNT(*) AS n FROM a3pkg WHERE semver_valid('latest')`); n != 0 {
		t.Errorf("a junk version matched %d rows, want 0", n)
	}
	res, err := db.Query(ctx, `SELECT semver_major('latest') AS m FROM a3pkg WHERE id = 0`)
	if err != nil {
		t.Fatalf("the lenient form refused: %v", err)
	}
	if got := res.Rows[0]["m"]; got != nil {
		t.Errorf("semver_major('latest') = %v, want NULL", got)
	}

	// A range that names no range is 22023 — and with NO ROWS AT ALL, which is
	// the shape a per-row fold gets wrong.
	for _, sql := range []string{
		`SELECT COUNT(*) AS n FROM a3pkg WHERE id < 0 AND semver_satisfies(v,'^^1.0')`,
		`SELECT semver_satisfies(v,'') AS s FROM a3pkg WHERE id < 0`,
		`SELECT semver_satisfies(v,'1.2.x-beta') AS s FROM a3pkg WHERE id < 0`,
	} {
		if _, err := db.Query(ctx, sql); err == nil {
			t.Errorf("%s answered rows instead of refusing", sql)
		} else if code := sqlerr.StateOf(err); code != "22023" {
			t.Errorf("%s answered SQLSTATE %q, want 22023 (%v)", sql, code, err)
		}
	}

	// The strict normalizer raises on a reached row; its lenient twin does not.
	if _, err := db.Query(ctx,
		`SELECT semver_normalize_strict('latest') AS n FROM a3pkg WHERE id = 0`); err == nil {
		t.Error("semver_normalize_strict answered rows for a string that is not a version")
	} else if code := sqlerr.StateOf(err); code != "22023" {
		t.Errorf("semver_normalize_strict answered SQLSTATE %q, want 22023 (%v)", code, err)
	}
}

// A RANGE WHOSE BOUND SITS AT THE ACCEPTANCE BOUND STILL NAMES ITS ROWS (#967).
//
// Adopted from the round-1 review's probe. A component is accepted up to
// int64's MAXIMUM — `semver_major('9223372036854775807.0.0')` answers it, and
// the shipped corpus draws components from that value — so every desugaring
// that closes a band by raising a component by one can be handed one with
// nowhere to go. Wrapped, the upper half drops every row the query named and
// the lower half admits every row: the wrong boolean in both directions, with
// no refusal to show for it. Saturated, the bound means what the expansion
// means, and these counts are what it means.
//
// The `1.M.x` cells are the ones that would survive a WRONG fix: a bound
// "made unbounded" rather than saturated would admit 2.0.0, which is outside
// the band the query wrote.
func TestARangeAtTheAcceptanceBoundKeepsTheRowsItNames(t *testing.T) {
	const max = "9223372036854775807"
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "v", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "a3max", schema, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	versions := []string{
		max + ".0.0",      // 1
		"1.0.0",           // 2
		"5.0.0",           // 3
		max + ".9.9",      // 4
		"1." + max + ".0", // 5
		"2.0.0",           // 6
		"0.0." + max,      // 7
	}
	rows := make([]map[string]any, 0, len(versions))
	for i, v := range versions {
		rows = append(rows, map[string]any{"id": int32(i + 1), "v": v})
	}
	ing := db.NewIngester("a3max", schema, nil, ingest.Config{MaxBufferRows: 16, RowGroupSize: 16})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	for _, tc := range []struct {
		rng  string
		want int64
		why  string
	}{
		{"^" + max + ".0.0", 2, "the two rows whose major is the maximum"},
		{max + ".x", 2, "the same band, spelled as an X-range"},
		{"~" + max + ".x", 2, "the same band, spelled with a tilde"},
		{">" + max + ".x", 0, "nothing is above the top of the domain"},
		{"<=" + max + ".x", 7, "everything is below it"},
		{"1.2.3 - " + max, 5, "every row at or above 1.2.3"},
		{"^0.0." + max, 1, "the single version at that patch"},
		{"1." + max + ".x", 1, "the 1.max band, and NOT 2.0.0"},
		{"~1." + max + ".0", 1, "the same band under a tilde"},
		{">1." + max + ".x", 4, "the rows above the 1.max band"},
	} {
		sql := fmt.Sprintf(`SELECT COUNT(*) AS n FROM a3max WHERE semver_satisfies(v, '%s')`, tc.rng)
		if n := svtCount(t, db, ctx, sql); n != tc.want {
			t.Errorf("%s matched %d rows, want %d (%s)", tc.rng, n, tc.want, tc.why)
		}
	}

	// The scalar answer, both directions, where the review measured the
	// wrong boolean.
	for _, tc := range []struct {
		expr string
		want bool
	}{
		{fmt.Sprintf(`semver_satisfies('%s.0.0','^%s.0.0')`, max, max), true},
		{fmt.Sprintf(`semver_satisfies('1.0.0','>%s.x')`, max), false},
		{fmt.Sprintf(`semver_satisfies('2.0.0','1.%s.x')`, max), false},
		{fmt.Sprintf(`semver_satisfies('1.%s.%s','1.%s.x')`, max, max, max), true},
	} {
		res, err := db.Query(ctx, `SELECT `+tc.expr+` AS s FROM a3max WHERE id = 1`)
		if err != nil {
			t.Fatalf("%s: %v", tc.expr, err)
		}
		if got := res.Rows[0]["s"]; got != tc.want {
			t.Errorf("%s = %v, want %v", tc.expr, got, tc.want)
		}
	}
}

// THE TRIVIAL LOWER BOUND THROUGH THE SQL DOOR (#967, round-2 review B1).
//
// node-semver deletes a comparator whose text is exactly `>=0.0.0` from every
// comparator set (`replaceGTE0`), and the deletion is a VALUE: `>=0.0.0` is
// false for exactly the pre-releases of 0.0.0, so keeping it dropped a row the
// rest of the set admits. The cell-by-cell agreement with the library is the
// expr gate's 2,970 cells; this is the same claim asked through SQL, with the
// two CONTROLS that say the change is confined to the one comparator — the
// same shapes over any other core still answer false.
//
// The expectations are node-semver 7.7.3's own answers, measured.
func TestARangeWithATrivialLowerBoundAnswersWhatNodeSemverAnswers(t *testing.T) {
	db, ctx := svtFixture(t)
	for _, tc := range []struct {
		expr string
		want bool
		why  string
	}{
		{`semver_satisfies('0.0.0-alpha','0.0.0 - 0.0.0-alpha')`, true,
			"the hyphen range's low end desugars to the trivial bound"},
		{`semver_satisfies('0.0.0-alpha','0 - 0.0.0-alpha')`, true, "the same, spelled as a major"},
		{`semver_satisfies('0.0.0-alpha','0.x - 0.0.0-alpha')`, true, "the same, spelled as an X-range"},
		{`semver_satisfies('0.0.0-0','>=0.0.0 <=0.0.0-alpha')`, true, "written out as comparators"},
		{`semver_satisfies('0.0.0-beta','>=0.0.0 >=0.0.0-alpha')`, true, "two lower bounds, one trivial"},
		{`semver_satisfies('0.0.0-alpha','>=0.0.0 || <=0.0.0-alpha')`, false,
			"an ANY alternative makes the whole range ANY, which admits no pre-release"},
		{`semver_satisfies('0.0.0-alpha','* || <=0.0.0-alpha')`, false,
			"the same rule reached without a strip"},
		// The controls: any other core is untouched, and the equality and the
		// `-0` spelling are not the comparator node deletes.
		{`semver_satisfies('1.0.0-alpha','1.0.0 - 1.0.0-alpha')`, false, "control: a different core"},
		{`semver_satisfies('0.0.1-alpha','>=0.0.1 <=0.0.1-alpha')`, false, "control: a different core"},
		{`semver_satisfies('0.0.0-alpha','>=0.0.0-0 <=0.0.0-alpha')`, true,
			"control: >=0.0.0-0 is a different comparator and is kept"},
		{`semver_satisfies('0.0.0','0.0.0 - 0.0.0-alpha')`, false,
			"control: the release itself is above the upper bound"},
		{`semver_satisfies('0.0.0','^0.x')`, true, "control: a release still satisfies the band"},
		{`semver_satisfies('1.0.0','^0.x')`, false, "control: the upper bound still bounds"},
		{`semver_satisfies('0.0.0-alpha','*')`, false,
			"control: a range that names no pre-release admits none"},
		{`semver_satisfies('0.0.0-alpha','>=0.0.0')`, false, "control: the same, alone"},
		// The reachable shape: Go module pseudo-versions are literally
		// v0.0.0-<timestamp>-<hash>, so "every pseudo-version built before
		// 2022" is a range people write over a go.sum or an SBOM — and the
		// numeric reading made it name nothing at all.
		{`semver_satisfies('v0.0.0-20210101000000-abcdef123456',
		                   '>=0.0.0 <0.0.0-20220101000000-000000000000')`, true,
			"a pseudo-version below the cutoff"},
		{`semver_satisfies('v0.0.0-20230101000000-abcdef123456',
		                   '>=0.0.0 <0.0.0-20220101000000-000000000000')`, false,
			"and one above it"},
	} {
		res, err := db.Query(ctx, `SELECT `+tc.expr+` AS s FROM a3pkg WHERE id = 0`)
		if err != nil {
			t.Fatalf("%s: %v", tc.expr, err)
		}
		if got := res.Rows[0]["s"]; got != tc.want {
			t.Errorf("%s = %v, want %v (%s)", tc.expr, got, tc.want, tc.why)
		}
	}
}
