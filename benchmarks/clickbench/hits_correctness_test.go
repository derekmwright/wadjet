package clickbench

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/wadjet"
)

const (
	duckdbBin          = "/tmp/duckdb"
	clickbenchBaseline = "baseline-duckdb-hits1m.json"
	floatEps           = 1e-4
)

// TestHitsCorrectness is the ClickBench correctness gate: all 43 queries run
// through wadjet over a real hits part (WADJET_HITS_PART) and their results
// must match DuckDB's on the same file. DuckDB runs the OFFICIAL
// duckdb-parquet dialect (queries-duckdb.sql + the official view/macro
// setup); wadjet runs our dialect (queries.sql); the two are paired by
// index — per-engine dialect files with identical semantics, exactly how
// ClickBench defines the workload.
//
// Comparison is positional (column i ↔ column i; names differ per dialect)
// over canonicalized cells read through QueryResult.Cells, with rows sorted
// canonically so intra-result tie order between engines can't flake the
// gate. It is positional for a reason — see canonicalCells.
//
// Two drifts this gate reported were the GATE and the DIALECT FILE, not the
// engine, and both are recorded here so the next reader does not re-derive
// them (ADR-0013: a gate's own reading of a legal result is not a class of
// nondeterminism, it is a defect in the gate):
//
//   - Q23, Q30, Q36 drifted at 042f9852 (v0.18.43), which made an unaliased
//     output column carry the name PostgreSQL gives it. That is correct, and
//     it made duplicate output names ordinary: Q30's ninety sums are ninety
//     columns called `sum`, Q36's three offsets are three called `?column?`,
//     Q23's two MINs are two called `min`. The gate read QueryResult.Rows,
//     which is keyed by name and holds one of each, so it compared the same
//     cell repeatedly against a correct engine. Fixed by reading Cells.
//
//   - Q28, Q29 drifted at 311c79eb (v0.18.34), which made LENGTH count
//     CHARACTERS as PostgreSQL does. The workload's reference semantics are
//     ClickHouse's, where `length` counts BYTES — which is why the DuckDB
//     dialect says STRLEN and not DuckDB's character-counting `length`. The
//     wadjet dialect's LENGTH silently stopped being the paired spelling
//     (Q28's AVG reads 76.4367 in bytes and 73.9680 in characters over
//     hits_0, where 137578 of 1M URLs are multi-byte),
//     so queries.sql now says OCTET_LENGTH, which is what ClickHouse's
//     `length` means here. The stored baseline stays valid.
//
// Modes (mirrors benchmarks/tpch/duckdb_compare_test.go):
//
//	default — compare wadjet output against the stored DuckDB checksums in
//	          baseline-duckdb-hits1m.json. No DuckDB binary required.
//	WADJET_CLICKBENCH_DUCKDB=1 — additionally shell out to /tmp/duckdb for
//	          live cell-by-cell cross-engine comparison.
//	WADJET_REGENERATE_CLICKBENCH_BASELINE=1 — regenerate the stored baseline
//	          from live DuckDB output. Requires /tmp/duckdb.
//
// DuckDB CLI setup:
//
//	wget -q https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip -O /tmp/duckdb.zip
//	unzip -d /tmp /tmp/duckdb.zip
func TestHitsCorrectness(t *testing.T) {
	regenerate := os.Getenv("WADJET_REGENERATE_CLICKBENCH_BASELINE") == "1"
	liveCompare := regenerate || os.Getenv("WADJET_CLICKBENCH_DUCKDB") == "1"

	ctx := context.Background()
	db, _ := openHitsDB(t, ctx)
	partPath := os.Getenv("WADJET_HITS_PART")

	if liveCompare {
		if _, err := os.Stat(duckdbBin); err != nil {
			t.Fatalf("live compare requires %s: %v", duckdbBin, err)
		}
	}

	wQueries := loadHitsQueries(t)
	dQueries := loadQueriesFile(t, "queries-duckdb.sql")
	if len(wQueries) != len(dQueries) {
		t.Fatalf("dialect files disagree: %d wadjet vs %d duckdb queries", len(wQueries), len(dQueries))
	}

	// Official duckdb-parquet setup (ClickBench duckdb-parquet/create.sql),
	// pointed at the local part.
	duckdbSetup := fmt.Sprintf(
		"CREATE VIEW hits AS SELECT * REPLACE (make_date(EventDate) AS EventDate) FROM read_parquet('%s', binary_as_string=True);\n"+
			"CREATE MACRO toDateTime(t) AS epoch_ms(t * 1000);\n", partPath)

	stored := loadClickBenchBaseline(t)
	updated := make(map[int]clickbenchBaselineEntry, len(wQueries))

	matches := 0
	for i := range wQueries {
		qNum := i + 1
		t.Run(fmt.Sprintf("Q%02d", qNum), func(t *testing.T) {
			// The comparison runs each query WITHOUT its trailing
			// LIMIT/OFFSET: at the LIMIT boundary both engines admit
			// arbitrary members of a tie group (ORDER BY c DESC LIMIT 10
			// with dozens of count ties), so limited output is not
			// deterministic across engines. Comparing the full result
			// multiset is strictly stronger and tie-immune. Perf runs use
			// the official queries verbatim — this transform is
			// correctness-harness-only.
			wq, dq := stripLimit(wQueries[i]), stripLimit(dQueries[i])

			qctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			res, err := db.Query(qctx, wq)
			cancel()
			if err != nil {
				t.Errorf("wadjet error: %v", err)
				return
			}

			if liveCompare {
				dRows, dCols, err := runDuckDBCSV(duckdbSetup, dq)
				if err != nil {
					t.Errorf("duckdb error: %v", err)
					return
				}
				wRows := canonicalCells(res, dCols)
				dCanon := canonicalStringRows(dRows)
				if regenerate {
					updated[qNum] = clickbenchBaselineEntry{
						RowCount: len(dCanon),
						Checksum: checksumRowsUnordered(dCanon),
					}
					if !canonRowsEqual(wRows, dCanon) {
						t.Errorf("wadjet DIVERGES from the baseline being written")
						reportCanonDiff(t, wRows, dCanon)
					}
					t.Logf("(regen) rows=%d", len(dCanon))
					return
				}
				if !canonRowsEqual(wRows, dCanon) {
					t.Errorf("live cross-engine divergence")
					reportCanonDiff(t, wRows, dCanon)
					return
				}
				matches++
				return
			}

			wRows := canonicalCells(res, nil)
			want, ok := stored[qNum]
			if !ok {
				t.Errorf("no stored baseline (rows=%d). Regenerate with WADJET_REGENERATE_CLICKBENCH_BASELINE=1", len(wRows))
				return
			}
			if len(wRows) != want.RowCount {
				t.Errorf("row count: got %d, want %d (DuckDB)", len(wRows), want.RowCount)
				return
			}
			if got := checksumRowsUnordered(wRows); got != want.Checksum {
				t.Errorf("content drift vs stored DuckDB baseline: got %s want %s\nfirst row: %v",
					got, want.Checksum, first(wRows))
				return
			}
			matches++
		})
	}
	if regenerate {
		writeClickBenchBaseline(t, updated)
		t.Logf("wrote %s with %d entries", clickbenchBaseline, len(updated))
		return
	}
	t.Logf("Summary: %d/%d queries matched DuckDB", matches, len(wQueries))
}

func loadQueriesFile(tb testing.TB, name string) []string {
	tb.Helper()
	f, err := os.Open(name)
	if err != nil {
		tb.Fatal(err)
	}
	defer f.Close()
	var queries []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if q := sc.Text(); q != "" {
			queries = append(queries, q)
		}
	}
	if err := sc.Err(); err != nil {
		tb.Fatal(err)
	}
	return queries
}

// canonicalCells converts a wadjet result into canonical positional cell
// slices, sorted canonically (see canonCell for the normalization rules).
//
// It reads POSITIONALLY, through QueryResult.Cells, and never through the
// name-keyed Rows map. A legal result may carry two columns of the same
// name — PostgreSQL names both columns of `SELECT MIN(a), MIN(b)` `min`
// and both of `SELECT a+1, a+2` `?column?`, and #513 plus the E3 naming
// arc made this engine agree — and a map cannot hold both, so a map read
// silently answers the LAST one for every colliding position. That read is
// what failed Q23, Q30 and Q36 against a correct engine: Q30's ninety
// `SUM(ResolutionWidth + k)` columns are all named `sum`, so the map held
// one of them and the gate compared ninety copies of `SUM(… + 89)`.
//
// dCols, when non-nil, is DuckDB's header: wadjet emits SELECT * output
// alphabetically and DuckDB in file order, so the cells are permuted into
// DuckDB's order when — and only when — every wadjet column name is unique
// and present there. Expression columns are named differently per dialect
// and stay positional, which is already their alignment.
func canonicalCells(res *wadjet.QueryResult, dCols []string) [][]string {
	perm := columnPermutation(res.Columns, dCols)
	out := make([][]string, len(res.Rows))
	for i := range res.Rows {
		cells := res.Cells(i)
		row := make([]string, len(perm))
		for j, src := range perm {
			if src < len(cells) {
				row[j] = canonCell(cells[src])
			}
		}
		out[i] = row
	}
	sortCanon(out)
	return out
}

// canonicalStringRows canonicalizes DuckDB CSV rows (already positional).
func canonicalStringRows(rows [][]string) [][]string {
	out := make([][]string, len(rows))
	for i, row := range rows {
		cells := make([]string, len(row))
		for j, s := range row {
			cells[j] = canonString(s)
		}
		out[i] = cells
	}
	sortCanon(out)
	return out
}

func sortCanon(rows [][]string) {
	sort.Slice(rows, func(a, b int) bool {
		ra, rb := rows[a], rows[b]
		for i := 0; i < len(ra) && i < len(rb); i++ {
			if ra[i] != rb[i] {
				return ra[i] < rb[i]
			}
		}
		return len(ra) < len(rb)
	})
}

var timestampRe = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(?:\.0+)?(?:Z|\+00:?00)?$`)

// canonCell canonicalizes a wadjet typed cell.
func canonCell(v any) string {
	switch x := v.(type) {
	case nil:
		return "<NULL>"
	case float64:
		return canonFloat(x)
	case float32:
		return canonFloat(float64(x))
	case int64:
		return strconv.FormatInt(x, 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int:
		return strconv.Itoa(x)
	case bool:
		return strconv.FormatBool(x)
	case string:
		return canonString(x)
	case []byte:
		return canonString(string(x))
	default:
		return canonString(fmt.Sprintf("%v", v))
	}
}

// canonString canonicalizes a cell already in string form (DuckDB CSV output,
// or wadjet string values): NULL marker, timestamp layout normalization, and
// numeric normalization so "1.50"/"1.5" and int64-vs-float printings agree.
func canonString(s string) string {
	if s == "<NULL>" {
		return "<NULL>"
	}
	// encoding/csv normalizes \r\n to \n inside quoted fields (RFC 4180),
	// so DuckDB's CSV output loses the \r that the raw data carries —
	// normalize the wadjet side identically.
	s = strings.ReplaceAll(s, "\r\n", "\n")
	if m := timestampRe.FindStringSubmatch(s); m != nil {
		return m[1] + " " + m[2]
	}
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return s // integer form is exact — keep verbatim
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return canonFloat(f)
	}
	return s
}

// canonFloat renders floats at 6 significant digits (the tpch convention) so
// ULP-level cross-engine drift doesn't produce false mismatches.
func canonFloat(f float64) string {
	if math.IsNaN(f) {
		return "<NaN>"
	}
	if math.IsInf(f, 1) {
		return "<+Inf>"
	}
	if math.IsInf(f, -1) {
		return "<-Inf>"
	}
	if f == math.Trunc(f) && math.Abs(f) < 1e15 {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', 6, 64)
}

func canonRowsEqual(a, b [][]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		}
		for j := range a[i] {
			if !canonCellEqual(a[i][j], b[i][j]) {
				return false
			}
		}
	}
	return true
}

// canonCellEqual is string equality with a float-tolerance fallback: two
// canonical cells that both parse as floats compare with relative epsilon.
func canonCellEqual(a, b string) bool {
	if a == b {
		return true
	}
	fa, errA := strconv.ParseFloat(a, 64)
	fb, errB := strconv.ParseFloat(b, 64)
	if errA != nil || errB != nil {
		return false
	}
	denom := math.Max(math.Abs(fa), math.Abs(fb))
	if denom == 0 {
		return fa == fb
	}
	return math.Abs(fa-fb)/denom < floatEps
}

func reportCanonDiff(t *testing.T, w, d [][]string) {
	t.Helper()
	if len(w) != len(d) {
		t.Logf("row count: wadjet=%d duckdb=%d", len(w), len(d))
	}
	shown := 0
	n := len(w)
	if len(d) < n {
		n = len(d)
	}
	for i := 0; i < n && shown < 5; i++ {
		if len(w[i]) != len(d[i]) {
			t.Logf("row %d width: wadjet=%d duckdb=%d", i, len(w[i]), len(d[i]))
			shown++
			continue
		}
		for j := range w[i] {
			if !canonCellEqual(w[i][j], d[i][j]) {
				t.Logf("row %d col %d: wadjet=%q duckdb=%q", i, j, w[i][j], d[i][j])
				shown++
				break
			}
		}
	}
	if shown == 0 && len(w) > 0 && len(d) > 0 {
		t.Logf("first wadjet row: %v", first(w))
		t.Logf("first duckdb row: %v", first(d))
	}
}

func first(rows [][]string) any {
	if len(rows) == 0 {
		return "<empty>"
	}
	return rows[0]
}

// runDuckDBCSV runs one query via the duckdb CLI and returns positional rows
// plus the header. NULL is encoded as the literal <NULL>.
func runDuckDBCSV(setup, sql string) ([][]string, []string, error) {
	script := setup + ".mode csv\n.headers on\n.nullvalue <NULL>\n" + sql + "\n"
	cmd := exec.Command(duckdbBin)
	cmd.Stdin = strings.NewReader(script)
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return nil, nil, fmt.Errorf("duckdb: %v: %s", err, ee.Stderr)
		}
		return nil, nil, fmt.Errorf("duckdb: %v", err)
	}
	r := csv.NewReader(strings.NewReader(string(out)))
	r.LazyQuotes = true
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("parse duckdb csv: %v", err)
	}
	if len(records) == 0 {
		return nil, nil, nil
	}
	return records[1:], records[0], nil
}

type clickbenchBaselineEntry struct {
	RowCount int    `json:"row_count"`
	Checksum string `json:"checksum"`
}

// stripLimit removes a trailing "LIMIT n [OFFSET m];" — see the comment at
// the call site: the correctness comparison runs full result sets.
var limitRe = regexp.MustCompile(`(?i)\s+LIMIT\s+\d+(\s+OFFSET\s+\d+)?\s*;?\s*$`)

func stripLimit(q string) string {
	return limitRe.ReplaceAllString(q, ";")
}

// columnPermutation returns, for each of DuckDB's column positions, the
// index of the wadjet cell that belongs there. It permutes by NAME only
// when the names can carry the mapping — every wadjet name unique and every
// DuckDB name present — which is the `SELECT *` case (wadjet emits those
// alphabetically, DuckDB in file order). Otherwise it is the identity: the
// two dialect files write the same select list, so position IS alignment,
// and a duplicate name means the names cannot align anything.
func columnPermutation(wCols, dCols []string) []int {
	identity := make([]int, len(wCols))
	for i := range identity {
		identity[i] = i
	}
	if len(dCols) != len(wCols) {
		return identity
	}
	at := make(map[string]int, len(wCols))
	for i, c := range wCols {
		if _, dup := at[c]; dup {
			return identity
		}
		at[c] = i
	}
	perm := make([]int, len(dCols))
	for j, dc := range dCols {
		i, ok := at[dc]
		if !ok {
			return identity
		}
		perm[j] = i
	}
	return perm
}

// checksumRowsUnordered hashes rows with cells sorted WITHIN each row, so
// the stored baseline is comparable regardless of engine column ordering
// (wadjet has no DuckDB header to align against in stored-baseline mode).
// The live compare remains strictly aligned; this mode is a drift gate.
func checksumRowsUnordered(rows [][]string) string {
	keys := make([]string, len(rows))
	for i, row := range rows {
		cells := append([]string(nil), row...)
		sort.Strings(cells)
		keys[i] = strings.Join(cells, "\x1f")
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func loadClickBenchBaseline(t *testing.T) map[int]clickbenchBaselineEntry {
	t.Helper()
	data, err := os.ReadFile(clickbenchBaseline)
	if err != nil {
		if os.IsNotExist(err) {
			return map[int]clickbenchBaselineEntry{}
		}
		t.Fatalf("read baseline: %v", err)
	}
	var raw map[string]clickbenchBaselineEntry
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("parse baseline: %v", err)
	}
	out := make(map[int]clickbenchBaselineEntry, len(raw))
	for k, v := range raw {
		n, err := strconv.Atoi(k)
		if err != nil {
			t.Fatalf("baseline key %q: %v", k, err)
		}
		out[n] = v
	}
	return out
}

func writeClickBenchBaseline(t *testing.T, b map[int]clickbenchBaselineEntry) {
	t.Helper()
	raw := make(map[string]clickbenchBaselineEntry, len(b))
	for k, v := range b {
		raw[strconv.Itoa(k)] = v
	}
	data, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		t.Fatalf("marshal baseline: %v", err)
	}
	if err := os.WriteFile(clickbenchBaseline, append(data, '\n'), 0o644); err != nil {
		t.Fatalf("write baseline: %v", err)
	}
}
