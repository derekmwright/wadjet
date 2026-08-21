package tpch

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// stampedEntry is a well-formed ground-truth entry the mutation tests bend.
func stampedEntry() FPEntry {
	return FPEntry{
		Engine:        "duckdb",
		EngineVersion: "v1.1.3",
		Dataset:       "s3://wadjet-bench-sf100-use2/tables/",
		CapturedAt:    "2026-08-19T00:00:00Z",
		Mode:          ModeOrderedTotal,
		Columns:       []string{"n_name", "revenue"},
		RowCount:      5,
		Fine:          "0123456789abcdef",
		Coarse:        "fedcba9876543210",
	}
}

func fileWith(kind string, e FPEntry) []byte {
	f := &FingerprintFile{
		Version: 1, Kind: kind, Scale: "SF100",
		Queries: map[string]FPEntry{"q05": e},
	}
	data, err := MarshalFingerprintFile(f)
	if err != nil {
		panic(err)
	}
	return data
}

// TestFingerprintLoaderRefusesUnstampedOrSelfProduced is the gate on the gate.
// An expectation that cannot be traced to an independent engine is the Q05
// failure mode in reverse: Wadjet's own answer frozen as truth, which a wrong
// engine passes and a corrected engine fails.
func TestFingerprintLoaderRefusesUnstampedOrSelfProduced(t *testing.T) {
	selfEntry := stampedEntry()
	selfEntry.Engine = SelfEngine

	unstamped := stampedEntry()
	unstamped.Engine = ""

	untraceable := stampedEntry()
	untraceable.EngineVersion = ""

	noDataset := stampedEntry()
	noDataset.Dataset = ""

	keyseqNoKeys := stampedEntry()
	keyseqNoKeys.Mode = ModeMultisetKeySeq

	keyseqNoDigest := stampedEntry()
	keyseqNoDigest.Mode = ModeMultisetKeySeq
	keyseqNoDigest.OrderKeys = []string{"revenue"}

	badMode := stampedEntry()
	badMode.Mode = "unordered"

	noDigest := stampedEntry()
	noDigest.Fine = ""

	noColumns := stampedEntry()
	noColumns.Columns = nil

	tests := []struct {
		name     string
		data     []byte
		wantKind string
		wantMsg  string
	}{
		{
			name:     "wadjet-produced entry in a ground-truth file",
			data:     fileWith(KindGroundTruth, selfEntry),
			wantKind: KindGroundTruth,
			wantMsg:  "NOT ground truth",
		},
		{
			name:     "unstamped entry",
			data:     fileWith(KindGroundTruth, unstamped),
			wantKind: KindGroundTruth,
			wantMsg:  "NOT ground truth",
		},
		{
			name:     "whole file mislabelled: a regression file loaded as truth",
			data:     fileWith(KindRegression, selfEntry),
			wantKind: KindGroundTruth,
			wantMsg:  "cannot establish correctness",
		},
		{
			name:     "reference engine with no version",
			data:     fileWith(KindGroundTruth, untraceable),
			wantKind: KindGroundTruth,
			wantMsg:  "not ground truth",
		},
		{
			name:     "reference engine with no dataset",
			data:     fileWith(KindGroundTruth, noDataset),
			wantKind: KindGroundTruth,
			wantMsg:  "not ground truth",
		},
		{
			name:     "ground-truth entry smuggled into a regression file",
			data:     fileWith(KindRegression, stampedEntry()),
			wantKind: KindRegression,
			wantMsg:  `want "wadjet"`,
		},
		{
			name:     "key-sequence mode with no keys",
			data:     fileWith(KindGroundTruth, keyseqNoKeys),
			wantKind: KindGroundTruth,
			wantMsg:  "no ORDER BY key digest",
		},
		{
			name:     "key-sequence mode with keys but no key digest",
			data:     fileWith(KindGroundTruth, keyseqNoDigest),
			wantKind: KindGroundTruth,
			wantMsg:  "no ORDER BY key digest",
		},
		{
			name:     "unknown comparison mode",
			data:     fileWith(KindGroundTruth, badMode),
			wantKind: KindGroundTruth,
			wantMsg:  "unknown mode",
		},
		{
			name:     "entry with no row digest",
			data:     fileWith(KindGroundTruth, noDigest),
			wantKind: KindGroundTruth,
			wantMsg:  "no row digest",
		},
		{
			name:     "entry with no columns",
			data:     fileWith(KindGroundTruth, noColumns),
			wantKind: KindGroundTruth,
			wantMsg:  "names no columns",
		},
		{
			name:     "unknown field (a hand-added readable value)",
			data:     []byte(`{"version":1,"kind":"ground-truth","queries":{},"sums":{"q05":123.45}}`),
			wantKind: KindGroundTruth,
			wantMsg:  "parse fingerprint file",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseFingerprintFile(tc.data, tc.wantKind)
			if err == nil {
				t.Fatalf("loader accepted %s", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("error %q does not mention %q", err, tc.wantMsg)
			}
		})
	}

	// The positive control: the same entry, properly stamped, loads.
	f, err := ParseFingerprintFile(fileWith(KindGroundTruth, stampedEntry()), KindGroundTruth)
	if err != nil {
		t.Fatalf("loader refused a properly stamped ground-truth entry: %v", err)
	}
	if len(f.Queries) != 1 {
		t.Fatalf("loaded %d entries, want 1", len(f.Queries))
	}
	// And a Wadjet self-fingerprint is legitimate in its own file.
	if _, err := ParseFingerprintFile(fileWith(KindRegression, selfEntry), KindRegression); err != nil {
		t.Fatalf("loader refused a self-fingerprint in a regression file: %v", err)
	}
}

func result(cols []string, rows ...map[string]any) *oracle.Result {
	return &oracle.Result{Columns: cols, Rows: rows}
}

// TestFingerprintDigestTracksValuesNotRendering is the other half of the
// contract: a changed VALUE must change the digest, while the same values
// re-rendered (integer-vs-float typing, trailing zeros, sub-quantum float
// noise) must not — otherwise the gate either misses corruption or flakes on
// two correct runs.
func TestFingerprintDigestTracksValuesNotRendering(t *testing.T) {
	cols := []string{"n_name", "revenue", "cnt"}
	base := result(cols,
		map[string]any{"n_name": "INDIA", "revenue": 12345.678901, "cnt": int64(7)},
		map[string]any{"n_name": "CHINA", "revenue": 999.5, "cnt": int64(3)},
	)
	q := FPQuery{Name: "q05", Mode: ModeOrderedTotal}
	entry := NewEntry("duckdb", "v1.1.3", "s3://bucket/tables/", "2026-08-19T00:00:00Z", q, SignatureOf(base, q))

	t.Run("same values re-rendered still match", func(t *testing.T) {
		// cnt as a float64 whole number (a reference engine may type a COUNT
		// as DOUBLE), revenue perturbed one ULP-ish below the fine quantum.
		reRendered := result(cols,
			map[string]any{"n_name": "INDIA", "revenue": 12345.678901000002, "cnt": float64(7)},
			map[string]any{"n_name": "CHINA", "revenue": 999.5000000000001, "cnt": float64(3)},
		)
		if ok, detail := entry.Match(SignatureOf(reRendered, q)); !ok {
			t.Fatalf("re-rendering the same values changed the digest: %s", detail)
		}
	})

	t.Run("a changed value fails", func(t *testing.T) {
		// The #312 shape at 1/1000 of its magnitude: one revenue inflated.
		wrong := result(cols,
			map[string]any{"n_name": "INDIA", "revenue": 12395.678901, "cnt": int64(7)},
			map[string]any{"n_name": "CHINA", "revenue": 999.5, "cnt": int64(3)},
		)
		if ok, _ := entry.Match(SignatureOf(wrong, q)); ok {
			t.Fatal("digest accepted a changed value")
		}
	})

	t.Run("a nulled string column fails", func(t *testing.T) {
		nulled := result(cols,
			map[string]any{"n_name": nil, "revenue": 12345.678901, "cnt": int64(7)},
			map[string]any{"n_name": "CHINA", "revenue": 999.5, "cnt": int64(3)},
		)
		if ok, _ := entry.Match(SignatureOf(nulled, q)); ok {
			t.Fatal("digest accepted a NULLed string column")
		}
	})

	t.Run("a dropped row fails", func(t *testing.T) {
		short := result(cols, base.Rows[0])
		if ok, _ := entry.Match(SignatureOf(short, q)); ok {
			t.Fatal("digest accepted a dropped row")
		}
	})

	t.Run("reordered rows fail under ordered-total", func(t *testing.T) {
		swapped := result(cols, base.Rows[1], base.Rows[0])
		if ok, _ := entry.Match(SignatureOf(swapped, q)); ok {
			t.Fatal("ordered-total digest accepted a result whose ORDER BY was dropped")
		}
	})

	t.Run("column set change fails", func(t *testing.T) {
		reordered := result([]string{"revenue", "n_name", "cnt"}, base.Rows...)
		if ok, _ := entry.Match(SignatureOf(reordered, q)); ok {
			t.Fatal("digest accepted a reordered SELECT list")
		}
	})
}

// TestKeySeqModeIsOrderSensitiveButTieImmune pins the fallback mode's
// contract: rows tied on the ORDER BY key may come back in either order, while
// a dropped or inverted ORDER BY still fails.
func TestKeySeqModeIsOrderSensitiveButTieImmune(t *testing.T) {
	cols := []string{"ps_partkey", "value"}
	q := FPQuery{Name: "q11", Mode: ModeMultisetKeySeq, OrderKeys: []string{"value"}}
	base := result(cols,
		map[string]any{"ps_partkey": int64(10), "value": 500.0},
		map[string]any{"ps_partkey": int64(20), "value": 500.0}, // tied with the row above
		map[string]any{"ps_partkey": int64(30), "value": 100.0},
	)
	entry := NewEntry("duckdb", "v1.1.3", "s3://bucket/tables/", "2026-08-19T00:00:00Z", q, SignatureOf(base, q))

	tiedSwap := result(cols, base.Rows[1], base.Rows[0], base.Rows[2])
	if ok, detail := entry.Match(SignatureOf(tiedSwap, q)); !ok {
		t.Fatalf("key-sequence mode failed on a legal tie reordering: %s", detail)
	}

	inverted := result(cols, base.Rows[2], base.Rows[0], base.Rows[1])
	if ok, _ := entry.Match(SignatureOf(inverted, q)); ok {
		t.Fatal("key-sequence mode accepted an inverted ORDER BY")
	}

	changed := result(cols,
		base.Rows[0],
		map[string]any{"ps_partkey": int64(20), "value": 501.0},
		base.Rows[2],
	)
	if ok, _ := entry.Match(SignatureOf(changed, q)); ok {
		t.Fatal("key-sequence mode accepted a changed value")
	}
}

// TestSignatureSnapsNearIntegers pins the remedy for the one instability the
// SF1 measurement found: a float sum that lands exactly on a whole number
// under one aggregation order and an ULP away under another renders as
// "48051445" versus "4.80514e+07" — a difference at EVERY precision, which is
// the one thing the dual-precision policy cannot absorb. It is q09's failure
// mode at SF1 (two of thirteen samples).
func TestSignatureSnapsNearIntegers(t *testing.T) {
	cols := []string{"nation", "o_year", "sum_profit"}
	q := FPQuery{Name: "q09", Mode: ModeOrderedTotal}
	row := func(v float64) *oracle.Result {
		return result(cols, map[string]any{"nation": "ALGERIA", "o_year": "1998", "sum_profit": v})
	}

	// The same answer, computed in two accumulation orders: one lands on the
	// whole number, the other is 7e-9 below it (1.5e-16 relative).
	onInteger := row(48051445)
	offByAnUlp := row(48051444.999999993)

	// The discontinuity is now fixed at its source — internal/oracle's
	// fingerprintFloat snaps near-integers (#377), with its own regression
	// test there — so SignatureOf no longer carries a local remedy and this
	// test keeps only the end-to-end property the gate depends on.

	entry := NewEntry("duckdb", "v1.4.1", "s3://bucket/tables/", "2026-08-19T00:00:00Z", q, SignatureOf(onInteger, q))
	if ok, detail := entry.Match(SignatureOf(offByAnUlp, q)); !ok {
		t.Fatalf("one ULP of accumulation noise around a whole number still breaks the digest: %s", detail)
	}

	// And the remedy must not swallow a real difference: 0.12 on 4.8e7 is
	// 2.5e-9 relative, an order of magnitude past the snap window.
	if ok, _ := entry.Match(SignatureOf(row(48051445.12), q)); ok {
		t.Fatal("snapping swallowed a value that is not the same number")
	}

	// Snapping must not mutate the caller's rows — the bench reuses them.
	if got := onInteger.Rows[0]["sum_profit"]; got != 48051445.0 {
		t.Fatalf("SignatureOf mutated its input: sum_profit is now %v", got)
	}
}

// TestCorrectnessVariants checks the variant set itself: every query is
// covered, every variant parses, every tiebreaker names a projected column,
// and the perf text is untouched.
func TestCorrectnessVariants(t *testing.T) {
	corpus := CorrectnessQueries(SF100)
	if len(corpus) != len(TPCHQueries) {
		t.Fatalf("corpus has %d queries, TPCHQueries has %d", len(corpus), len(TPCHQueries))
	}
	// Pin the projection helper itself: a helper that answers "yes" to
	// everything would make every check below vacuous.
	if projects(t, corpus[0].SQL, "no_such_column") {
		t.Fatal("projects() reports a column the query does not select")
	}
	for _, q := range corpus {
		t.Run(q.Name, func(t *testing.T) {
			if q.Mode != ModeOrderedTotal && q.Mode != ModeMultisetKeySeq {
				t.Fatalf("unknown mode %q", q.Mode)
			}
			if q.Why == "" {
				t.Error("variant records no rationale")
			}
			if _, err := plansql.Parse(q.SQL); err != nil {
				t.Fatalf("variant does not parse: %v\n%s", err, q.SQL)
			}
			perf := GetQuery(q.Num, SF100).SQL
			switch {
			case q.Tiebreak != "":
				if q.Mode != ModeOrderedTotal {
					t.Errorf("tiebreak %q on mode %s", q.Tiebreak, q.Mode)
				}
				if q.SQL == perf {
					t.Errorf("declares tiebreak %q but the SQL is unchanged", q.Tiebreak)
				}
				if !strings.Contains(q.SQL, ", "+q.Tiebreak) {
					t.Errorf("tiebreak %q not appended:\n%s", q.Tiebreak, q.SQL)
				}
				// The tiebreaker must already be projected — an ORDER BY on a
				// column the result does not carry is not verifiable from the
				// result, which is the whole point of appending it.
				if !projects(t, q.SQL, q.Tiebreak) {
					t.Errorf("tiebreak %q is not in the SELECT list", q.Tiebreak)
				}
			default:
				if q.SQL != perf {
					t.Errorf("no tiebreak declared but the SQL differs from the perf text")
				}
			}
			for _, k := range q.OrderKeys {
				if !projects(t, q.SQL, k) {
					t.Errorf("order key %q is not in the SELECT list", k)
				}
			}
		})
	}

	// The perf text the benchmark times must be the canonical text, whatever
	// the correctness variants do to their copies.
	for n := range TPCHQueries {
		if GetQuery(n, SF100).SQL != TPCHQueries[n].SQL && n != 11 {
			t.Errorf("q%02d: GetQuery mutated the canonical text", n)
		}
	}
}

// projects reports whether sql's top-level SELECT list yields a column named
// alias.
func projects(t *testing.T, sql, alias string) bool {
	t.Helper()
	pq, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if pq.SelectInfo == nil {
		t.Fatalf("statement carries no SELECT info")
	}
	for _, c := range pq.SelectInfo.Columns {
		for _, cand := range []string{c.Alias, c.ColumnRef, strings.TrimSpace(c.Expr)} {
			if i := strings.LastIndex(cand, "."); i >= 0 {
				cand = cand[i+1:]
			}
			if strings.EqualFold(cand, alias) {
				return true
			}
		}
	}
	return false
}

// TestGroundTruthSF100File pins the committed canonical file: it parses, it is
// stamped as ground truth, and — while unpopulated — it says so distinctly so
// a benchmark reports an INACTIVE gate rather than a pass.
func TestGroundTruthSF100File(t *testing.T) {
	f, err := GroundTruthSF100()
	switch {
	case errors.Is(err, ErrUnpopulated):
		t.Logf("SF100 value gate is not active yet: %v (generate it per docs/design/sf100-value-fingerprints.md)", err)
	case err != nil:
		t.Fatalf("committed fingerprint-sf100.json does not load: %v", err)
	default:
		if err := f.CheckCoversCorpus(CorrectnessQueries(SF100)); err != nil {
			t.Fatalf("fingerprint-sf100.json: %v", err)
		}
	}
	if f == nil || f.Kind != KindGroundTruth {
		t.Fatalf("committed file is not stamped %q", KindGroundTruth)
	}
}

// TestFingerprintGateAgainstDuckDBSF001 runs the SF100 apparatus at a scale CI
// can afford: the same correctness variants, the same order-sensitive
// comparison, against DuckDB fingerprints over the committed fixtures. It is
// the evidence that the SF100 gate will compare rather than flake — the
// tiebroken ORDER BYs, the column names, and the cross-engine cell rendering
// are all exercised here, where a failure costs seconds instead of a
// benchmark window.
func TestFingerprintGateAgainstDuckDBSF001(t *testing.T) {
	truth, err := GroundTruthSF001()
	if err != nil {
		t.Fatalf("SF0.01 ground truth: %v", err)
	}
	corpus := CorrectnessQueries(SF001)
	if err := truth.CheckCoversCorpus(corpus); err != nil {
		t.Fatalf("fingerprint-sf001.json: %v — regenerate with WADJET_FP_GENERATE=1 WADJET_FP_DATA=duckdb-data "+
			"WADJET_FP_SCALE=0.01 WADJET_FP_OUT=fingerprint-sf001.json", err)
	}

	ctx := context.Background()
	db := ingestDuckDBFixture(t, ctx, duckdbFixtureRows(t))
	for _, q := range corpus {
		t.Run(q.Name, func(t *testing.T) {
			res, err := db.Query(ctx, q.SQL)
			if err != nil {
				t.Fatalf("query failed: %v\n%s", err, q.SQL)
			}
			sig := SignatureOf(&oracle.Result{Columns: res.Columns, Rows: res.Rows}, q)
			want := truth.Queries[q.Name]
			if ok, detail := want.Match(sig); !ok {
				t.Errorf("diverges from %s %s over %s: %s", want.Engine, want.EngineVersion, want.Dataset, detail)
			}
		})
	}
}

// TestGroundTruthFileCarriesNoValues enforces the constraint the file exists
// under: no sums, no min/max, no sample rows. Row counts and column names are
// structural and allowed; everything else must be a digest or provenance.
func TestGroundTruthFileCarriesNoValues(t *testing.T) {
	allowed := map[string]bool{
		"engine": true, "engine_version": true, "dataset": true, "captured_at": true,
		"mode": true, "tiebreak": true, "order_keys": true,
		"columns": true, "row_count": true,
		"fine": true, "coarse": true, "key_fine": true, "key_coarse": true,
		"why": true,
	}
	for file, blob := range map[string][]byte{
		"fingerprint-sf100.json": groundTruthSF100JSON,
		"fingerprint-sf001.json": groundTruthSF001JSON,
	} {
		var raw struct {
			Queries map[string]map[string]json.RawMessage `json:"queries"`
		}
		if err := json.Unmarshal(blob, &raw); err != nil {
			t.Fatalf("unmarshal %s: %v", file, err)
		}
		for name, entry := range raw.Queries {
			for field := range entry {
				if !allowed[field] {
					t.Errorf("%s entry %s carries field %q, which is not a digest, a row count, a column name, or provenance",
						file, name, field)
				}
			}
		}
	}
}
