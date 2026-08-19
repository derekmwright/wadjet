package tpch

// SF100 value-level correctness gate.
//
// benchmarks/tpch/baseline-sf100.json gates ROW COUNTS. Every wrong-value bug
// this project found in 2026-08 passes such a gate: a dropped join predicate
// inflated Q05 revenues ~25x with an identical row count (#312), MEDIAN
// returned SUM on the DAG, STDDEV was computed over one parallel clone's rows.
// This file adds the missing arm — a digest of the VALUES — at SF100.
//
// Two properties drive the design.
//
//  1. The stored file must carry no readable result values. A committed answer
//     key invites implementing toward it, and invites a red gate being "fixed"
//     by editing the expectation. So the stored form is oracle.Fingerprint: a
//     row count plus truncated SHA-256 digests of the canonical row rendering
//     at 6 and 4 significant float digits. Row counts and column NAMES stay
//     (structural, already externally validated against Trino 470, and a digest
//     with no size is not a gate); everything else is a digest. The harness's
//     ValueSig (per-column numeric SUM) is deliberately NOT stored here — it is
//     a readable value.
//
//  2. Every fingerprint is ORDER-SENSITIVE. An unordered digest sorts rows
//     before hashing, so a result that lost its ORDER BY passes it; that is the
//     #320 shape. But a positional digest over a query whose ORDER BY is not a
//     TOTAL order is flaky rather than strong — SQL leaves the order of tied
//     rows unspecified, so two correct engines may differ. Each query therefore
//     declares one of two order-sensitive modes, and the stored entry records
//     WHICH, so a reader can see what a passing digest proves:
//
//     ModeOrderedTotal — the ORDER BY is a total order (after appending a
//     deterministic tiebreaker where the canonical text leaves one open) and
//     every key is projected, so the rows are digested positionally.
//
//     ModeMultisetKeySeq — the order is not made total; the digest covers the
//     row MULTISET plus the positional sequence of the ORDER BY KEY values.
//     Tied rows carry equal keys, so tie order may vary freely, while a
//     dropped or inverted ORDER BY still changes the key sequence.
//     internal/oracle/compare.go applies the same scheme to generated
//     queries.
//
// Correctness and performance run DIFFERENT VARIANTS of the same query. The
// canonical TPC-H text in TPCHQueries is what the benchmark times, untouched.
// CorrectnessQueries returns the variant the gate digests: for five queries it
// appends a tiebreaker column (already projected) to the trailing ORDER BY so
// the promised order is actually verifiable. The tiebreaker is named in the
// stored entry.

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"regexp"
	"sort"
	"strings"

	"github.com/derekmwright/wadjet/internal/oracle"
)

// Comparison modes. Both are order-sensitive; they differ in how they stay
// immune to the order of TIED rows, which SQL does not pin down.
const (
	ModeOrderedTotal   = "ordered-total"
	ModeMultisetKeySeq = "multiset+keyseq"
)

// File kinds. The distinction is the whole point of the provenance stamp: a
// fingerprint produced by Wadjet is regression detection, never correctness.
const (
	// KindGroundTruth: every entry came from an external reference engine.
	// This file can fail Wadjet for being wrong.
	KindGroundTruth = "ground-truth"
	// KindRegression: every entry came from Wadjet itself. This file can
	// only say "the answer changed", never "the answer is right".
	KindRegression = "regression"
)

// SelfEngine stamps a Wadjet-produced entry. It is refused in a ground-truth
// file by name.
const SelfEngine = "wadjet"

// referenceEngines are the engines whose output may stand as ground truth: an
// independent implementation of TPC-H reading the same parquet. DuckDB is the
// repo's existing reference (benchmarks/tpch/duckdb-setup/); Trino 470 already
// produced the externally-validated SF100 row counts in baseline-sf100.json.
var referenceEngines = map[string]bool{"duckdb": true, "trino": true}

// ErrUnpopulated reports a well-formed file that carries no entries yet — the
// SF100 gate is not active. Callers distinguish it from a malformed file so a
// benchmark run says "gate not active, generate it" rather than "corrupt".
var ErrUnpopulated = errors.New("fingerprint file has no entries")

//go:embed fingerprint-sf100.json
var groundTruthSF100JSON []byte

//go:embed fingerprint-sf001.json
var groundTruthSF001JSON []byte

// FingerprintFile is the on-disk shape: a provenance header plus one entry per
// correctness query.
type FingerprintFile struct {
	Version   int                `json:"version"`
	Kind      string             `json:"kind"`
	Scale     string             `json:"scale"`
	Generator string             `json:"generator"`
	Note      string             `json:"note"`
	Queries   map[string]FPEntry `json:"queries"`
}

// FPEntry is one query's stored signature. It carries no result values: the
// row count and column names are structural, everything else is a digest.
type FPEntry struct {
	// Engine names what produced this entry. A ground-truth file requires a
	// reference engine here; "wadjet" is refused by name.
	Engine        string `json:"engine"`
	EngineVersion string `json:"engine_version"`
	// Dataset names the bytes the engine read, so an entry can be traced to
	// the data the benchmark reads (they must be the same parquet).
	Dataset    string `json:"dataset"`
	CapturedAt string `json:"captured_at"`

	Mode string `json:"mode"`
	// Tiebreak names the columns appended to the query's ORDER BY to make it
	// total (ModeOrderedTotal only). Empty when the canonical order was
	// already total.
	Tiebreak string `json:"tiebreak,omitempty"`
	// OrderKeys are the projected ORDER BY key columns whose SEQUENCE is
	// digested (ModeMultisetKeySeq only).
	OrderKeys []string `json:"order_keys,omitempty"`

	Columns  []string `json:"columns"`
	RowCount int      `json:"row_count"`
	// Fine/Coarse digest the rows at 6 and 4 significant float digits —
	// positionally under ModeOrderedTotal, as a multiset under
	// ModeMultisetKeySeq.
	Fine   string `json:"fine"`
	Coarse string `json:"coarse"`
	// KeyFine/KeyCoarse digest the ORDER BY key sequence
	// (ModeMultisetKeySeq only).
	KeyFine   string `json:"key_fine,omitempty"`
	KeyCoarse string `json:"key_coarse,omitempty"`

	Why string `json:"why,omitempty"`
}

// Signature is a computed (not yet stored) result signature.
type Signature struct {
	Mode string
	// Rows is the row-content fingerprint; Keys is the ORDER BY key-sequence
	// fingerprint, populated only under ModeMultisetKeySeq.
	Rows oracle.Fingerprint
	Keys oracle.Fingerprint
	// Columns is the result's column list, in order.
	Columns []string
}

func (s Signature) String() string {
	if s.Mode == ModeMultisetKeySeq {
		return fmt.Sprintf("%s rows=%d %s/%s keys=%s/%s", s.Mode, s.Rows.Rows,
			s.Rows.Fine, s.Rows.Coarse, s.Keys.Fine, s.Keys.Coarse)
	}
	return fmt.Sprintf("%s rows=%d %s/%s", s.Mode, s.Rows.Rows, s.Rows.Fine, s.Rows.Coarse)
}

// FPQuery is one correctness variant: the SQL the gate runs (which is NOT the
// SQL the benchmark times) plus how its answer is compared.
type FPQuery struct {
	Num  int
	Name string // "q03"
	SQL  string
	Mode string
	// Tiebreak lists the columns appended to the trailing ORDER BY, "" when
	// the canonical order was already total.
	Tiebreak string
	// OrderKeys are the ORDER BY key columns digested positionally under
	// ModeMultisetKeySeq.
	OrderKeys []string
	Why       string
}

// fpVariant declares one query's deviation from "canonical text, already a
// total order". A query absent from this table needs neither.
type fpVariant struct {
	tiebreak  string
	mode      string
	orderKeys []string
	why       string
}

// fpVariants: the per-query correctness decisions.
//
// The default for every TPC-H query is ModeOrderedTotal with no tiebreak,
// because most of the 22 already order by a key set that includes a unique key
// of the result (the GROUP BY key list, or s_name / p_partkey which are unique
// in the generated data). The five entries below are the ones whose canonical
// ORDER BY leaves ties possible; each appends a column the query ALREADY
// PROJECTS, so the promised order becomes verifiable without changing what the
// query means.
var fpVariants = map[int]fpVariant{
	3: {
		tiebreak: "l_orderkey",
		why:      "ORDER BY revenue DESC, o_orderdate ties when two orders shipped the same day for equal revenue; l_orderkey is the GROUP BY key and is projected",
	},
	5: {
		tiebreak: "n_name",
		why:      "ORDER BY revenue DESC alone; n_name is the GROUP BY key and is projected",
	},
	10: {
		tiebreak: "c_custkey",
		why:      "ORDER BY revenue DESC alone; c_custkey is the GROUP BY key and is projected",
	},
	11: {
		tiebreak: "ps_partkey",
		why:      "ORDER BY value DESC alone over ~93k rows at SF100; ps_partkey is the GROUP BY key and is projected",
	},
	18: {
		tiebreak: "o_orderkey",
		why:      "ORDER BY o_totalprice DESC, o_orderdate ties when one customer's orders share a total; o_orderkey is the GROUP BY key and is projected",
	},
}

// QueryName renders the corpus name for a query number.
func QueryName(n int) string { return fmt.Sprintf("q%02d", n) }

// CorrectnessQueries returns the 22 correctness variants for scale factor sf,
// in query order. These are NOT the queries the benchmark times: GetQuery
// returns that text unmodified, and this function may append a tiebreaker to
// its trailing ORDER BY. Both read the same tables and mean the same thing;
// only the order of tied rows differs, which is exactly what SQL leaves open
// and what a positional digest cannot be allowed to guess at.
func CorrectnessQueries(sf ScaleFactor) []FPQuery {
	nums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		nums = append(nums, n)
	}
	sort.Ints(nums)

	out := make([]FPQuery, 0, len(nums))
	for _, n := range nums {
		def := GetQuery(n, sf)
		v := fpVariants[n]
		q := FPQuery{
			Num:       n,
			Name:      QueryName(n),
			SQL:       def.SQL,
			Mode:      ModeOrderedTotal,
			Tiebreak:  v.tiebreak,
			OrderKeys: v.orderKeys,
			Why:       v.why,
		}
		if v.mode != "" {
			q.Mode = v.mode
		}
		if q.Mode == ModeOrderedTotal && v.tiebreak != "" {
			sql, err := appendOrderTiebreak(def.SQL, v.tiebreak)
			if err != nil {
				// Only reachable if a query's text stops ending in an ORDER
				// BY clause, which the variant table is written against;
				// TestCorrectnessVariants pins it.
				panic(fmt.Sprintf("tpch: correctness variant for q%02d: %v", n, err))
			}
			q.SQL = sql
		}
		if q.Why == "" {
			q.Why = defaultWhy(def.SQL)
		}
		out = append(out, q)
	}
	return out
}

func defaultWhy(sql string) string {
	if !hasTrailingOrderBy(sql) {
		return "single-row result, no ORDER BY: the sequence is trivially total"
	}
	return "canonical ORDER BY already includes a unique key of the result"
}

var fpTrailingLimitRe = regexp.MustCompile(`(?is)\s+LIMIT\s+\d+\s*;?\s*$`)

// appendOrderTiebreak adds extra to the end of the statement's trailing ORDER
// BY, ahead of any trailing LIMIT. It refuses a statement that does not end in
// an ORDER BY clause rather than producing SQL that parses but sorts by
// something else.
func appendOrderTiebreak(sql, extra string) (string, error) {
	body, limit := sql, ""
	if m := fpTrailingLimitRe.FindString(sql); m != "" {
		body, limit = sql[:len(sql)-len(m)], m
	}
	if !hasTrailingOrderBy(body) {
		return "", fmt.Errorf("statement does not end in an ORDER BY clause, cannot append tiebreaker %q", extra)
	}
	return strings.TrimRight(body, " \t\n") + ", " + extra + limit, nil
}

// hasTrailingOrderBy reports whether sql's last clause is an ORDER BY at the
// top level. A subquery's ORDER BY is always followed by its closing paren, so
// "no ')' after the last ORDER BY" is the test.
func hasTrailingOrderBy(sql string) bool {
	i := strings.LastIndex(strings.ToUpper(sql), "ORDER BY")
	if i < 0 {
		return false
	}
	return !strings.ContainsAny(sql[i:], ")")
}

// snapEpsilon is how close to a whole number a float cell must sit before a
// signature treats it as that whole number. It sits midway on a log scale
// between the worst accumulation noise measured at SF1 (2.9e-12) and the fine
// digest's own quantum (1e-6) — three orders of margin on each side — so it
// separates "the same answer computed in a different order" from "a different
// answer" with room to spare.
const snapEpsilon = 1e-9

// snapNearIntegers returns res with every float cell within snapEpsilon
// (relative) of a whole number replaced by that whole number.
//
// It exists because the cell rendering has one DISCONTINUITY: an exact integer
// renders as its full digits ("48051445") while every other value quantizes
// ("4.80514e+07"). That branch is needed — a large integer SUM that one engine
// reports as text and the other as float64 has to agree, and large keys must
// stay distinguishable — but it means two values ONE ULP apart can render
// differently at EVERY precision, which is the one thing the dual-precision
// policy cannot absorb.
//
// This is not hypothetical: at SF1, q09's sum_profit lands exactly on a whole
// number under one aggregation order and ~1e-15 away under another, and both
// digests moved with it (docs/design/sf100-value-fingerprints.md). Snapping
// removes the discontinuity from both sides — the reference engine's answer is
// digested through this same function — and cannot mask a real error, since
// the fine quantum already absorbs a thousand times more.
func snapNearIntegers(res *oracle.Result) *oracle.Result {
	out := &oracle.Result{Columns: res.Columns, Rows: make([]map[string]any, len(res.Rows))}
	for i, row := range res.Rows {
		// Copy on first change only: the caller's rows are never mutated,
		// and a result with nothing to snap costs no allocation.
		target, copied := row, false
		for _, col := range res.Columns {
			f, ok := row[col].(float64)
			if !ok {
				continue
			}
			snapped, changed := snapFloat(f)
			if !changed {
				continue
			}
			if !copied {
				target, copied = maps.Clone(row), true
			}
			target[col] = snapped
		}
		out.Rows[i] = target
	}
	return out
}

func snapFloat(f float64) (float64, bool) {
	if f == 0 || math.IsNaN(f) || math.IsInf(f, 0) || math.Abs(f) >= 1<<53 {
		return f, false
	}
	r := math.Round(f)
	if r == f || r == 0 {
		return f, false
	}
	if math.Abs(f-r) <= snapEpsilon*math.Abs(f) {
		return r, true
	}
	return f, false
}

// SignatureOf digests one result under q's comparison mode.
func SignatureOf(res *oracle.Result, q FPQuery) Signature {
	res = snapNearIntegers(res)
	sig := Signature{Mode: q.Mode, Columns: append([]string(nil), res.Columns...)}
	switch q.Mode {
	case ModeMultisetKeySeq:
		sig.Rows = oracle.FingerprintOf(res, false)
		sig.Keys = oracle.FingerprintOf(&oracle.Result{Columns: q.OrderKeys, Rows: res.Rows}, true)
	default:
		sig.Rows = oracle.FingerprintOf(res, true)
	}
	return sig
}

// Match reports whether got is the same answer as the stored entry. The detail
// string names the first property that differs.
func (e FPEntry) Match(got Signature) (bool, string) {
	if e.Mode != got.Mode {
		return false, fmt.Sprintf("compared as %s but the stored entry is %s", got.Mode, e.Mode)
	}
	if cols := strings.Join(got.Columns, ","); len(e.Columns) > 0 && cols != strings.Join(e.Columns, ",") {
		return false, fmt.Sprintf("columns %s, want %s", cols, strings.Join(e.Columns, ","))
	}
	if ok, detail := e.rowFingerprint().Match(got.Rows); !ok {
		return false, detail
	}
	if e.Mode == ModeMultisetKeySeq {
		if ok, detail := e.keyFingerprint().Match(got.Keys); !ok {
			return false, "ORDER BY key sequence differs (row multiset matches, so this is an ordering divergence): " + detail
		}
	}
	return true, ""
}

func (e FPEntry) rowFingerprint() oracle.Fingerprint {
	return oracle.Fingerprint{Rows: e.RowCount, Fine: e.Fine, Coarse: e.Coarse}
}

func (e FPEntry) keyFingerprint() oracle.Fingerprint {
	return oracle.Fingerprint{Rows: e.RowCount, Fine: e.KeyFine, Coarse: e.KeyCoarse}
}

// NewEntry stamps a computed signature into a storable entry. engine names
// what produced it — a reference engine for a ground-truth file, SelfEngine
// for a regression file. Nothing else in the repo writes an entry, so an
// unstamped or mis-stamped entry means a hand edit, which ParseFingerprintFile
// refuses.
func NewEntry(engine, version, dataset, capturedAt string, q FPQuery, sig Signature) FPEntry {
	e := FPEntry{
		Engine:        engine,
		EngineVersion: version,
		Dataset:       dataset,
		CapturedAt:    capturedAt,
		Mode:          sig.Mode,
		Tiebreak:      q.Tiebreak,
		OrderKeys:     q.OrderKeys,
		Columns:       append([]string(nil), sig.Columns...),
		RowCount:      sig.Rows.Rows,
		Fine:          sig.Rows.Fine,
		Coarse:        sig.Rows.Coarse,
		Why:           q.Why,
	}
	if sig.Mode == ModeMultisetKeySeq {
		e.KeyFine, e.KeyCoarse = sig.Keys.Fine, sig.Keys.Coarse
	}
	return e
}

// GroundTruthSF100 returns the committed SF100 ground-truth file, embedded so
// a benchmark binary carries it (the SF100 run happens on an EC2 host with no
// checkout). It returns ErrUnpopulated while the file has no entries — the
// gate is then inactive and the caller must say so out loud rather than
// reporting a pass.
func GroundTruthSF100() (*FingerprintFile, error) {
	return ParseFingerprintFile(groundTruthSF100JSON, KindGroundTruth)
}

// GroundTruthSF001 returns the SF0.01 sibling: the same correctness variants
// captured from DuckDB over the committed benchmarks/tpch/duckdb-data
// fixtures. It exists so this apparatus is exercised at a scale CI can run —
// the ordered-total comparison, the tiebreakers, and the cross-engine cell
// rendering are all proven there rather than first meeting reality on a 280
// GiB bucket.
func GroundTruthSF001() (*FingerprintFile, error) {
	return ParseFingerprintFile(groundTruthSF001JSON, KindGroundTruth)
}

// ParseFingerprintFile parses and validates a fingerprint file.
//
// The provenance check is the loud one. An entry that cannot be traced to an
// external reference engine is the failure mode this whole file exists to
// prevent: Wadjet's own answer frozen as the expectation, against which a
// CORRECT engine fails and a WRONG engine passes. A Wadjet-produced file is
// legitimate — as regression detection — but it must say so in its kind, and a
// ground-truth load refuses it by name.
func ParseFingerprintFile(data []byte, wantKind string) (*FingerprintFile, error) {
	var f FingerprintFile
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("parse fingerprint file: %w", err)
	}
	if f.Kind != wantKind {
		return nil, fmt.Errorf("fingerprint file is kind %q, want %q — a %s file records what Wadjet answered, "+
			"which detects regressions but cannot establish correctness", f.Kind, wantKind, KindRegression)
	}
	if err := validateEntries(&f); err != nil {
		return nil, err
	}
	if len(f.Queries) == 0 {
		return &f, ErrUnpopulated
	}
	return &f, nil
}

func validateEntries(f *FingerprintFile) error {
	names := make([]string, 0, len(f.Queries))
	for name := range f.Queries {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		e := f.Queries[name]
		if f.Kind == KindGroundTruth {
			if e.Engine == SelfEngine {
				return fmt.Errorf("entry %s was produced by %q — this file is NOT ground truth. "+
					"A Wadjet fingerprint detects that an answer CHANGED; only an independent engine over the same "+
					"parquet can say the answer is RIGHT. Keep self-fingerprints in a %q file",
					name, e.Engine, KindRegression)
			}
			if !referenceEngines[e.Engine] {
				return fmt.Errorf("entry %s has engine %q, want one of the reference engines (duckdb, trino) — "+
					"this file is NOT ground truth; regenerate it, never hand-write an entry", name, e.Engine)
			}
			if e.EngineVersion == "" || e.Dataset == "" {
				return fmt.Errorf("entry %s names engine %q but not its version and dataset — an expectation that "+
					"cannot be traced back to the bytes it was read from is not ground truth", name, e.Engine)
			}
		} else if e.Engine != SelfEngine {
			return fmt.Errorf("entry %s in a %q file has engine %q, want %q", name, f.Kind, e.Engine, SelfEngine)
		}
		switch e.Mode {
		case ModeOrderedTotal:
		case ModeMultisetKeySeq:
			if e.KeyFine == "" || e.KeyCoarse == "" {
				return fmt.Errorf("entry %s compares %s but carries no ORDER BY key digest", name, e.Mode)
			}
			if len(e.OrderKeys) == 0 {
				return fmt.Errorf("entry %s compares %s but names no ORDER BY keys", name, e.Mode)
			}
		default:
			return fmt.Errorf("entry %s has unknown mode %q", name, e.Mode)
		}
		if e.Fine == "" || e.Coarse == "" {
			return fmt.Errorf("entry %s carries no row digest", name)
		}
		if len(e.Columns) == 0 {
			return fmt.Errorf("entry %s names no columns", name)
		}
		if e.RowCount < 0 {
			return fmt.Errorf("entry %s has row_count %d", name, e.RowCount)
		}
	}
	return nil
}

// CheckCoversCorpus requires the file and the corpus to name the same queries
// and each entry to carry the mode and tiebreaker its variant declares. A
// corpus query with no entry would pass unchecked; a stale entry hides that a
// variant changed under it, which would make the digest describe SQL nobody
// runs any more.
func (f *FingerprintFile) CheckCoversCorpus(corpus []FPQuery) error {
	seen := make(map[string]bool, len(corpus))
	for _, q := range corpus {
		seen[q.Name] = true
		e, ok := f.Queries[q.Name]
		if !ok {
			return fmt.Errorf("no entry for %s", q.Name)
		}
		if e.Mode != q.Mode {
			return fmt.Errorf("entry %s stored as %q but the variant declares %q — regenerate", q.Name, e.Mode, q.Mode)
		}
		if e.Tiebreak != q.Tiebreak {
			return fmt.Errorf("entry %s stored tiebreak %q but the variant appends %q — regenerate", q.Name, e.Tiebreak, q.Tiebreak)
		}
		if strings.Join(e.OrderKeys, ",") != strings.Join(q.OrderKeys, ",") {
			return fmt.Errorf("entry %s stored order keys %v but the variant declares %v — regenerate", q.Name, e.OrderKeys, q.OrderKeys)
		}
	}
	for name := range f.Queries {
		if !seen[name] {
			return fmt.Errorf("stale entry %s that no corpus query claims", name)
		}
	}
	return nil
}

// MarshalFingerprintFile renders f for storage.
func MarshalFingerprintFile(f *FingerprintFile) ([]byte, error) {
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}
