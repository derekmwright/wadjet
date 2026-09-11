package oracle

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// Fingerprint stores row count and canonical-row digests for offline references.
// Cover EVERY column, strings and NULLs; NULL is distinct from empty (#314).
// Caller chooses ordering per query: ordered preserves sequence, unordered sorts
// rows before digesting. Wrong selection invents failures or hides dropped
// ORDER BY (#313/#316/#320); follow the query's determinism boundary.
// Match either fine or coarse float precision, as Canon.Diff does, to tolerate
// small accumulation noise. This is an approximate comparison policy.
// See docs/internals/oracle-stored-result-fingerprints.md for the design.
type Fingerprint struct {
	// Rows is the row count; it is part of the identity, so a fingerprint
	// never matches a result of a different size.
	Rows int `json:"row_count"`
	// Fine and Coarse are the digests at 6 and 4 significant float digits.
	Fine   string `json:"fine"`
	Coarse string `json:"coarse"`
}

// FingerprintOf digests res. ordered keeps the engine's row sequence in the
// digest; with it false the rendered rows are sorted first, so the digest is
// order-insensitive.
//
// Cells render through fingerprintCell rather than canonCell: this digest is
// compared ACROSS engines, where integer-vs-float column typing is the
// reference engine's business and must not read as a divergence.
func FingerprintOf(res *Result, ordered bool) Fingerprint {
	c := canonRowsWith(res, fingerprintCell)
	if !ordered {
		sort.Strings(c.rows)
		sort.Strings(c.coarseRows)
	}
	return Fingerprint{
		Rows:   len(c.rows),
		Fine:   digestRows(c.rows),
		Coarse: digestRows(c.coarseRows),
	}
}

// Match reports whether got is the same answer as f: identical row counts
// and agreement at either precision. The detail string names the first
// property that differs.
func (f Fingerprint) Match(got Fingerprint) (bool, string) {
	if f.Rows != got.Rows {
		return false, fmt.Sprintf("row count %d, want %d", got.Rows, f.Rows)
	}
	if f.Fine == got.Fine || f.Coarse == got.Coarse {
		return true, ""
	}
	return false, fmt.Sprintf("content digest %s/%s, want %s/%s (fine/coarse, %d rows either way)",
		got.Fine, got.Coarse, f.Fine, f.Coarse, f.Rows)
}

func (f Fingerprint) String() string {
	return fmt.Sprintf("%d rows %s/%s", f.Rows, f.Fine, f.Coarse)
}

func digestRows(rows []string) string {
	h := sha256.New()
	for _, r := range rows {
		h.Write([]byte(r))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// fingerprintCell is canonCell's cross-engine sibling: same dual-precision
// float policy, except a number renders the same whether the engine typed
// it as an integer or a float. A value that is an exact integer renders as
// its full digits — never quantized, so two large keys that differ in the
// last digit stay distinguishable — and everything else quantizes to the
// requested significant digits.
func fingerprintCell(v any) (fine, rough string) {
	switch tv := v.(type) {
	case nil:
		return "<null>", "<null>"
	case float64:
		return fingerprintFloat(tv, 6), fingerprintFloat(tv, 4)
	case float32:
		return fingerprintFloat(float64(tv), 6), fingerprintFloat(float64(tv), 4)
	case int:
		s := strconv.Itoa(tv)
		return s, s
	case int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		s := fmt.Sprint(tv)
		return s, s
	case []byte:
		s := string(tv)
		return s, s
	case string:
		s := canonicalDecimalCell(tv)
		return s, s
	default:
		s := fmt.Sprint(tv)
		return s, s
	}
}

// snapEpsilon is how close to a whole number a float must sit (relative)
// before fingerprintFloat treats it as that whole number. It sits midway on
// a log scale between the worst accumulation noise measured at SF1
// (2.9e-12) and the fine digest's own quantum (1e-6) — three orders of
// margin on each side. Ported unchanged from
// benchmarks/tpch/fingerprint.go's snapEpsilon/snapFloat (#377): that file
// proved the remedy for its SF100 signatures first and scoped it there to
// avoid blast radius on the then-175 committed SF0.01 baselines; this port
// closes the same hole in the SF0.01 gate every one of those baselines
// runs through.
const snapEpsilon = 1e-9

// snapNearInteger returns f rounded to the nearest whole number when f sits
// within snapEpsilon (relative) of one, and f unchanged otherwise.
//
// fingerprintFloat's exact-integer branch is a DISCONTINUITY: an exact
// integer renders as its full digits ("48051445") while every other value
// quantizes ("4.80514e+07"), so two floats one ULP apart — the same answer,
// computed in a different accumulation order, straddling a whole number —
// can render differently at EVERY precision, which is the one thing the
// dual-precision policy (match at 6 OR 4 significant digits) cannot absorb.
// Snapping first removes the discontinuity from both sides — the reference
// engine's answer renders through this same function — and cannot mask a
// real error, since the fine quantum already absorbs a thousand times more.
func snapNearInteger(f float64) float64 {
	if f == 0 || math.IsNaN(f) || math.IsInf(f, 0) || math.Abs(f) >= 1<<53 {
		return f
	}
	r := math.Round(f)
	if r == f || r == 0 {
		return f
	}
	if math.Abs(f-r) <= snapEpsilon*math.Abs(f) {
		return r
	}
	return f
}

// fingerprintFloat renders an exact integer as its digits (so a SUM one
// engine reports as BIGINT and the other as DOUBLE agree) and quantizes
// everything else to prec significant digits. A value within snapEpsilon of
// a whole number is snapped to it first (#377), so accumulation-order noise
// straddling an integer boundary cannot flip which branch a cell takes.
func fingerprintFloat(f float64, prec int) string {
	f = snapNearInteger(f)
	if f == math.Trunc(f) && math.Abs(f) < 1<<53 {
		return strconv.FormatInt(int64(f), 10) // also collapses -0
	}
	return canonFloat(f, prec)
}

// TextCell types one cell of a reference engine's TEXT output (a CSV field,
// say) for FingerprintOf. Only a value carrying a fraction or an exponent
// becomes a float — that is where quantization is needed and where the two
// engines' last digits legitimately differ. Integer-looking text stays text,
// which is exactly how fingerprintCell renders an integer, so "1234567"
// digests identically whether it arrived as text, as an int64, or as a
// float64; and a string that merely looks integral ("007", a country code)
// keeps its own spelling.
//
// null is the reference engine's spelling of NULL; text equal to it becomes
// a nil cell. An empty field is the empty STRING, never NULL — conflating
// the two is how a NULLed column reads as blank and passes.
func TextCell(s, null string) any {
	if s == null {
		return nil
	}
	if !strings.ContainsAny(s, ".eE") {
		return s
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return s
	}
	// ParseFloat also accepts "inf"/"nan"/hex floats; require the text to
	// read as a plain decimal number before treating it as one.
	if strings.IndexFunc(s, func(r rune) bool {
		return !strings.ContainsRune("0123456789+-.eE", r)
	}) >= 0 {
		return s
	}
	return f
}

// canonicalDecimalCell removes trailing FRACTION zeros and then an empty point,
// so 100.0 and 100 hash alike across engine text conventions (#555).
// Trim text, never round through float64: preserve every significant digit
// of wide DECIMAL values (#455).
// This also equates genuine TEXT '1.50' and '1.5'; TextCell already collapses
// those reference-side values, so the fingerprint does not distinguish them.
// See docs/internals/oracle-decimal-text-canonicalization.md for the design.
func canonicalDecimalCell(s string) string {
	dot := strings.IndexByte(s, '.')
	if dot < 0 {
		return s
	}
	sign := 0
	if s[0] == '-' || s[0] == '+' {
		sign = 1
	}
	body := s[sign:]
	dot -= sign
	if dot <= 0 || dot >= len(body)-1 {
		return s
	}
	for i := 0; i < len(body); i++ {
		if i == dot {
			continue
		}
		if body[i] < '0' || body[i] > '9' {
			return s // not plain decimal text: an exponent, a date, an address
		}
	}
	trimmed := strings.TrimRight(body[dot+1:], "0")
	if trimmed == "" {
		return s[:sign] + body[:dot]
	}
	return s[:sign] + body[:dot] + "." + trimmed
}
