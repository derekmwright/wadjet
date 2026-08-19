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

// Fingerprint is a stored-comparable digest of a whole result: the row
// count plus one digest per precision of the canonical row rendering. It
// exists so a reference engine's answer can be committed to a file and
// compared later without that engine on the machine — the shape the
// DuckDB ground-truth gate needs.
//
// Three properties the gate depends on, in the order they were learned the
// hard way:
//
//   - Every column is covered, strings and NULLs included. A per-column
//     numeric sum (internal/harness's value signature) skips string columns
//     entirely, which is how a NULLed name column (#314) shipped green.
//     Here a NULL renders "<null>", distinct from the empty string, and a
//     string cell contributes its bytes.
//
//   - Order sensitivity is the CALLER's decision, per query. With ordered
//     set, the row sequence is part of the digest, so a dropped ORDER BY
//     (#313/#316/#320) changes it; without, rows are sorted first, so an
//     engine free to return them in any order is not held to one. Passing
//     ordered for a query with no top-level ORDER BY would manufacture
//     failures; passing it false for one that has an ORDER BY is the blind
//     spot those three bugs walked through.
//
//   - Float summation order does not move it. The rows are rendered at two
//     precisions and a match at EITHER counts, which is the same
//     dual-precision policy Canon.Diff applies and for the same reason (see
//     the Canon doc comment): one ULP of accumulation noise can flip a
//     rendered digit at a rounding boundary, but not at two independent
//     quanta at once.
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
	default:
		s := fmt.Sprint(tv)
		return s, s
	}
}

// fingerprintFloat renders an exact integer as its digits (so a SUM one
// engine reports as BIGINT and the other as DOUBLE agree) and quantizes
// everything else to prec significant digits.
func fingerprintFloat(f float64, prec int) string {
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
