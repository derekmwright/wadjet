// SPDX-License-Identifier: MIT

// This file holds expr payload fns; ADR-0012 and ADR-0024 governs the execution contracts.
package expr

import (
	"encoding/base32"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/bits"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/geoip"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// --- Payload Search Functions ---

func fnPayloadContains(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	data := toBytes(args[0])
	pattern := toBytes(args[1])
	if len(pattern) == 0 {
		return true
	}
	for i := 0; i <= len(data)-len(pattern); i++ {
		found := true
		for j := 0; j < len(pattern); j++ {
			if data[i+j] != pattern[j] {
				found = false
				break
			}
		}
		if found {
			return true
		}
	}
	return false
}

func fnPayloadMatches(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	// The pattern is read as every SQL pattern is (translateAndCompile).
	return mustCompileSQLRegex(toString(args[1]), ownFunctionFlags).re.MatchString(toString(args[0]))
}

func fnPayloadOffset(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	data := toBytes(args[0])
	off := int(ToInt64(args[1]))
	length := int(ToInt64(args[2]))
	if off < 0 || length <= 0 || off+length > len(data) {
		return nil
	}
	return hex.EncodeToString(data[off : off+length])
}

func fnPayloadLength(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return int64(len(toBytes(args[0])))
}

// ── Regex: Additional ───────────────────────────────────────────────────────

// fnRegexpCount is regexp_count(string, pattern [, start [, flags]]): the
// number of matches PostgreSQL takes (findAll), searching from the start'th
// CHARACTER. A start below 1 is 22023 as on the server; a NULL start or
// flags is NULL.
func fnRegexpCount(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	start := int64(1)
	if len(args) >= 3 {
		if args[2] == nil {
			return nil
		}
		start = ToInt64(args[2])
		switch v := args[2].(type) {
		case int32, int16, int8:
		case int64:
			// The start is an integer on the server: a value past int4
			// arrives as a bigint, for which there is no regexp_count.
			if v != int64(int32(v)) {
				panic(fatalEval{sqlerr.New("42883", "function regexp_count(text, text, bigint) does not exist")})
			}
		case string:
		default:
			panic(fatalEval{sqlerr.New("42883", "function regexp_count(text, text, %T) does not exist: the start is an integer", v)})
		}
		if txt, ok := args[2].(string); ok {
			// An untyped literal ('3') is read as the integer it spells,
			// as the server resolves it.
			v, err := strconv.ParseInt(strings.TrimSpace(txt), 10, 32)
			if err != nil {
				panic(fatalEval{sqlerr.New("22P02", "invalid input syntax for type integer: %s", sqlerr.Quote(txt))})
			}
			start = v
		}
		if start < 1 {
			panic(fatalEval{sqlerr.New("22023", "invalid value for parameter \"start\": %d", start)})
		}
	}
	f, ok := regexFlagsArg(args, 3, "regexp_count")
	if !ok {
		return nil
	}
	re := mustCompileSQLRegex(toString(args[1]), f)
	s := toString(args[0])
	if start > 1 {
		off, ok := runeOffset(s, start-1)
		if !ok {
			return int64(0)
		}
		if re.readsLeft {
			// A match at the start position would read the character
			// before it (\y, \Y, ^, \A); Go's regexp matches a suffix only
			// as a whole text, which would answer for a different string.
			panic(fatalEval{sqlerr.New("0A000",
				"regexp_count with a start position over a pattern holding ^, \\A, \\y or \\Y is not supported")})
		}
		s = s[off:]
	}
	return int64(len(re.findAll(s)))
}

// runeOffset is the byte offset of the n'th character of s (0-based); n
// equal to the character count is len(s). ok=false: s is shorter.
func runeOffset(s string, n int64) (int, bool) {
	var i int64
	for off := range s {
		if i == n {
			return off, true
		}
		i++
	}
	return len(s), i == n
}

func fnRegexpExtractAll(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	re := mustCompileSQLRegex(toString(args[1]), ownFunctionFlags)
	src := toString(args[0])
	matches := re.findAll(src)
	if len(matches) == 0 {
		return "[]"
	}
	// Return as JSON array string (no native array type yet)
	parts := make([]string, len(matches))
	for i, m := range matches {
		escaped, _ := json.Marshal(src[m[0]:m[1]])
		parts[i] = string(escaped)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func fnRegexpSplit(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	re := mustCompileSQLRegex(toString(args[1]), ownFunctionFlags)
	parts := re.re.Split(toString(args[0]), -1)
	jsonParts := make([]string, len(parts))
	for i, p := range parts {
		escaped, _ := json.Marshal(p)
		jsonParts[i] = string(escaped)
	}
	return "[" + strings.Join(jsonParts, ",") + "]"
}

// ── String: Additional ──────────────────────────────────────────────────────

func fnSplit(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	parts := strings.Split(fmt.Sprint(args[0]), fmt.Sprint(args[1]))
	jsonParts := make([]string, len(parts))
	for i, p := range parts {
		escaped, _ := json.Marshal(p)
		jsonParts[i] = string(escaped)
	}
	return "[" + strings.Join(jsonParts, ",") + "]"
}

// ── Bitwise: Shifts ─────────────────────────────────────────────────────────

func fnBitwiseLeftShift(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	v := bitIntArg(args[0])
	shift := bitIntArg(args[1])
	if shift < 0 || shift >= 64 {
		return nil
	}
	return int64(v << uint(shift))
}

func fnBitwiseRightShift(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	v := bitIntArg(args[0])
	shift := bitIntArg(args[1])
	if shift < 0 || shift >= 64 {
		return nil
	}
	// Logical right shift (unsigned)
	return int64(uint64(v) >> uint(shift))
}

func fnBitwiseArithmeticShiftRight(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	v := bitIntArg(args[0])
	shift := bitIntArg(args[1])
	if shift < 0 || shift >= 64 {
		return nil
	}
	// Arithmetic right shift (preserves sign bit)
	return int64(v >> uint(shift))
}

// ── UUID: Generation ────────────────────────────────────────────────────────

func fnUUID(args []any) any {
	// Generate a random UUID v4
	var buf [16]byte
	for i := range buf {
		buf[i] = byte(rand.Intn(256))
	}
	buf[6] = (buf[6] & 0x0f) | 0x40 // version 4
	buf[8] = (buf[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
}

func fnToBase32(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return base32Encoding.EncodeToString([]byte(fmt.Sprint(args[0])))
}

func fnFromBase32(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data, err := base32Encoding.DecodeString(fmt.Sprint(args[0]))
	if err != nil {
		// Try with padding
		data, err = base32.StdEncoding.DecodeString(fmt.Sprint(args[0]))
		if err != nil {
			return nil
		}
	}
	return string(data)
}

func fnXXHash64(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	// XXHash64 implementation using FNV-like approach
	// Using a simple but correct XXHash64 implementation
	data := []byte(fmt.Sprint(args[0]))
	h := xxhash64Sum(data)
	return fmt.Sprintf("%016x", h)
}

// xxhash64Sum computes XXHash64 of data with seed 0.
func xxhash64Sum(data []byte) uint64 {
	const (
		prime1 uint64 = 0x9E3779B185EBCA87
		prime2 uint64 = 0x14DEF9DEA2F79CD6
		prime3 uint64 = 0x165667B19E3779F9
		prime4 uint64 = 0x85EBCA77C2B2AE63
		prime5 uint64 = 0x27D4EB2F165667C5
	)

	n := len(data)
	var h uint64

	if n >= 32 {
		v1 := prime1 + prime2
		v2 := prime2
		v3 := uint64(0)
		var v4 uint64
		v4 -= prime1

		for len(data) >= 32 {
			v1 = xxh64Round(v1, binary.LittleEndian.Uint64(data[0:8]))
			v2 = xxh64Round(v2, binary.LittleEndian.Uint64(data[8:16]))
			v3 = xxh64Round(v3, binary.LittleEndian.Uint64(data[16:24]))
			v4 = xxh64Round(v4, binary.LittleEndian.Uint64(data[24:32]))
			data = data[32:]
		}

		h = bits.RotateLeft64(v1, 1) + bits.RotateLeft64(v2, 7) +
			bits.RotateLeft64(v3, 12) + bits.RotateLeft64(v4, 18)
		h = xxh64MergeRound(h, v1)
		h = xxh64MergeRound(h, v2)
		h = xxh64MergeRound(h, v3)
		h = xxh64MergeRound(h, v4)
	} else {
		h = prime5
	}

	h += uint64(n)

	for len(data) >= 8 {
		k := binary.LittleEndian.Uint64(data[0:8])
		k *= prime2
		k = bits.RotateLeft64(k, 31)
		k *= prime1
		h ^= k
		h = bits.RotateLeft64(h, 27)*prime1 + prime4
		data = data[8:]
	}

	for len(data) >= 4 {
		h ^= uint64(binary.LittleEndian.Uint32(data[0:4])) * prime1
		h = bits.RotateLeft64(h, 23)*prime2 + prime3
		data = data[4:]
	}

	for len(data) > 0 {
		h ^= uint64(data[0]) * prime5
		h = bits.RotateLeft64(h, 11) * prime1
		data = data[1:]
	}

	h ^= h >> 33
	h *= prime2
	h ^= h >> 29
	h *= prime3
	h ^= h >> 32

	return h
}

func xxh64Round(acc, input uint64) uint64 {
	const prime1 uint64 = 0x9E3779B185EBCA87
	const prime2 uint64 = 0x14DEF9DEA2F79CD6
	acc += input * prime2
	acc = bits.RotateLeft64(acc, 31)
	acc *= prime1
	return acc
}

func xxh64MergeRound(acc, val uint64) uint64 {
	const prime1 uint64 = 0x9E3779B185EBCA87
	const prime4 uint64 = 0x85EBCA77C2B2AE63
	val = xxh64Round(0, val)
	acc ^= val
	acc = acc*prime1 + prime4
	return acc
}

func fnMurmur3(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	data := []byte(fmt.Sprint(args[0]))
	h := murmur3Hash128(data)
	return fmt.Sprintf("%016x%016x", h[0], h[1])
}

// murmur3Hash128 computes MurmurHash3 x64_128 with seed 0.
func murmur3Hash128(data []byte) [2]uint64 {
	const (
		c1 uint64 = 0x87c37b91114253d5
		c2 uint64 = 0x4cf5ad432745937f
	)

	var h1, h2 uint64
	nblocks := len(data) / 16

	for i := 0; i < nblocks; i++ {
		k1 := binary.LittleEndian.Uint64(data[i*16:])
		k2 := binary.LittleEndian.Uint64(data[i*16+8:])

		k1 *= c1
		k1 = bits.RotateLeft64(k1, 31)
		k1 *= c2
		h1 ^= k1
		h1 = bits.RotateLeft64(h1, 27)
		h1 += h2
		h1 = h1*5 + 0x52dce729

		k2 *= c2
		k2 = bits.RotateLeft64(k2, 33)
		k2 *= c1
		h2 ^= k2
		h2 = bits.RotateLeft64(h2, 31)
		h2 += h1
		h2 = h2*5 + 0x38495ab5
	}

	tail := data[nblocks*16:]
	var k1, k2 uint64
	switch len(tail) {
	case 15:
		k2 ^= uint64(tail[14]) << 48
		fallthrough
	case 14:
		k2 ^= uint64(tail[13]) << 40
		fallthrough
	case 13:
		k2 ^= uint64(tail[12]) << 32
		fallthrough
	case 12:
		k2 ^= uint64(tail[11]) << 24
		fallthrough
	case 11:
		k2 ^= uint64(tail[10]) << 16
		fallthrough
	case 10:
		k2 ^= uint64(tail[9]) << 8
		fallthrough
	case 9:
		k2 ^= uint64(tail[8])
		k2 *= c2
		k2 = bits.RotateLeft64(k2, 33)
		k2 *= c1
		h2 ^= k2
		fallthrough
	case 8:
		k1 ^= uint64(tail[7]) << 56
		fallthrough
	case 7:
		k1 ^= uint64(tail[6]) << 48
		fallthrough
	case 6:
		k1 ^= uint64(tail[5]) << 40
		fallthrough
	case 5:
		k1 ^= uint64(tail[4]) << 32
		fallthrough
	case 4:
		k1 ^= uint64(tail[3]) << 24
		fallthrough
	case 3:
		k1 ^= uint64(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint64(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint64(tail[0])
		k1 *= c1
		k1 = bits.RotateLeft64(k1, 31)
		k1 *= c2
		h1 ^= k1
	}

	h1 ^= uint64(len(data))
	h2 ^= uint64(len(data))

	h1 += h2
	h2 += h1

	// fmix64
	fmix := func(h uint64) uint64 {
		h ^= h >> 33
		h *= 0xff51afd7ed558ccd
		h ^= h >> 33
		h *= 0xc4ceb9fe1a85ec53
		h ^= h >> 33
		return h
	}

	h1 = fmix(h1)
	h2 = fmix(h2)

	h1 += h2
	h2 += h1

	return [2]uint64{h1, h2}
}

// ── Date/Time: ISO 8601 ─────────────────────────────────────────────────────

func fnFromISO8601Timestamp(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := fmt.Sprint(args[0])
	// Try common ISO 8601 formats
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.000",
		"2006-01-02 15:04:05",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return instantBox(t)
		}
	}
	return nil
}

func fnFromISO8601Date(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := fmt.Sprint(args[0])
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return nil
	}
	return dateBox(t)
}

func fnToISO8601(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ms := int64(ToFloat64(args[0]))
	t := time.UnixMilli(ms).UTC()
	return t.Format(time.RFC3339)
}

func fnToMilliseconds(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := fmt.Sprint(args[0])
	// Try parsing as ISO 8601 first
	for _, layout := range []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UnixMilli()
		}
	}
	// If already a number, return as-is
	if v, ok := args[0].(int64); ok {
		return v
	}
	return nil
}

func fnTimezoneHour(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ms := int64(ToFloat64(args[0]))
	t := time.UnixMilli(ms)
	_, offset := t.Zone()
	return int64(offset / 3600)
}

func fnTimezoneMinute(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	ms := int64(ToFloat64(args[0]))
	t := time.UnixMilli(ms)
	_, offset := t.Zone()
	return int64((offset % 3600) / 60)
}

// ── Formatting ──────────────────────────────────────────────────────────────

func fnFormatNumber(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	v := ToFloat64(args[0])

	// Default: format with commas, no decimal places
	decimals := 0
	if len(args) >= 2 && args[1] != nil {
		decimals = int(ToFloat64(args[1]))
	}

	// Format the number
	negative := v < 0
	if negative {
		v = -v
	}

	// Format with specified decimal places
	s := strconv.FormatFloat(v, 'f', decimals, 64)

	// Split integer and decimal parts
	parts := strings.SplitN(s, ".", 2)
	intPart := parts[0]

	// Add comma separators to integer part
	var result strings.Builder
	if negative {
		result.WriteByte('-')
	}
	for i, c := range intPart {
		if i > 0 && (len(intPart)-i)%3 == 0 {
			result.WriteByte(',')
		}
		result.WriteRune(c)
	}
	if len(parts) == 2 {
		result.WriteByte('.')
		result.WriteString(parts[1])
	}

	return result.String()
}

// ── GeoIP / ASN Functions ───────────────────────────────────────────────────

// geoipParseIP extracts a net.IP from the first argument.
func geoipParseIP(args []any) net.IP {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return net.ParseIP(fmt.Sprint(args[0]))
}

func fnGeoipCountry(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil || rec.Country.ISOCode == "" {
		return nil
	}
	return rec.Country.ISOCode
}

func fnGeoipCountryName(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil {
		return nil
	}
	name := rec.Country.Names["en"]
	if name == "" {
		return nil
	}
	return name
}

func fnGeoipCity(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil {
		return nil
	}
	name := rec.City.Names["en"]
	if name == "" {
		return nil
	}
	return name
}

func fnGeoipSubdivision(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil || len(rec.Subdivisions) == 0 {
		return nil
	}
	name := rec.Subdivisions[0].Names["en"]
	if name == "" {
		// Fall back to ISO code
		if rec.Subdivisions[0].ISOCode != "" {
			return rec.Subdivisions[0].ISOCode
		}
		return nil
	}
	return name
}

func fnGeoipPostalCode(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil || rec.Postal.Code == "" {
		return nil
	}
	return rec.Postal.Code
}

func fnGeoipLatitude(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil {
		return nil
	}
	if rec.Location.Latitude == 0 && rec.Location.Longitude == 0 {
		return nil
	}
	return rec.Location.Latitude
}

func fnGeoipLongitude(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil {
		return nil
	}
	if rec.Location.Latitude == 0 && rec.Location.Longitude == 0 {
		return nil
	}
	return rec.Location.Longitude
}

func fnGeoipTimezone(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil || rec.Location.TimeZone == "" {
		return nil
	}
	return rec.Location.TimeZone
}

func fnGeoipContinent(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupCity(ip)
	if rec == nil || rec.Continent.Code == "" {
		return nil
	}
	return rec.Continent.Code
}

func fnGeoipASN(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupASN(ip)
	if rec == nil || rec.Number == 0 {
		return nil
	}
	return int64(rec.Number)
}

func fnGeoipOrg(args []any) any {
	ip := geoipParseIP(args)
	if ip == nil {
		return nil
	}
	rec := geoip.DefaultReader.LookupASN(ip)
	if rec == nil || rec.Organization == "" {
		return nil
	}
	return rec.Organization
}
