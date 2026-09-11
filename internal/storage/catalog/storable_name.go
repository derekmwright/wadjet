package catalog

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// CheckStorableName requires one usable object-key component at CREATE,
// complementing objstore.ValidateObjectKey's store-level guard (#23/#24/#25).
// Reject empty names, '/', backslash, NUL, '.', '..' and leading '.' with 42602;
// internal '..' such as x..y remains allowed. Do not silently rewrite names.
// This deliberate PostgreSQL identifier divergence is recorded in ADR-0012:
// table and partition-column names enter the object-store namespace.
// See docs/internals/catalog-storable-name-boundary.md for the design.
func CheckStorableName(kind, name string) error {
	switch {
	case name == "":
		return sqlerr.New("42602", "%s name is empty", kind)
	case strings.ContainsRune(name, '/'):
		return storableNameError(kind, name, "a '/'")
	case strings.ContainsRune(name, '\\'):
		return storableNameError(kind, name, "a '\\'")
	case strings.ContainsRune(name, 0):
		return storableNameError(kind, name, "a NUL byte")
	case name == "." || name == "..":
		return sqlerr.New("42602",
			"%s name %s is not a usable object-key component", kind, sqlerr.Quote(name))
	case strings.HasPrefix(name, "."):
		return sqlerr.New("42602",
			"%s name %s begins with '.': a %s name is a component of the object key its data is stored under, "+
				"and this deployment cannot store one", kind, sqlerr.Quote(name), kind)
	case len(name) > MaxNameBytes:
		// PostgreSQL TRUNCATES here rather than refusing — measured live on
		// postgres:17-alpine, an 80-byte name becomes 63 bytes with
		// `NOTICE 42622 identifier "…" will be truncated to "…"`. Wadjet
		// cannot: a relation name is a component of the object key its data is
		// stored under, so truncating it would silently point two different
		// tables at ONE location. Refusing is the only answer that keeps the
		// name and the location the same thing — and it is loud, where a
		// 300-byte name used to be accepted at CREATE and then fail every
		// write with ENAMETOOLONG (round-1 review P5).
		//
		// 42622 name_too_long is PostgreSQL's own class for this condition,
		// which is what its NOTICE carries.
		return sqlerr.New("42622",
			"%s name is %d bytes, over the %d-byte limit: a %s name is a component of the object key "+
				"its data is stored under, and this deployment cannot truncate one the way PostgreSQL does",
			kind, len(name), MaxNameBytes, kind)
	}
	return nil
}

// MaxNameBytes is PostgreSQL's own effective identifier length —
// NAMEDATALEN - 1, measured: a longer name is truncated to exactly this many
// bytes there. Holding wadjet to the same number is what makes "a name this
// engine ACCEPTS behaves the way PostgreSQL's does" true rather than nearly
// true: at or below it the two agree byte for byte, and above it PostgreSQL's
// own answer is already lossy.
const MaxNameBytes = 63

func storableNameError(kind, name, what string) error {
	return sqlerr.New("42602",
		"%s name %s contains %s: a %s name is a component of the object key its data is stored under, "+
			"and this deployment cannot store one", kind, sqlerr.Quote(name), what, kind)
}
