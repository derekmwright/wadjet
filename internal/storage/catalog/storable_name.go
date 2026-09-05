package catalog

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// CheckStorableName refuses a relation or column name that cannot be one
// component of an object key.
//
// A table's data lives at `tables/<name>/…` (partition.TablePrefix) and a
// partition key's column name becomes a directory component below it
// (`<col>=<value>/`), so these two names are not only identifiers: they are
// spelled into the object store's namespace. The LEXER takes a delimited
// identifier byte-exact, so `CREATE TABLE "../../../tmp/x"` handed the store a
// key that climbed out of its root, and on `storage.type: file` that was an
// arbitrary file write (CodeQL go/path-injection #23/#24/#25).
//
// objstore.ValidateObjectKey closes that at the store, which is the layer that
// has to be right regardless of who the caller is. This is the layer where a
// PERSON can be told what is wrong, at CREATE, before a table exists whose
// every write would fail.
//
// The rule is objstore.ValidateObjectKey's, narrowed to ONE component: no
// '/', no '\', no NUL, the name is not "." or "..", and it does not begin with
// '.'. A ".." INSIDE a component ("x..y") is accepted, because it names a real
// directory and the store accepts the key — the danger is a component that IS
// "..", not the two characters. The SQLSTATE is PostgreSQL's 42602
// invalid_name.
//
// This is a DELIBERATE DIVERGENCE and it is recorded in ADR-0012's list:
// PostgreSQL accepts any of these inside a double-quoted identifier, because
// its relations are rows in pg_class and never filenames. Wadjet's are objects
// in a store, and the alternative to refusing the name is a table whose data
// has no home — or, on a filesystem store, one whose data lands somewhere it
// was never meant to. It is name-only and LOUD: no query answers differently,
// and no name is silently rewritten.
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
