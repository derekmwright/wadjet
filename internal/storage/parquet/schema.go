// Package parquet provides Parquet file reading and writing on top of objstore.
package parquet

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TypeID identifies the data type of a column.
type TypeID int

const (
	TypeBool TypeID = iota
	TypeInt32
	TypeInt64
	TypeFloat32
	TypeFloat64
	TypeString
	TypeBytes
	TypeTimestamp
	TypeIPv4     // stored as uint32 (4 bytes)
	TypeIPv6     // stored as [16]byte via BytesColumn
	TypeCIDR     // stored as string (e.g., "192.168.1.0/24")
	TypeMAC      // stored as uint64 (6 bytes, upper 2 zero)
	TypePort     // stored as uint16 in Int32Data
	TypeProtocol // IANA protocol number, stored as uint8 in Int32Data
	TypeDuration // nanoseconds, stored as int64
	TypeUUID     // 128-bit UUID stored as 16-byte ByteArray
	TypeDate     // calendar date stored as int32 (days since 1970-01-01)
	TypeDecimal  // fixed-point DECIMAL(p,s) stored as scaled Int128
	TypeArray    // variable-length array of a single element type
	TypeRow      // struct/row with named fields
	TypeMap      // key-value map, stored as ARRAY(ROW("key","value"))
	TypeVector   // fixed-dimension float32 embedding vector
)

func (t TypeID) String() string {
	switch t {
	case TypeBool:
		return "BOOL"
	case TypeInt32:
		return "INT32"
	case TypeInt64:
		return "INT64"
	case TypeFloat32:
		return "FLOAT32"
	case TypeFloat64:
		return "FLOAT64"
	case TypeString:
		return "STRING"
	case TypeBytes:
		return "BYTES"
	case TypeTimestamp:
		return "TIMESTAMP"
	case TypeIPv4:
		return "IPV4"
	case TypeIPv6:
		return "IPV6"
	case TypeCIDR:
		return "CIDR"
	case TypeMAC:
		return "MAC"
	case TypePort:
		return "PORT"
	case TypeProtocol:
		return "PROTOCOL"
	case TypeDuration:
		return "DURATION"
	case TypeUUID:
		return "UUID"
	case TypeDate:
		return "DATE"
	case TypeDecimal:
		return "DECIMAL"
	case TypeArray:
		return "ARRAY"
	case TypeRow:
		return "ROW"
	case TypeMap:
		return "MAP"
	case TypeVector:
		return "VECTOR"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(t))
	}
}

// ParseTypeID parses a type name string into a TypeID.
// Supports parameterized types like "DECIMAL(10,2)".
func ParseTypeID(s string) (TypeID, error) {
	upper := strings.ToUpper(strings.TrimSpace(s))
	// Handle parameterized types
	if strings.HasPrefix(upper, "DECIMAL") || strings.HasPrefix(upper, "NUMERIC") {
		return TypeDecimal, nil
	}
	if strings.HasPrefix(upper, "ARRAY") {
		return TypeArray, nil
	}
	if strings.HasPrefix(upper, "ROW") || strings.HasPrefix(upper, "STRUCT") {
		return TypeRow, nil
	}
	if strings.HasPrefix(upper, "MAP") {
		return TypeMap, nil
	}
	if strings.HasPrefix(upper, "VECTOR") {
		return TypeVector, nil
	}
	// A PARAMETERIZED string spelling. `VARCHAR(255)` is what a PostgreSQL
	// user writes and what a migration tool emits, and it used to fail the
	// whole CREATE TABLE with `unknown type: VARCHAR(4)` — this switch reads
	// the WHOLE name, so the parameter made it match nothing. The length is
	// not stored (one unparameterized TypeString is all the engine has), so an
	// INSERT past n is accepted where PostgreSQL raises 22001: a SUPERSET,
	// which ADR-0012 records as the acceptable direction, and a far smaller
	// divergence than refusing the table (#838).
	if _, err, ok := StringTypeLength(upper); ok {
		if err != nil {
			return 0, err
		}
		return TypeString, nil
	}
	// FLOAT(n) — the SQL-standard spelling of "a binary float with at least n
	// bits of mantissa", which PostgreSQL resolves to real or double precision
	// (#652). It failed the whole CREATE TABLE before, for the same reason
	// VARCHAR(4) did.
	if bits, err, ok := FloatTypePrecision(upper); ok {
		if err != nil {
			return 0, err
		}
		if bits <= 24 {
			return TypeFloat32, nil
		}
		return TypeFloat64, nil
	}
	switch upper {
	case "BOOL", "BOOLEAN":
		return TypeBool, nil
	case "INT32", "INT", "INTEGER":
		return TypeInt32, nil
	case "INT64", "BIGINT", "LONG":
		return TypeInt64, nil
	case "FLOAT32", "FLOAT":
		return TypeFloat32, nil
	case "FLOAT64", "DOUBLE":
		return TypeFloat64, nil
	case "STRING", "VARCHAR", "TEXT":
		return TypeString, nil
	case "BYTES", "BINARY", "VARBINARY":
		return TypeBytes, nil
	case "TIMESTAMP", "DATETIME":
		return TypeTimestamp, nil
	case "IPV4", "IP":
		return TypeIPv4, nil
	case "IPV6":
		return TypeIPv6, nil
	case "CIDR":
		return TypeCIDR, nil
	case "MAC", "MACADDR":
		return TypeMAC, nil
	case "PORT":
		return TypePort, nil
	case "PROTOCOL", "PROTO":
		return TypeProtocol, nil
	case "DURATION", "INTERVAL":
		return TypeDuration, nil
	case "UUID", "GUID":
		return TypeUUID, nil
	case "DATE":
		return TypeDate, nil
	default:
		// PostgreSQL's undefined_object for a type name that names nothing:
		// `type "bogustype" does not exist`, SQLSTATE 42704, measured live on
		// 17.11. It used to be a bare error carrying no class, so the wire
		// reported the blanket 42000 (#366's shape) — and the CAST door did
		// not consult this at all, declaring a STRING column over the operand
		// instead (#652).
		//
		// The CLASS is what every door reports, not only the message: arc E2
		// found `CREATE TABLE t (a NOSUCHTYPE)` answering over HTTP with the
		// text alone while the API reference promised a SQLSTATE, and this is
		// the one place that class is decided for all of them.
		return 0, sqlerr.New("42704", "type %s does not exist", sqlerr.Quote(strings.TrimSpace(s)))
	}
}

// KnownTypeName reports whether this engine has a type by this name, for the
// callers that need the QUESTION without the answer — expr's CAST door, which
// has its own wider accept-set of PostgreSQL spellings on top of this one.
func KnownTypeName(s string) bool {
	_, err := ParseTypeID(s)
	return err == nil
}

// StringTypeLength reads a PARAMETERIZED string type name — `VARCHAR(255)`,
// `CHAR(4)`, `CHARACTER VARYING(10)` — and validates the parameter.
//
// It is ONE reading, and that is the whole point of it living here. The review
// of #838's first pass found the CAST door refusing `VARCHAR(0)` with 22023
// while the DDL door CREATED the table: one type name, two dispositions across
// two doors, which is the defect class this arc exists to close. The refusal
// has to be where both doors can read it, below the expression layer and below
// the planner — the same argument `ParseDateDays` settles for dates.
//
// PostgreSQL 17.11's own rules and messages, measured:
//
//	VARCHAR(0), CHAR(0)        22023  length for type varchar|char must be at least 1
//	VARCHAR(1e8), CHAR(1e8)    22023  length for type varchar|char cannot exceed 10485760
//	VARCHAR(abc), VARCHAR(-1)  42601  syntax error at or near "abc"|"-"
//	TEXT(5)                    42601  type modifier is not allowed for type "text"
//	VARCHAR(10485760)          accepted — the exact maximum
//
// The type NAME in the 22023 message is PostgreSQL's internal one: `char` for
// all of CHAR / CHARACTER / NCHAR, `varchar` for the varying spellings. TEXT
// is deliberately NOT a length-carrying name here, because PostgreSQL allows
// no modifier on it at all.
//
// ok=false means the name is not a parameterized string type and the caller's
// own rules stand; a non-nil error is the refusal, which every caller must
// PROPAGATE — it is the answer to the query.
func StringTypeLength(name string) (n int, err error, ok bool) {
	upper := strings.ToUpper(strings.TrimSpace(name))
	base, isParam := stripTypeParams(upper,
		"VARCHAR", "CHARACTER VARYING", "CHAR", "CHARACTER", "NCHAR", "NVARCHAR", "TEXT")
	if !isParam {
		return 0, nil, false
	}
	// The argument is read from the ORIGINAL spelling, not the uppercased
	// one: PostgreSQL's syntax error names the offending token verbatim, so
	// `VARCHAR(abc)` must say `"abc"` and not `"ABC"`.
	orig := strings.TrimSpace(name)
	open := strings.IndexByte(orig, '(')
	arg := strings.TrimSpace(orig[open+1 : len(orig)-1])
	if base == "TEXT" {
		return 0, sqlerr.New("42601", `type modifier is not allowed for type "text"`), true
	}
	kind := "varchar"
	if base == "CHAR" || base == "CHARACTER" || base == "NCHAR" {
		kind = "char"
	}
	v, convErr := strconv.Atoi(arg)
	if convErr != nil || v < 0 {
		// A non-numeric modifier, or a negative one: PostgreSQL's GRAMMAR
		// rejects both before any type lookup, naming the offending token.
		tok := arg
		if strings.HasPrefix(arg, "-") {
			tok = "-"
		}
		return 0, sqlerr.New("42601", "syntax error at or near %q", tok), true
	}
	if v < 1 {
		return 0, sqlerr.New("22023", "length for type %s must be at least 1", kind), true
	}
	if v > maxStringTypeLength {
		return 0, sqlerr.New("22023", "length for type %s cannot exceed %d",
			kind, maxStringTypeLength), true
	}
	return v, nil, true
}

// StringLengthUnconstrainedVarchar is what the string-modifier channel
// carries for a VARCHAR destination spelled WITHOUT a length. It is not a
// length — the wire typmod is -1, the same as for `text` — it is the
// declaration `character varying`, which PostgreSQL sends as OID 1043 where
// `text` is 25. Zero stays "not a string destination at all".
const StringLengthUnconstrainedVarchar = -1

// VarcharNoLength reports whether name is the VARCHAR family spelled without a
// length modifier. `SELECT 'a'::varchar` describes as `character varying`
// (1043) at typmod -1 on PostgreSQL 17.11, measured — a different DECLARATION
// from `text` (25) even though the two hold the same bytes and compare the
// same way, and a JDBC or ODBC client reads the OID to pick its column class.
//
// `CHAR` and `CHARACTER` without a length are deliberately NOT here.
// PostgreSQL reads those as `character(1)` and TRUNCATES the value to one
// character, which is a VALUE this engine does not produce; claiming 1043 for
// them would publish a wrong length rule over a wrong value. They stay in
// ADR-0012's bpchar residual.
func VarcharNoLength(name string) bool {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "VARCHAR", "NVARCHAR", "CHARACTER VARYING":
		return true
	}
	return false
}

// maxStringTypeLength is PostgreSQL's own cap on a varchar/bpchar modifier,
// MaxAttrSize — 10485760, measured: `VARCHAR(10485760)` is accepted and
// `VARCHAR(10485761)` is 22023.
const maxStringTypeLength = 10485760

// FloatTypePrecision reads `FLOAT(n)`, the SQL-standard spelling of a binary
// float with at least n bits of mantissa (#652).
//
// PostgreSQL 17 resolves it by WIDTH — `float(1..24)` is real, `float(25..53)`
// is double precision, and a bare `FLOAT` is double precision (pg_typeof,
// measured live) — and refuses the two ends with 22023 and its own wording:
//
//	CAST(x AS FLOAT(0))   precision for type float must be at least 1 bit
//	CAST(x AS FLOAT(54))  precision for type float must be less than 54 bits
//
// ok=false means the name is not a parameterized FLOAT at all and the caller's
// own rules stand; a non-nil error is the 22023, which every caller must
// PROPAGATE rather than fall back around — it is the answer to the query.
//
// It lives here, below both the expression layer and the planner, because all
// three read it: the cast evaluator picks float32 vs float64 from it, the
// planner declares the output column from it, and DDL types a column from it.
// One reading, three callers, which is what keeps a `FLOAT(1)` column and a
// `CAST(x AS FLOAT(1))` from disagreeing about what a float(1) is.
func FloatTypePrecision(name string) (bits int, err error, ok bool) {
	upper := strings.ToUpper(strings.TrimSpace(name))
	if _, isParam := stripTypeParams(upper, "FLOAT"); !isParam {
		return 0, nil, false
	}
	open := strings.IndexByte(upper, '(')
	n, convErr := strconv.Atoi(strings.TrimSpace(upper[open+1 : len(upper)-1]))
	if convErr != nil {
		return 0, nil, false
	}
	// 22023 is PostgreSQL's invalid_parameter_value, which is what it raises
	// for a type modifier outside its type's range.
	if n < 1 {
		return 0, sqlerr.New("22023", "precision for type float must be at least 1 bit"), true
	}
	if n > 53 {
		return 0, sqlerr.New("22023", "precision for type float must be less than 54 bits"), true
	}
	return n, nil, true
}

// stripTypeParams reports whether upper is one of names followed by a
// parenthesized parameter list — `VARCHAR(255)`, `CHARACTER VARYING (10)` —
// and returns the bare name. It matches the NAME exactly rather than by
// prefix, so `VARCHARX(1)` is still unknown.
func stripTypeParams(upper string, names ...string) (string, bool) {
	open := strings.IndexByte(upper, '(')
	if open < 0 || !strings.HasSuffix(upper, ")") {
		return "", false
	}
	base := strings.TrimSpace(upper[:open])
	for _, n := range names {
		if base == n {
			return base, true
		}
	}
	return "", false
}

// ParseDecimalParams extracts precision and scale from a type string like
// "DECIMAL(10,2)", and REFUSES a declaration this carrier cannot honour.
// A bare DECIMAL with no parameters is (38, 0).
//
// The bounds are 1 <= precision <= 38 and 0 <= scale <= precision.
//
//   - The precision bound is a DOCUMENTED DIVERGENCE from PostgreSQL, which
//     accepts numeric(p, s) up to p = 1000 because its numeric is unbounded.
//     Wadjet's DECIMAL is a 128-bit unscaled integer (ADR-0024 item 1) and 38
//     digits is its whole range, so `DECIMAL(50,2)` is a column no value can
//     satisfy. It used to be ACCEPTED, and the writer then emitted a 16-byte
//     FIXED_LEN_BYTE_ARRAY leaf annotated DECIMAL(50, s) — an annotation the
//     payload cannot hold, in a file the Apache implementation refuses to open
//     (R8/#647). Refusing the DECLARATION is the only honest answer: the
//     alternative is a column that lies about itself in every file it writes.
//   - The scale bound is the PARQUET FORMAT's, not wadjet's: the DECIMAL
//     logical type requires 0 <= scale <= precision. PostgreSQL accepts scale
//     from -1000 to 1000, and `numeric(9,10)` (a value below 0.1 with nine
//     significant digits) is legal there; there is no parquet annotation for
//     it, so it is refused here too.
//
// The SQLSTATE is 22023 invalid_parameter_value, which is what PostgreSQL
// raises for a precision outside ITS bound ("NUMERIC precision 1001 must be
// between 1 and 1000", verified live on postgres:17-alpine).
func ParseDecimalParams(s string) (precision, scale int, err error) {
	upper := strings.ToUpper(strings.TrimSpace(s))
	precision, scale = 38, 0 // a bare DECIMAL

	idx := strings.Index(upper, "(")
	end := strings.Index(upper, ")")
	if idx >= 0 && end > idx {
		parts := strings.Split(upper[idx+1:end], ",")
		if len(parts) > 2 {
			return 0, 0, sqlerr.New("22023",
				"invalid type %q: DECIMAL takes at most two parameters, a precision and a scale", s)
		}
		if p := strings.TrimSpace(parts[0]); p != "" {
			if precision, err = strconv.Atoi(p); err != nil {
				return 0, 0, sqlerr.New("22023",
					"invalid type %q: NUMERIC precision %q is not an integer", s, p)
			}
		}
		if len(parts) == 2 {
			// A scale parameter with no precision beside it is not a
			// declaration this grammar has; the empty first part above kept
			// the default 38, which is what `DECIMAL(,2)` would mean.
			if sc := strings.TrimSpace(parts[1]); sc != "" {
				if scale, err = strconv.Atoi(sc); err != nil {
					return 0, 0, sqlerr.New("22023",
						"invalid type %q: NUMERIC scale %q is not an integer", s, sc)
				}
			}
		}
	}
	if precision < 1 || precision > MaxDecimalDigits {
		return 0, 0, sqlerr.New("22023",
			"NUMERIC precision %d must be between 1 and %d: wadjet's DECIMAL is a 128-bit "+
				"unscaled integer and %d digits is its whole range (ADR-0024 item 1), where "+
				"PostgreSQL's unbounded numeric allows up to 1000",
			precision, MaxDecimalDigits, MaxDecimalDigits)
	}
	if scale < 0 || scale > precision {
		return 0, 0, sqlerr.New("22023",
			"NUMERIC scale %d must be between 0 and the precision %d: the parquet DECIMAL "+
				"annotation has no form for a negative scale or for a scale past the precision, "+
				"where PostgreSQL allows -1000 to 1000",
			scale, precision)
	}
	return precision, scale, nil
}

// DeclaredColumn builds a Column from one DDL column declaration: the type
// exactly as written, plus its nullability.
//
// It is the ONE place a declaration becomes a Column, because a DECIMAL's
// (p, s) lives in the type TEXT and nowhere else. Three copies of "ParseTypeID
// and fill in the name" existed — the embedded API, the HTTP server and gRPC —
// and only the first read the parameters, so `CREATE TABLE t (d DECIMAL(9,2))`
// over HTTP or gRPC produced a Precision 0, Scale 0 column: 12.34 stored as
// 12, 9999999.999 stored as 10000000 with no error, and DECIMAL(50,2)
// accepted (#647 review). Copies of a declaration parser drift toward the
// laziest one; there is now one to drift from.
// It resolves EVERY parameterized type, not only DECIMAL's (p, s). The first
// version read the decimal parameters and nothing else, so `VECTOR(384)`
// created a column with `Dimension: 0` — a table no INSERT could ever write,
// failing at flush with an internal error and no SQLSTATE — and
// `ARRAY(DECIMAL(9,2))`, `ROW(a INT64, d DECIMAL(9,2))` and
// `MAP(STRING, DECIMAL(9,2))` lost their element, field and key/value
// declarations entirely (#675). ResolveColumn already knew how to read all of
// them and had no non-test caller; this is that caller.
// The NAME is taken as given. It used to be lowercased here, which folded a
// DELIMITED declaration too — `CREATE TABLE t ("WatchID" INT64)` stored
// `watchid`, so the one spelling PostgreSQL guarantees would work was the one
// that did not, and a DDL-created table could not hold a name a
// parquet-registered one holds every day. Since #731 an UNQUOTED identifier
// is already folded when it gets here (the lexer does it, once), so the only
// declarations this changes are the delimited ones, which are exactly the
// ones that asked to keep their bytes. Fold-uniqueness within the schema is
// still enforced, by catalog.checkDistinctColumnNames.
func DeclaredColumn(name, typeStr string, nullable bool) (Column, error) {
	col, err := ResolveColumn(name, typeStr)
	if err != nil {
		return Column{}, err
	}
	// ResolveColumn answers for a nested field, where parquet's repetition is
	// optional by default; only the TOP-level declaration carries a NOT NULL.
	col.Nullable = nullable
	return col, nil
}

// ResolveColumn parses a type string into a fully-resolved Column with nested types.
// Handles: "INT64", "ARRAY(STRING)", "ROW(name STRING, age INT32)", "MAP(STRING, INT64)".
func ResolveColumn(name, typeStr string) (Column, error) {
	trimmed := strings.TrimSpace(typeStr)
	upper := strings.ToUpper(trimmed)

	// Check for parameterized nested types
	if idx := strings.Index(upper, "("); idx >= 0 {
		base := strings.TrimSpace(upper[:idx])
		// Extract inner params (everything between outer parens)
		end := strings.LastIndex(upper, ")")
		if end < 0 {
			return Column{}, fmt.Errorf("unmatched parenthesis in type: %s", typeStr)
		}
		// The ORIGINAL spelling, not the upper-cased copy: a ROW's inner text
		// carries FIELD NAMES, and slicing the upper-cased string renamed
		// every one of them (`ROW(a INT64)` declared a field called "A" while
		// every other door lower-cases). The base type above is decided from
		// the upper-cased copy, which is where case does not matter.
		inner := strings.TrimSpace(trimmed[idx+1 : end])

		switch base {
		case "ARRAY":
			elemCol, err := ResolveColumn("element", inner)
			if err != nil {
				return Column{}, fmt.Errorf("invalid ARRAY element type: %w", err)
			}
			return Column{Name: name, Type: TypeArray, Nullable: true, ElementType: &elemCol}, nil

		case "MAP":
			parts := splitTopLevel(inner)
			if len(parts) != 2 {
				return Column{}, fmt.Errorf("MAP requires exactly 2 type parameters, got %d", len(parts))
			}
			keyCol, err := ResolveColumn("key", parts[0])
			if err != nil {
				return Column{}, fmt.Errorf("invalid MAP key type: %w", err)
			}
			valCol, err := ResolveColumn("value", parts[1])
			if err != nil {
				return Column{}, fmt.Errorf("invalid MAP value type: %w", err)
			}
			entryCol := Column{
				Name: "entry", Type: TypeRow, Fields: []Column{keyCol, valCol},
			}
			return Column{Name: name, Type: TypeMap, Nullable: true, ElementType: &entryCol}, nil

		case "ROW", "STRUCT":
			fields, err := parseRowFields(inner)
			if err != nil {
				return Column{}, err
			}
			return Column{Name: name, Type: TypeRow, Nullable: true, Fields: fields}, nil

		case "DECIMAL", "NUMERIC":
			p, s, err := ParseDecimalParams(typeStr)
			if err != nil {
				return Column{}, err
			}
			return Column{Name: name, Type: TypeDecimal, Nullable: true, Precision: p, Scale: s}, nil

		case "VECTOR":
			dim := parseVectorDim(inner)
			if dim <= 0 {
				return Column{}, fmt.Errorf("VECTOR requires positive dimension, got %q", inner)
			}
			return Column{Name: name, Type: TypeVector, Nullable: true, Dimension: dim}, nil
		}
	}

	// Simple type. The ORIGINAL spelling, for the reason the ROW branch above
	// gives: ParseTypeID upper-cases whatever it is handed for the name
	// lookup, and it also has to QUOTE the offending token back in a syntax
	// error — `VARCHAR(abc)` is `syntax error at or near "abc"` on the live
	// server, not `"ABC"`. Handing it the upper-cased copy shouted the user's
	// identifier back at them.
	tid, err := ParseTypeID(trimmed)
	if err != nil {
		return Column{}, err
	}
	col := Column{Name: name, Type: tid, Nullable: true}
	if tid == TypeDecimal {
		p, s, err := ParseDecimalParams(typeStr)
		if err != nil {
			return Column{}, err
		}
		col.Precision, col.Scale = p, s
	}
	return col, nil
}

// parseVectorDim parses the dimension from a VECTOR(N) inner string.
func parseVectorDim(s string) int {
	s = strings.TrimSpace(s)
	var dim int
	if _, err := fmt.Sscanf(s, "%d", &dim); err != nil {
		return 0
	}
	return dim
}

// splitTopLevel splits a string by commas, but only at the top level (not inside parentheses).
func splitTopLevel(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	parts = append(parts, strings.TrimSpace(s[start:]))
	return parts
}

// parseRowFields parses "name1 TYPE1, name2 TYPE2, ..." into Column definitions.
func parseRowFields(s string) ([]Column, error) {
	parts := splitTopLevel(s)
	fields := make([]Column, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		// Split into name and type — first token is the name, rest is the type
		spaceIdx := strings.IndexByte(part, ' ')
		if spaceIdx < 0 {
			return nil, fmt.Errorf("ROW field must have name and type: %q", part)
		}
		// Lower-cased for the reason every other column name is: the schema
		// is compared, projected and JSON-round-tripped by name, and a ROW
		// declared through one door must be byte-identical to the same ROW
		// declared through another (#675).
		fieldName := strings.ToLower(strings.TrimSpace(part[:spaceIdx]))
		fieldType := strings.TrimSpace(part[spaceIdx+1:])
		col, err := ResolveColumn(fieldName, fieldType)
		if err != nil {
			return nil, fmt.Errorf("invalid ROW field %q: %w", fieldName, err)
		}
		fields = append(fields, col)
	}
	return fields, nil
}

// Column defines a column in a Parquet schema.
type Column struct {
	Name        string   `json:"name"`
	Type        TypeID   `json:"type"`
	Nullable    bool     `json:"nullable"`
	Precision   int      `json:"precision,omitempty"`    // for DECIMAL: max digits (1-38)
	Scale       int      `json:"scale,omitempty"`        // for DECIMAL: digits after decimal point
	Dimension   int      `json:"dimension,omitempty"`    // for VECTOR: number of float32 elements
	ElementType *Column  `json:"element_type,omitempty"` // for ARRAY: element column definition
	Fields      []Column `json:"fields,omitempty"`       // for ROW/MAP: child field definitions
}

// Field returns c's named child field. It answers only for a ROW column:
// that is the one shape a field PATH (`rw.c`) can name, and it is the test
// the runtime resolver makes too (expr.ColRef.resolveSlow requires the
// parent vector to be TypeRow). MAP reuses ElementType for its entry ROW and
// ARRAY has no names at all, so neither is addressable this way.
//
// The match is case-insensitive, like every other column-name resolution in
// the planner. A field path whose type is not resolved here is declared
// STRING all the way down — the projection's output vector, the sort
// comparator and the wire OID with it (#568).
func (c *Column) Field(name string) (Column, bool) {
	if c == nil || c.Type != TypeRow {
		return Column{}, false
	}
	for i := range c.Fields {
		if strings.EqualFold(c.Fields[i].Name, name) {
			return c.Fields[i], true
		}
	}
	return Column{}, false
}

// Schema defines the schema for a Parquet file.
type Schema struct {
	Columns []Column `json:"columns"`
}

// ColumnIndex returns the index of a named column, or -1 if not found.
func (s *Schema) ColumnIndex(name string) int {
	for i, c := range s.Columns {
		if c.Name == name {
			return i
		}
	}
	return -1
}

// HasColumn returns true if the schema contains a column with the given name.
func (s *Schema) HasColumn(name string) bool {
	return s.ColumnIndex(name) >= 0
}

// HasNestedColumns returns true if any column is a nested type (ARRAY, ROW, MAP).
func (s *Schema) HasNestedColumns() bool {
	for _, c := range s.Columns {
		if c.Type == TypeArray || c.Type == TypeRow || c.Type == TypeMap {
			return true
		}
	}
	return false
}

// FlatColumns returns the leaf-level columns, flattening any nested types.
// Useful for code paths that need flat column names (e.g., projection).
func (s *Schema) FlatColumns() []Column {
	var flat []Column
	for _, c := range s.Columns {
		flattenColumn(c, &flat)
	}
	return flat
}

func flattenColumn(c Column, out *[]Column) {
	switch c.Type {
	case TypeArray, TypeMap:
		if c.ElementType != nil {
			flattenColumn(*c.ElementType, out)
		}
	case TypeRow:
		for _, f := range c.Fields {
			flattenColumn(f, out)
		}
	default:
		*out = append(*out, c)
	}
}

// ColumnNames returns the names of all columns.
func (s *Schema) ColumnNames() []string {
	names := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		names[i] = c.Name
	}
	return names
}
