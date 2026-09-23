// SPDX-License-Identifier: MIT

package expr

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/planner/syscatalog"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The pg_catalog functions a catalog query calls: format_type(),
// pg_get_userbyid(), the reg* type conversions and the pg_get_*def family.
//
// They split in two. The STATIC ones answer from facts that do not depend on
// the storage catalog — a type's name, a namespace's name, a NULL for an
// object kind this server has none of — and are ordinary registered
// functions. The CATALOG ones answer about relations and roles, which are the
// caller's view of the storage catalog (syscatalog.Access), so they are bound
// at compile time to a CatalogResolver the planner supplies for the statement
// (WithCatalog). A path that compiles without one — a stage-DAG worker, a
// DML predicate — refuses them by name rather than answering from nothing.

// CatalogResolver answers the catalog questions a compiled expression asks,
// for one statement and one identity.
type CatalogResolver interface {
	// RelationName renders a relation OID as regclass's output does: the
	// name, schema-qualified when its schema is not on the search path, or
	// ok=false for an OID that names no relation this identity can see.
	RelationName(oid int64) (string, bool)
	// RelationOID reads a relation name as regclass's input does, or
	// ok=false when no relation of that name is visible.
	RelationOID(name string) (int64, bool)
	// RelationKind is pg_class.relkind for a visible relation's OID, and
	// whether it is a user table.
	RelationKind(oid int64) (kind string, user bool, ok bool)
	// ColumnExists reports whether a visible relation has the column.
	ColumnExists(oid int64, column string) bool
	// RoleName is the name of a role OID.
	RoleName(oid int64) (string, bool)
}

// WithCatalog binds the catalog functions to r for this compile.
func WithCatalog(r CatalogResolver) CompileOption {
	if r == nil {
		return nil
	}
	return func(c *compileContext) { c.catalog = r }
}

// catalogFuncs are the functions that need a CatalogResolver.
var catalogFuncs = map[string]func(r CatalogResolver, args []any) any{
	"pg_get_userbyid":            catPgGetUserByID,
	"regclassout":                catRegclassOut,
	"regclassin":                 catRegclassIn,
	"to_regclass":                catToRegclass,
	"regrolein":                  catRegroleIn,
	"regroleout":                 catRegroleOut,
	"pg_relation_is_publishable": catRelationIsPublishable,
	"pg_get_serial_sequence":     catGetSerialSequence,
}

func init() {
	for name := range catalogFuncs {
		name := name
		DefaultRegistry.Register(name, func([]any) any {
			panic(fatalEval{catalogRefusal(name)})
		}, catalogFuncReturn[name])
	}
	static := map[string]builtin{
		"format_type":                     {fnFormatType, RetString},
		"regtypeout":                      {fnRegtypeOut, RetString},
		"regtypein":                       {fnRegtypeIn, RetInt64},
		"regnamespaceout":                 {fnRegnamespaceOut, RetString},
		"regnamespacein":                  {fnRegnamespaceIn, RetInt64},
		"regprocout":                      {fnRegprocOut, RetString},
		"pg_get_expr":                     {fnPgGetExpr, RetString},
		"pg_get_constraintdef":            {fnNullForUnknownObject, RetString},
		"pg_get_indexdef":                 {fnNullForUnknownObject, RetString},
		"pg_get_statisticsobjdef_columns": {fnNullForUnknownObject, RetString},
		"pg_get_function_result":          {fnNullForUnknownObject, RetString},
		"pg_get_function_arguments":       {fnNullForUnknownObject, RetString},
		"pg_get_triggerdef":               {fnNullForUnknownObject, RetString},
		"pg_get_ruledef":                  {fnNullForUnknownObject, RetString},
		"pg_get_viewdef":                  {fnNullForUnknownObject, RetString},
		"json_build_object":               {fnJSONBuildObject, RetString},
		"array_upper":                     {fnArrayUpper, RetInt32},
		"array_lower":                     {fnArrayLower, RetInt32},
	}
	for name, b := range static {
		DefaultRegistry.Register(name, b.fn, b.ret)
	}
	stringInputFuncs["regclassin"] = true
	stringInputFuncs["to_regclass"] = true
	stringInputFuncs["regtypein"] = true
	stringInputFuncs["regnamespacein"] = true
	stringInputFuncs["regrolein"] = true
}

var catalogFuncReturn = map[string]Ret{
	"pg_get_userbyid":            RetString,
	"regclassout":                RetString,
	"regclassin":                 RetInt64,
	"to_regclass":                RetInt64,
	"regrolein":                  RetInt64,
	"regroleout":                 RetString,
	"pg_relation_is_publishable": RetBool,
	"pg_get_serial_sequence":     RetString,
}

func catalogRefusal(name string) error {
	return sqlerr.New("0A000",
		"%s() reads the system catalog, which is not available on this execution path", name)
}

// catalogCall is a catalog function bound to its resolver.
type catalogCall struct {
	name string
	fn   func(CatalogResolver, []any) any
	r    CatalogResolver
	args []Expr
}

func (c *catalogCall) Eval(b *batch.RecordBatch, row int) any {
	vals := make([]any, len(c.args))
	for i, a := range c.args {
		vals[i] = a.Eval(b, row)
	}
	return c.fn(c.r, vals)
}

// compileCatalogCall binds a catalog function, or refuses it by name on a
// path with no catalog.
func compileCatalogCall(name string, args []Expr, ctx *compileContext) (Expr, bool, error) {
	fn, ok := catalogFuncs[name]
	if !ok {
		return nil, false, nil
	}
	if ctx.catalog == nil {
		return nil, true, catalogRefusal(name)
	}
	return &catalogCall{name: name, fn: fn, r: ctx.catalog, args: args}, true, nil
}

func oidArg(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int32:
		return int64(x), true
	case int:
		return int64(x), true
	case float64:
		if x == math.Trunc(x) {
			return int64(x), true
		}
	case string:
		n, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		return n, err == nil
	}
	return 0, false
}

// pg_get_userbyid(oid): the role's name, or PostgreSQL's own rendering of an
// OID that names none.
func catPgGetUserByID(r CatalogResolver, args []any) any {
	if len(args) != 1 || args[0] == nil {
		return nil
	}
	oid, ok := oidArg(args[0])
	if !ok {
		return nil
	}
	if name, ok := r.RoleName(oid); ok {
		return name
	}
	return fmt.Sprintf("unknown (OID=%d)", oid)
}

// regclassout: the output of oid::regclass — the relation's name, "-" for
// 0, and the number itself for an OID that names no relation.
func catRegclassOut(r CatalogResolver, args []any) any {
	if len(args) != 1 || args[0] == nil {
		return nil
	}
	oid, ok := oidArg(args[0])
	if !ok {
		return nil
	}
	if oid == 0 {
		return "-"
	}
	if name, ok := r.RelationName(oid); ok {
		return name
	}
	return strconv.FormatInt(oid, 10)
}

// regclassin: 'name'::regclass — the OID, or PostgreSQL's 42P01.
func catRegclassIn(r CatalogResolver, args []any) any {
	if len(args) != 1 || args[0] == nil {
		return nil
	}
	s := strings.TrimSpace(toString(args[0]))
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	if s == "-" {
		return int64(0)
	}
	if oid, ok := r.RelationOID(s); ok {
		return oid
	}
	panic(fatalEval{sqlerr.New("42P01", "relation %q does not exist", unquoteRelationName(s))})
}

// to_regclass(text): regclassin that answers NULL where regclassin raises.
func catToRegclass(r CatalogResolver, args []any) any {
	if len(args) != 1 || args[0] == nil {
		return nil
	}
	if oid, ok := r.RelationOID(strings.TrimSpace(toString(args[0]))); ok {
		return oid
	}
	return nil
}

func catRegroleOut(r CatalogResolver, args []any) any {
	if len(args) != 1 || args[0] == nil {
		return nil
	}
	oid, ok := oidArg(args[0])
	if !ok {
		return nil
	}
	if oid == 0 {
		return "-"
	}
	if name, ok := r.RoleName(oid); ok {
		return name
	}
	return strconv.FormatInt(oid, 10)
}

func catRegroleIn(r CatalogResolver, args []any) any {
	if len(args) != 1 || args[0] == nil {
		return nil
	}
	s := strings.TrimSpace(toString(args[0]))
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	if name, ok := r.RoleName(syscatalog.BootstrapRoleOID); ok && name == unquoteRelationName(s) {
		return syscatalog.BootstrapRoleOID
	}
	panic(fatalEval{sqlerr.New("42704", "role %q does not exist", unquoteRelationName(s))})
}

// pg_relation_is_publishable(regclass): true for a user table, false for a
// system relation, NULL for an OID that names no relation.
func catRelationIsPublishable(r CatalogResolver, args []any) any {
	if len(args) != 1 || args[0] == nil {
		return nil
	}
	oid, ok := oidArg(args[0])
	if !ok {
		if s, isStr := args[0].(string); isStr {
			oid, ok = r.RelationOID(strings.TrimSpace(s))
		}
		if !ok {
			return nil
		}
	}
	kind, user, ok := r.RelationKind(oid)
	if !ok {
		return nil
	}
	return user && kind == "r"
}

// pg_get_serial_sequence(table, column): NULL — this server has no
// sequences — for a column that exists, and PostgreSQL's refusals for a
// relation or column that does not.
func catGetSerialSequence(r CatalogResolver, args []any) any {
	if len(args) != 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	table := strings.TrimSpace(toString(args[0]))
	oid, ok := r.RelationOID(table)
	if !ok {
		panic(fatalEval{sqlerr.New("42P01", "relation %q does not exist", unquoteRelationName(table))})
	}
	col := toString(args[1])
	if !r.ColumnExists(oid, col) {
		panic(fatalEval{sqlerr.New("42703", "column %q of relation %q does not exist",
			col, unquoteRelationName(table))})
	}
	return nil
}

// unquoteRelationName is the name as PostgreSQL echoes it in its refusal:
// the last component, unquoted.
func unquoteRelationName(s string) string {
	parts := SplitQualifiedName(s)
	if len(parts) == 0 {
		return s
	}
	return parts[len(parts)-1]
}

// SplitQualifiedName splits a possibly-qualified, possibly-quoted relation
// name the way regclass input reads one: dot-separated parts, an unquoted
// part folded to lower case, a quoted part verbatim with "" unescaped. It
// returns nil for a malformed name.
func SplitQualifiedName(s string) []string {
	var parts []string
	i := 0
	for i <= len(s) {
		if i < len(s) && s[i] == '"' {
			var b strings.Builder
			j := i + 1
			for {
				if j >= len(s) {
					return nil
				}
				if s[j] == '"' {
					if j+1 < len(s) && s[j+1] == '"' {
						b.WriteByte('"')
						j += 2
						continue
					}
					break
				}
				b.WriteByte(s[j])
				j++
			}
			parts = append(parts, b.String())
			i = j + 1
		} else {
			j := strings.IndexByte(s[i:], '.')
			end := len(s)
			if j >= 0 {
				end = i + j
			}
			part := strings.TrimSpace(s[i:end])
			if part == "" {
				return nil
			}
			parts = append(parts, strings.ToLower(part))
			i = end
		}
		if i >= len(s) {
			break
		}
		if s[i] != '.' {
			return nil
		}
		i++
		if i == len(s) {
			return nil
		}
	}
	return parts
}

// format_type(type oid, typmod integer): the SQL name of a type, with its
// modifier where the type takes one. An OID this catalog does not list is
// "???", PostgreSQL's own rendering of one it cannot find.
func fnFormatType(args []any) any {
	if len(args) != 2 || args[0] == nil {
		return nil
	}
	oid, ok := oidArg(args[0])
	if !ok {
		return nil
	}
	_, sql, found := syscatalog.TypeName(oid)
	if !found {
		return "???"
	}
	if args[1] == nil {
		return sql
	}
	mod, ok := oidArg(args[1])
	if !ok || mod < 0 {
		return sql
	}
	switch oid {
	case 1700: // numeric(p,s)
		if mod >= 4 {
			return fmt.Sprintf("numeric(%d,%d)", (mod-4)>>16, (mod-4)&0xFFFF)
		}
	case 1231: // numeric(p,s)[]: an array's modifier is its element's
		if mod >= 4 {
			return fmt.Sprintf("numeric(%d,%d)[]", (mod-4)>>16, (mod-4)&0xFFFF)
		}
	case 1043, 1042: // character varying(n), character(n)
		if mod >= 4 {
			return fmt.Sprintf("%s(%d)", sql, mod-4)
		}
	}
	return sql
}

func fnRegtypeOut(args []any) any {
	if len(args) != 1 || args[0] == nil {
		return nil
	}
	oid, ok := oidArg(args[0])
	if !ok {
		return nil
	}
	if oid == 0 {
		return "-"
	}
	if _, sql, found := syscatalog.TypeName(oid); found {
		return sql
	}
	return strconv.FormatInt(oid, 10)
}

func fnRegtypeIn(args []any) any {
	if len(args) != 1 || args[0] == nil {
		return nil
	}
	s := strings.ToLower(strings.TrimSpace(toString(args[0])))
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	s = strings.TrimPrefix(s, "pg_catalog.")
	for _, row := range syscatalog.PgTypeRows() {
		oid, _ := strconv.ParseInt(row["oid"], 10, 64)
		_, sql, _ := syscatalog.TypeName(oid)
		if row["typname"] == s || sql == s {
			return oid
		}
	}
	panic(fatalEval{sqlerr.New("42704", "type %q does not exist", toString(args[0]))})
}

var namespaceNames = map[int64]string{
	syscatalog.NamespacePgCatalog:         syscatalog.SchemaPgCatalog,
	syscatalog.NamespacePublic:            "public",
	syscatalog.NamespaceInformationSchema: syscatalog.SchemaInformationSchema,
}

func fnRegnamespaceOut(args []any) any {
	if len(args) != 1 || args[0] == nil {
		return nil
	}
	oid, ok := oidArg(args[0])
	if !ok {
		return nil
	}
	if oid == 0 {
		return "-"
	}
	if name, found := namespaceNames[oid]; found {
		return name
	}
	return strconv.FormatInt(oid, 10)
}

func fnRegnamespaceIn(args []any) any {
	if len(args) != 1 || args[0] == nil {
		return nil
	}
	s := strings.TrimSpace(toString(args[0]))
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	name := unquoteRelationName(s)
	for oid, n := range namespaceNames {
		if n == name {
			return oid
		}
	}
	panic(fatalEval{sqlerr.New("3F000", "schema %q does not exist", name)})
}

// regprocout: this server lists no functions in pg_proc, so an OID renders
// as itself and 0 as "-".
func fnRegprocOut(args []any) any {
	if len(args) != 1 || args[0] == nil {
		return nil
	}
	oid, ok := oidArg(args[0])
	if !ok {
		return nil
	}
	if oid == 0 {
		return "-"
	}
	return strconv.FormatInt(oid, 10)
}

// pg_get_expr(pg_node_tree, relation oid [, pretty]): this server stores no
// expression trees — every pg_node_tree column in its catalog is NULL — so
// the only argument that can reach it is NULL, and its answer is NULL. Any
// other value is not a pg_node_tree, which is PostgreSQL's own refusal.
func fnPgGetExpr(args []any) any {
	if len(args) < 2 || args[0] == nil {
		return nil
	}
	panic(fatalEval{sqlerr.New("42804", "cannot accept a value of type pg_node_tree")})
}

// The pg_get_*def family over an object OID this server has none of —
// constraints, indexes, statistics objects, functions, triggers, rules and
// views: NULL, which is what PostgreSQL answers for an OID that names none.
func fnNullForUnknownObject(args []any) any { return nil }

// json_build_object(variadic "any"): a JSON object of alternating keys and
// values, rendered the way PostgreSQL renders one — `{"a" : 1, "b" : "x"}`.
func fnJSONBuildObject(args []any) any {
	if len(args)%2 != 0 {
		panic(fatalEval{sqlerr.New("22023",
			"argument list must have even number of elements")})
	}
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i < len(args); i += 2 {
		if args[i] == nil {
			panic(fatalEval{sqlerr.New("22004", "null value not allowed for object key")})
		}
		if i > 0 {
			b.WriteString(", ")
		}
		k, _ := json.Marshal(toString(args[i]))
		b.Write(k)
		b.WriteString(" : ")
		b.WriteString(jsonValue(args[i+1]))
	}
	b.WriteByte('}')
	return b.String()
}

func jsonValue(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case bool:
		return strconv.FormatBool(x)
	case int64, int32, int:
		return fmt.Sprint(x)
	case float64:
		if math.IsInf(x, 0) || math.IsNaN(x) {
			q, _ := json.Marshal(strconv.FormatFloat(x, 'g', -1, 64))
			return string(q)
		}
		return strconv.FormatFloat(x, 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = jsonValue(e)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, len(keys))
		for i, k := range keys {
			q, _ := json.Marshal(k)
			parts[i] = string(q) + " : " + jsonValue(x[k])
		}
		return "{" + strings.Join(parts, ", ") + "}"
	}
	q, _ := json.Marshal(toString(v))
	return string(q)
}

// array_upper / array_lower (anyarray, dimension): the bounds of a
// one-dimensional array, whose lower bound is 1. An empty array, a NULL
// array and a dimension the array does not have are NULL.
func fnArrayUpper(args []any) any { return arrayBound(args, true) }
func fnArrayLower(args []any) any { return arrayBound(args, false) }

func arrayBound(args []any, upper bool) any {
	if len(args) != 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	arr, ok := args[0].([]any)
	if !ok || len(arr) == 0 {
		return nil
	}
	dim, ok := oidArg(args[1])
	if !ok || dim != 1 {
		return nil
	}
	if upper {
		return int32(len(arr))
	}
	return int32(1)
}
