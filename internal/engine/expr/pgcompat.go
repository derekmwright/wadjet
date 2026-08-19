package expr

import (
	"os"
	"strings"
)

// PostgreSQL-compatibility scalar functions, plus the handful of common
// spellings the registry was missing.
//
// These exist because of what the #341 inventory found. Erroring on an
// unresolvable function name is the fix, but the silence it replaces was
// load-bearing on the pgwire introspection path: a probe that fired 75 real
// DataGrip / psql / SQLAlchemy / Superset statements at the server found 29
// distinct function names that reached the expression compiler and answered
// NULL. Most pg_catalog functions never get that far — the synthetic-answer
// path in internal/server/pgwire intercepts them on statement text, which is
// why format_type, pg_get_expr, pg_table_is_visible, pg_get_userbyid,
// pg_get_viewdef, pg_get_constraintdef, pg_get_indexdef, pg_size_pretty and
// pg_database_size are absent below. The ones here are the residue: the
// functions a client calls in a FROM-less SELECT, or over a real table, where
// no intercept applies.
//
// Each one answers what a single-node engine with no roles, no tablespaces and
// no per-object grants can honestly answer. Where PostgreSQL's answer depends
// on state Wadjet does not keep, the shim returns the value that keeps a client
// moving (privileges: granted; visibility: visible; comments: none) rather than
// a fabricated one.
//
// Deliberately NOT shimmed, so they now error: pg_get_functiondef,
// pg_get_partkeydef, pg_get_serial_sequence, pg_relation_size,
// pg_total_relation_size, pg_tablespace_location, to_regclass, pg_sleep. Each
// needs catalog or storage state a scalar function cannot reach from here, and
// a plausible-looking wrong answer from one of them is worse than a named
// error — to_regclass in particular returns NULL for "no such table", so a
// shim that always returned NULL would report every table as missing.
func init() {
	pgBuiltins := map[string]builtin{
		// --- Session and configuration ---
		// current_setting is the GUC reader every JDBC/psycopg client calls
		// during connection setup. PostgreSQL raises for an unknown setting in
		// the one-argument form; we return empty, matching what matchShow's
		// default arm already answers over the wire.
		"current_setting": {fnCurrentSetting, RetString},
		// set_config returns the value it set. Nothing here is settable, so it
		// echoes the requested value: a client that sets search_path and reads
		// the result back sees what it asked for.
		"set_config":          {fnSetConfig, RetString},
		"pg_backend_pid":      {fnPgBackendPID, RetInt64},
		"pg_client_encoding":  {fnPgClientEncoding, RetString},
		"pg_encoding_to_char": {fnPgEncodingToChar, RetString},
		"pg_is_in_recovery":   {fnFalse, RetBool},
		"pg_my_temp_schema":   {fnZero, RetInt64},
		"pg_conf_load_time":   {fnPgConfLoadTime, RetString},
		"inet_server_addr":    {fnNull, RetString},
		"inet_client_addr":    {fnNull, RetString},
		"current_query":       {fnNull, RetString},
		"txid_current":        {fnOne, RetInt64},
		"pg_current_xact_id":  {fnOne, RetInt64},

		// --- Privileges. No per-object grants exist at this layer, so every
		// check a connected session makes is granted. Answering false would
		// make a BI client hide tables the user can plainly query. ---
		"has_table_privilege":      {fnTrue, RetBool},
		"has_schema_privilege":     {fnTrue, RetBool},
		"has_database_privilege":   {fnTrue, RetBool},
		"has_column_privilege":     {fnTrue, RetBool},
		"has_function_privilege":   {fnTrue, RetBool},
		"has_sequence_privilege":   {fnTrue, RetBool},
		"has_tablespace_privilege": {fnTrue, RetBool},
		"pg_has_role":              {fnTrue, RetBool},

		// --- Search-path visibility. There is one schema, so everything in
		// the catalog is visible from it. ---
		"pg_table_is_visible":     {fnTrue, RetBool},
		"pg_type_is_visible":      {fnTrue, RetBool},
		"pg_function_is_visible":  {fnTrue, RetBool},
		"pg_opclass_is_visible":   {fnTrue, RetBool},
		"pg_operator_is_visible":  {fnTrue, RetBool},
		"pg_collation_is_visible": {fnTrue, RetBool},

		// --- Object comments. Wadjet stores none; NULL is PostgreSQL's own
		// answer for an object without one, so this is exact rather than a
		// stand-in. ---
		"obj_description":   {fnNull, RetString},
		"col_description":   {fnNull, RetString},
		"shobj_description": {fnNull, RetString},

		// --- Quoting and array rendering. Real implementations: these are
		// ordinary string functions that clients and users both call. ---
		"quote_ident":     {fnQuoteIdent, RetString},
		"quote_literal":   {fnQuoteLiteral, RetString},
		"quote_nullable":  {fnQuoteNullable, RetString},
		"array_to_string": {fnArrayToString, RetString},

		// --- Common spellings the registry lacked. Every one of these is
		// named in issue #341 or in the funcReturnType comment (#310) as a
		// function "registered nowhere" that therefore answered NULL. ---
		//
		// date_part is EXTRACT's function spelling — same argument order, same
		// unit vocabulary — so it shares fnExtract and vecExtract outright.
		"date_part": {fnExtract, RetFloat64},
		"ceiling":   {fnCeil, RetFloat64},
		"trunc":     {fnTruncate, RetFloat64},
		"strlen":    {fnLength, RetFloat64},
		"ascii":     {fnASCII, RetFloat64},
	}
	for name, b := range pgBuiltins {
		DefaultRegistry.Register(name, b.fn, b.ret)
	}

	// date_part reads its second argument as an instant, exactly as extract
	// does. Without this it would answer 1970 for every DATE column, which is
	// the #319 bug it would otherwise reintroduce under a new name.
	temporalInputFuncs["date_part"] = true
	stringInputFuncs["ascii"] = true

	DefaultRegistry.RegisterVec("date_part", vecExtract)
	DefaultRegistry.RegisterVec("ceiling", vecCeil)
}

// pgSettings is the GUC vocabulary current_setting answers from. It mirrors the
// values matchShow and the startup ParameterStatus messages already report in
// internal/server/pgwire, so a client that asks the same question two ways gets
// one answer.
var pgSettings = map[string]string{
	"server_version":                "15.0",
	"server_version_num":            "150000",
	"server_encoding":               "UTF8",
	"client_encoding":               "UTF8",
	"datestyle":                     "ISO, MDY",
	"timezone":                      "UTC",
	"standard_conforming_strings":   "on",
	"integer_datetimes":             "on",
	"intervalstyle":                 "postgres",
	"search_path":                   "public",
	"application_name":              "",
	"transaction_isolation":         "read committed",
	"transaction_read_only":         "off",
	"is_superuser":                  "on",
	"max_index_keys":                "32",
	"bytea_output":                  "hex",
	"in_hot_standby":                "off",
	"default_transaction_isolation": "read committed",
}

func fnCurrentSetting(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	if v, ok := pgSettings[strings.ToLower(toString(args[0]))]; ok {
		return v
	}
	return ""
}

func fnSetConfig(args []any) any {
	if len(args) < 2 || args[1] == nil {
		return ""
	}
	return toString(args[1])
}

func fnPgBackendPID(args []any) any { return int64(os.Getpid()) }

func fnPgClientEncoding(args []any) any { return "UTF8" }

// fnPgEncodingToChar maps PostgreSQL's encoding ids. 6 is UTF8, which is the
// only encoding this server speaks, so every other id is unknown rather than
// guessed at.
func fnPgEncodingToChar(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	if int(ToFloat64(args[0])) == 6 {
		return "UTF8"
	}
	return ""
}

// fnPgConfLoadTime answers from processStart (expr.go, shared with
// pg_postmaster_start_time): there is no configuration file to reload, so the
// last time settings took effect is when the process came up.
func fnPgConfLoadTime(args []any) any {
	return processStart.UTC().Format("2006-01-02 15:04:05Z07:00")
}

func fnTrue(args []any) any  { return true }
func fnFalse(args []any) any { return false }
func fnNull(args []any) any  { return nil }
func fnZero(args []any) any  { return int64(0) }
func fnOne(args []any) any   { return int64(1) }

// fnQuoteIdent double-quotes an identifier only when it needs it: an
// identifier that is already lowercase, alphanumeric and does not lead with a
// digit is returned bare, which is what PostgreSQL does.
func fnQuoteIdent(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	if s != "" && !needsQuoting(s) {
		return s
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func needsQuoting(s string) bool {
	if s[0] >= '0' && s[0] <= '9' {
		return true
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' {
			continue
		}
		return true
	}
	return false
}

func fnQuoteLiteral(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	return quoteLiteral(toString(args[0]))
}

// fnQuoteNullable differs from quote_literal in exactly one case, which is the
// reason it exists: a NULL input renders as the unquoted token NULL.
func fnQuoteNullable(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return "NULL"
	}
	return quoteLiteral(toString(args[0]))
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// fnArrayToString joins an array with a separator, dropping NULL elements
// unless a third argument supplies their replacement text — PostgreSQL's rule.
func fnArrayToString(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	sep := toString(args[1])
	var nullText string
	haveNullText := len(args) >= 3 && args[2] != nil
	if haveNullText {
		nullText = toString(args[2])
	}
	elems, ok := args[0].([]any)
	if !ok {
		return toString(args[0])
	}
	parts := make([]string, 0, len(elems))
	for _, e := range elems {
		if e == nil {
			if haveNullText {
				parts = append(parts, nullText)
			}
			continue
		}
		parts = append(parts, toString(e))
	}
	return strings.Join(parts, sep)
}

// fnASCII returns the code point of the first character. PostgreSQL returns the
// UTF-8 code point, not the byte, so a multi-byte leading character answers
// its rune value.
func fnASCII(args []any) any {
	if len(args) < 1 || args[0] == nil {
		return nil
	}
	s := toString(args[0])
	if s == "" {
		return float64(0)
	}
	for _, r := range s {
		return float64(r)
	}
	return float64(0)
}
