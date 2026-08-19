package pgwire

import (
	"strings"
	"testing"
)

// Issue #341 made an unresolvable function name a query error instead of a
// column of NULLs. That silence was load-bearing here: BI clients send
// pg_catalog functions this engine does not implement, and some of them
// "worked" only because an unknown function answered NULL.
//
// Most pg_catalog functions never reach the expression compiler — the
// synthetic-answer path in server.go and catalog_rows.go intercepts them on
// statement text — so they are unaffected either way. The statements below are
// the ones that DO reach it: a function called in a FROM-less SELECT, or over a
// real table, where no intercept applies. Every one of them answered NULL
// before #341 and would have started erroring without a compat shim.
//
// This test is the guard on that decision. It is not a style check: dropping a
// shim from internal/engine/expr/pgcompat.go turns the corresponding statement
// from "answers" into "connection-time error" for a real client, and this is
// where that shows up.
func TestIntrospectionFunctionsAnswer(t *testing.T) {
	_, srv := setupRealDB(t)
	db := openPQ(t, srv.Addr())

	// Statements that must SUCCEED. The shimmed function is named alongside so
	// a failure here points straight at the registration that went missing.
	answered := []struct{ fn, sql string }{
		{"current_setting", `SELECT current_setting('server_version_num')`},
		{"current_setting", `SELECT current_setting('search_path')`},
		{"current_setting", `SELECT current_setting('TimeZone')`},
		{"set_config", `SELECT set_config('search_path', 'public', false)`},
		{"pg_backend_pid", `SELECT pg_backend_pid()`},
		{"pg_is_in_recovery", `SELECT pg_is_in_recovery()`},
		{"txid_current", `SELECT txid_current()`},
		{"pg_client_encoding", `SELECT pg_client_encoding()`},
		{"pg_encoding_to_char", `SELECT pg_encoding_to_char(6)`},
		{"pg_conf_load_time", `SELECT pg_conf_load_time()`},
		{"pg_my_temp_schema", `SELECT pg_my_temp_schema()`},
		{"inet_server_addr", `SELECT inet_server_addr()`},
		{"current_query", `SELECT current_query()`},
		{"quote_ident", `SELECT quote_ident('users')`},
		{"quote_literal", `SELECT quote_literal('abc')`},
		{"array_to_string", `SELECT array_to_string(ARRAY['a','b'], ',')`},
		{"has_table_privilege", `SELECT has_table_privilege('users', 'SELECT')`},
		{"has_schema_privilege", `SELECT has_schema_privilege('public','USAGE')`},
		{"has_database_privilege", `SELECT has_database_privilege('wadjet','CONNECT')`},
		{"has_column_privilege", `SELECT has_column_privilege('users','id','SELECT')`},
		{"has_function_privilege", `SELECT has_function_privilege(1,'EXECUTE')`},
		{"pg_type_is_visible", `SELECT pg_type_is_visible(1)`},
		{"pg_function_is_visible", `SELECT pg_function_is_visible(1)`},
		{"pg_opclass_is_visible", `SELECT pg_opclass_is_visible(1)`},
		{"col_description", `SELECT col_description(1, 1)`},
		{"shobj_description", `SELECT shobj_description(1, 'pg_database')`},

		// These reach the synthetic path rather than the compiler. They are
		// here so that a change to the intercept which accidentally routes
		// them at the engine fails as a pgwire test rather than in a client.
		{"intercepted", `SELECT version()`},
		{"intercepted", `SELECT current_schema()`},
		{"intercepted", `SELECT current_schemas(true)`},
		{"intercepted", `SELECT pg_postmaster_start_time()`},
		{"intercepted", `SELECT pg_size_pretty(pg_database_size(current_database()))`},
		{"intercepted", `SELECT c.relname, obj_description(c.oid, 'pg_class') FROM pg_catalog.pg_class c`},
		{"intercepted", `SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod) FROM pg_catalog.pg_attribute a WHERE a.attnum > 0`},
		{"intercepted", `select relname from pg_class c join pg_namespace n on n.oid=c.relnamespace where pg_catalog.pg_table_is_visible(c.oid) and relkind in ('r','v')`},
	}
	for _, c := range answered {
		rows, err := db.Query(c.sql)
		if err != nil {
			t.Errorf("[%s] %s\n  must answer, got: %v", c.fn, c.sql, err)
			continue
		}
		for rows.Next() {
		}
		if err := rows.Err(); err != nil {
			t.Errorf("[%s] %s\n  must answer, got: %v", c.fn, c.sql, err)
		}
		rows.Close()
	}
}

// TestUnshimmedIntrospectionReportsTheName is the other half of the #341
// decision, pinned at the wire. These pg_catalog functions are deliberately not
// implemented: each needs catalog or storage state a scalar function cannot
// reach, and a plausible-looking wrong answer is worse than a named error.
// to_regclass is the clearest case — it returns NULL for "no such table", so a
// shim that always returned NULL would report every table as missing.
//
// The requirement is not merely that they fail. It is that the failure NAMES
// the function, because that is the whole point of the issue: before the fix
// these returned NULL and a user had no indication which function was missing.
func TestUnshimmedIntrospectionReportsTheName(t *testing.T) {
	_, srv := setupRealDB(t)
	db := openPQ(t, srv.Addr())

	for _, fn := range []string{
		"pg_get_functiondef", "pg_get_partkeydef", "pg_relation_size",
		"pg_total_relation_size", "to_regclass", "pg_sleep",
	} {
		sql := "SELECT " + fn + "(1)"
		rows, err := db.Query(sql)
		if err == nil {
			for rows.Next() {
			}
			err = rows.Err()
			rows.Close()
		}
		if err == nil {
			t.Errorf("%s: expected an error naming the function, got success", sql)
			continue
		}
		if !strings.Contains(err.Error(), fn) {
			t.Errorf("%s: error must name %q, got %q", sql, fn, err.Error())
		}
	}
}

// TestUnknownFunctionOverTheWire: the issue's headline case, seen the way a
// user sees it. Before the fix this returned a column of NULLs.
func TestUnknownFunctionOverTheWire(t *testing.T) {
	_, srv := setupRealDB(t)
	db := openPQ(t, srv.Addr())

	rows, err := db.Query(`SELECT no_such_function(name) FROM users`)
	if err == nil {
		n := 0
		for rows.Next() {
			n++
		}
		err = rows.Err()
		rows.Close()
		if err == nil {
			t.Fatalf("expected an error, got %d rows", n)
		}
	}
	if !strings.Contains(err.Error(), "no_such_function") {
		t.Errorf("error must name the function, got %q", err.Error())
	}
}
