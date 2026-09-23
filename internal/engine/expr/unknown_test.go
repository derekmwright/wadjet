// SPDX-License-Identifier: MIT

package expr

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// Issue #341: an unresolvable function name evaluated to NULL. A typo, a
// PostgreSQL builtin we lack, and an aggregate from another dialect were all
// indistinguishable from a genuinely NULL result — and for the aggregate the
// row COUNT was wrong too, because a name nothing recognizes as an aggregate
// never triggers grouping.

func compileCall(t *testing.T, name string, args ...plansql.Node) error {
	t.Helper()
	_, err := Compile(&plansql.FuncCallNode{Name: name, Args: args})
	return err
}

// TestUnknownScalarErrors is the headline case from the issue.
func TestUnknownScalarErrors(t *testing.T) {
	err := compileCall(t, "no_such_function", &plansql.ColRef{Column: "n_name"})
	if err == nil {
		t.Fatal("compiling no_such_function must fail, got nil error")
	}
	if !IsUnknownFunc(err) {
		t.Errorf("error must be an UnknownFuncError, got %T: %v", err, err)
	}
	// The name is the whole point of the error: it is what tells a user
	// porting a dashboard WHICH function is missing.
	if !strings.Contains(err.Error(), "no_such_function") {
		t.Errorf("error must name the function, got %q", err.Error())
	}
}

// TestUnknownAggregateErrors covers the worse half of the bug. array_agg and
// count_if are not in plansql.knownAggregates, so the planner never grouped;
// the projection became a scalar that answered NULL per row and the result had
// 25 rows where one was correct.
func TestUnknownAggregateErrors(t *testing.T) {
	for _, name := range []string{"array_agg", "count_if"} {
		err := compileCall(t, name, &plansql.ColRef{Column: "n_name"})
		if err == nil {
			t.Errorf("%s: must fail, got nil error", name)
			continue
		}
		if !IsUnknownFunc(err) {
			t.Errorf("%s: must be an UnknownFuncError, got %T", name, err)
		}
		msg := err.Error()
		if !strings.Contains(msg, name) {
			t.Errorf("%s: error must name the function, got %q", name, msg)
		}
		// An aggregate must be identified as one, or the reader goes hunting
		// for a spelling mistake in a name that is spelled correctly.
		if !strings.Contains(msg, "aggregate") {
			t.Errorf("%s: error must say it is an aggregate, got %q", name, msg)
		}
	}
}

// TestKnownAggregatesStillCompile: names the planner DOES implement reach the
// compiler in nested and HAVING rewrites, and a direct Compile of count(*) is
// a supported call. None may be rejected.
func TestKnownAggregatesStillCompile(t *testing.T) {
	for _, name := range []string{"sum", "count", "avg", "min", "max", "median"} {
		if err := compileCall(t, name, &plansql.ColRef{Column: "x"}); err != nil {
			t.Errorf("%s(x): must compile, got %v", name, err)
		}
	}
	if _, err := Compile(&plansql.FuncCallNode{Name: "count", Star: true}); err != nil {
		t.Errorf("count(*): must compile, got %v", err)
	}
}

// TestKnownScalarsStillCompile covers resolution itself: mixed case, and the
// alias pairs that share one implementation under two registry entries.
func TestKnownScalarsStillCompile(t *testing.T) {
	// Each call is spelled at an arity its DOCUMENTED signature takes: since
	// #1053 the registry records one, and a call with the wrong count is
	// 42883 before the name check's answer can matter. `substr('abc')` was
	// here as a name probe and is now a refusal on its own merits, which is
	// why the argument lists below differ per function rather than being one
	// literal for all of them.
	cases := []struct {
		name, why string
		args      []plansql.Node
	}{
		{"upper", "plain lowercase", []plansql.Node{&plansql.Lit{Value: "abc"}}},
		{"UPPER", "uppercase — the registry lowercases on both write and read",
			[]plansql.Node{&plansql.Lit{Value: "abc"}}},
		{"UpPeR", "mixed case", []plansql.Node{&plansql.Lit{Value: "abc"}}},
		{"substr", "alias pair with substring", []plansql.Node{
			&plansql.Lit{Value: "abc"}, &plansql.Lit{Value: "1", Kind: plansql.LitNumber}}},
		{"substring", "alias pair with substr", []plansql.Node{
			&plansql.Lit{Value: "abc"}, &plansql.Lit{Value: "1", Kind: plansql.LitNumber}}},
		{"len", "alias pair with length", []plansql.Node{&plansql.Lit{Value: "abc"}}},
		{"length", "alias pair with len", []plansql.Node{&plansql.Lit{Value: "abc"}}},
		{"ucase", "alias of upper", []plansql.Node{&plansql.Lit{Value: "abc"}}},
		{"character_length", "alias of char_length", []plansql.Node{&plansql.Lit{Value: "abc"}}},
	}
	for _, c := range cases {
		if err := compileCall(t, c.name, c.args...); err != nil {
			t.Errorf("%s (%s): must compile, got %v", c.name, c.why, err)
		}
	}
}

// TestIssueFunctionsNowRegistered pins the three names the issue reports as
// answering NULL. The choice made here is to IMPLEMENT them rather than let
// them error: each is an ordinary spelling of a function the engine already
// had (date_part is EXTRACT, ceiling is ceil), or a one-line builtin (ascii).
func TestIssueFunctionsNowRegistered(t *testing.T) {
	// date_part is EXTRACT's function spelling and takes the unit AND the
	// instant, so its probe is two arguments since #1053 gave the registry an
	// arity; the other four take one.
	for _, c := range []struct {
		name string
		args []plansql.Node
	}{
		{"date_part", []plansql.Node{&plansql.Lit{Value: "year"}, &plansql.Lit{Value: "1996-01-10"}}},
		{"ascii", []plansql.Node{&plansql.Lit{Value: "x"}}},
		{"ceiling", []plansql.Node{&plansql.Lit{Value: "1", Kind: plansql.LitNumber}}},
		{"trunc", []plansql.Node{&plansql.Lit{Value: "1", Kind: plansql.LitNumber}}},
		{"strlen", []plansql.Node{&plansql.Lit{Value: "x"}}},
	} {
		if !DefaultRegistry.Has(c.name) {
			t.Errorf("%s must be registered", c.name)
		}
		if err := compileCall(t, c.name, c.args...); err != nil {
			t.Errorf("%s: must compile, got %v", c.name, err)
		}
	}
}

func TestIssueFunctionValues(t *testing.T) {
	// DATE_PART('year', DATE '1996-01-10') → 1996, per the issue's table.
	if got := fnExtract([]any{"year", "1996-01-10"}); got != 1996.0 {
		t.Errorf("date_part year: got %v want 1996", got)
	}
	// ascii('ALGERIA') → 65.
	if got := fnASCII([]any{"ALGERIA"}); got != 65.0 {
		t.Errorf("ascii: got %v want 65", got)
	}
	// ceiling(901.0) → 901, and it must round UP, not truncate.
	if got := fnCeil([]any{901.0}); got != 901.0 {
		t.Errorf("ceiling(901.0): got %v want 901", got)
	}
	if got := fnCeil([]any{900.5}); got != 901.0 {
		t.Errorf("ceiling(900.5): got %v want 901", got)
	}
	// ascii of a multi-byte leading character is its code point, not its
	// first byte: PostgreSQL answers 233 for 'é', never 195.
	if got := fnASCII([]any{"é"}); got != 233.0 {
		t.Errorf("ascii(é): got %v want 233", got)
	}
	if got := fnASCII([]any{""}); got != 0.0 {
		t.Errorf("ascii(empty): got %v want 0", got)
	}
}

// TestStrictFunctionsKillSwitch: WADJET_STRICT_FUNCTIONS=0 restores the old
// silence. It exists for an operator whose BI client sends a pg_catalog
// function the compat shims missed; it is not a way to pass a test.
func TestStrictFunctionsKillSwitch(t *testing.T) {
	prev := strictFunctions
	t.Cleanup(func() { strictFunctions = prev })

	strictFunctions = false
	if err := checkKnown("no_such_function"); err != nil {
		t.Errorf("with the switch off, an unknown name must compile, got %v", err)
	}
	strictFunctions = true
	if err := checkKnown("no_such_function"); err == nil {
		t.Error("with the switch on, an unknown name must fail")
	}
}

// TestNearestFuncSuggestion: a typo one edit from a real function should say
// so. This is the cheapest case to diagnose and the most common.
func TestNearestFuncSuggestion(t *testing.T) {
	err := checkKnown("uppr")
	if err == nil {
		t.Fatal("uppr must fail")
	}
	if !strings.Contains(err.Error(), "upper") {
		t.Errorf("error should suggest upper, got %q", err.Error())
	}
	// A name nothing resembles must not invent a suggestion.
	far := checkKnown("zzzqqqxxxyyy")
	if far == nil {
		t.Fatal("zzzqqqxxxyyy must fail")
	}
	if strings.Contains(far.Error(), "did you mean") {
		t.Errorf("no suggestion should be offered, got %q", far.Error())
	}
}

// TestPgCompatShimsRegistered pins the introspection surface the #341
// inventory found reaching the compiler. Each of these answered NULL before,
// and each would START ERRORING without a shim — that is the blast radius the
// shims exist to absorb, so a later edit that drops one should fail here
// rather than in somebody's BI client.
func TestPgCompatShimsRegistered(t *testing.T) {
	for _, name := range []string{
		"current_setting", "set_config", "pg_backend_pid", "pg_client_encoding",
		"pg_encoding_to_char", "pg_is_in_recovery", "pg_my_temp_schema",
		"pg_conf_load_time", "inet_server_addr", "current_query", "txid_current",
		"has_table_privilege", "has_schema_privilege", "has_database_privilege",
		"has_column_privilege", "has_function_privilege",
		"pg_table_is_visible", "pg_type_is_visible", "pg_function_is_visible",
		"pg_opclass_is_visible",
		"obj_description", "col_description", "shobj_description",
		"quote_ident", "quote_literal", "array_to_string",
		// The catalog functions (pg_catalog_fns.go): answered from the
		// statement's catalog view, so the NULL-for-every-table hazard the
		// unshimmed list below guards is not theirs.
		"to_regclass", "pg_get_serial_sequence", "pg_get_userbyid", "format_type",
	} {
		if !DefaultRegistry.Has(name) {
			t.Errorf("pg compat shim %q is not registered", name)
		}
	}
}

func TestPgCompatValues(t *testing.T) {
	// current_setting answers from the same vocabulary the wire protocol
	// reports at startup, so a client asking twice gets one answer.
	if got := fnCurrentSetting([]any{"server_version_num"}); got != "170000" {
		t.Errorf("current_setting(server_version_num): got %v", got)
	}
	// Case-insensitive: JDBC sends TimeZone, psycopg sends timezone.
	if got := fnCurrentSetting([]any{"TimeZone"}); got != "UTC" {
		t.Errorf("current_setting(TimeZone): got %v", got)
	}
	// An unknown setting is empty, not an error: PostgreSQL raises here, but
	// raising would undo the compatibility this shim exists to provide.
	if got := fnCurrentSetting([]any{"no_such_guc"}); got != "" {
		t.Errorf("current_setting(no_such_guc): got %v want empty", got)
	}
	// quote_ident leaves a plain identifier bare and quotes one that needs it.
	if got := fnQuoteIdent([]any{"users"}); got != "users" {
		t.Errorf("quote_ident(users): got %v", got)
	}
	if got := fnQuoteIdent([]any{"My Table"}); got != `"My Table"` {
		t.Errorf("quote_ident(My Table): got %v", got)
	}
	if got := fnQuoteIdent([]any{`a"b`}); got != `"a""b"` {
		t.Errorf("quote_ident doubles embedded quotes: got %v", got)
	}
	if got := fnQuoteLiteral([]any{"O'Brien"}); got != "'O''Brien'" {
		t.Errorf("quote_literal: got %v", got)
	}
	// quote_nullable differs from quote_literal in exactly one case.
	if got := fnQuoteNullable([]any{nil}); got != "NULL" {
		t.Errorf("quote_nullable(NULL): got %v", got)
	}
	// array_to_string drops NULLs unless given replacement text.
	if got := fnArrayToString([]any{[]any{"a", nil, "b"}, ","}); got != "a,b" {
		t.Errorf("array_to_string drops NULLs: got %v", got)
	}
	if got := fnArrayToString([]any{[]any{"a", nil, "b"}, ",", "?"}); got != "a,?,b" {
		t.Errorf("array_to_string with null text: got %v", got)
	}
	// Privileges and visibility are granted/visible: answering false would
	// make a BI client hide tables the user can plainly query.
	if fnTrue(nil) != true || fnFalse(nil) != false {
		t.Error("privilege and recovery shims answer the wrong constant")
	}
	// Comments are genuinely absent, and NULL is PostgreSQL's own answer for
	// an object without one — exact, not a stand-in.
	if fnNull(nil) != nil {
		t.Error("obj_description must be NULL")
	}
}

// TestUnshimmedIntrospectionErrors pins the other half of the decision: the
// pg_catalog functions deliberately left unimplemented. Each needs state no
// catalog view answers — a relation's storage size, a function's definition —
// and a plausible-looking wrong answer from one of them is worse than a named
// error. to_regclass and pg_get_serial_sequence left this list when the
// catalog became relations the planner can bind a function to (ADR-0044).
func TestUnshimmedIntrospectionErrors(t *testing.T) {
	for _, name := range []string{
		"pg_get_functiondef", "pg_get_partkeydef",
		"pg_relation_size", "pg_total_relation_size", "pg_sleep", "pg_database_size",
	} {
		if err := checkKnown(name); err == nil {
			t.Errorf("%s: expected an error, got nil — if this was implemented "+
				"on purpose, move it to TestPgCompatShimsRegistered", name)
		}
	}
}
