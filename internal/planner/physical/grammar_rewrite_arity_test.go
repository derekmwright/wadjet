// SPDX-License-Identifier: MIT

package physical

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// B1's gate, and the THIRD direction of the arity table's closure.
//
// The table was built from the DOCUMENTED signature, and this grammar has
// spellings that produce a call the documentation never writes as one:
// `TRIM(BOTH ' ' FROM s)` is parsed into `trim(s, ' ')`, a TWO-argument call
// for a function docs/sql-reference.md documented as `TRIM(s)`. Neither
// closure gate could see it — one compares the table with the registry and the
// other with the docs, and a rewrite is in neither — so every SQL-standard
// TRIM spelling was refused `42883 function trim(text, unknown) does not
// exist` on a shape the base answered with PostgreSQL's exact value (round-1
// review, B1).
//
// The only thing that catches that is PARSING the spelling and asking the
// table about the call it produces. It lives here rather than in expr because
// this package already has the walk that finds every call in an expression,
// and it is the walk the binder's own refusal uses.
func TestEveryGrammarRewriteProducesACallItsSignatureAccepts(t *testing.T) {
	produced := map[string]bool{}
	checked := 0
	for _, c := range grammarRewriteSpellings() {
		t.Run(c.name, func(t *testing.T) {
			node, err := plansql.ParseExpressionComplete(c.sql)
			if err != nil {
				t.Fatalf("parse: %v\n  expression: %s", err, c.sql)
			}
			var calls []*plansql.FuncCallNode
			walkExpr(node, nil, nil, &calls)
			if len(calls) == 0 {
				t.Fatalf("%s produced no function call at all; this spelling exists to be "+
					"rewritten into one\n  expression: %s", c.name, c.sql)
			}
			sawWanted := false
			for _, fc := range calls {
				name := strings.ToLower(strings.TrimSpace(fc.Name))
				produced[name] = true
				if name == c.wantFn {
					sawWanted = true
					if got := len(fc.Args); got != c.wantArgs {
						t.Errorf("%s was rewritten into %s with %d arguments, want %d — this "+
							"table's expectation and the grammar disagree, so the arity check "+
							"below is not measuring the shape it names\n  expression: %s",
							c.name, name, got, c.wantArgs, c.sql)
					}
				}
				sig, ok := expr.SignatureOf(name)
				if !ok {
					continue // an aggregate or a UDF: not this table's business
				}
				checked++
				if !sig.Accepts(len(fc.Args)) {
					t.Errorf("the grammar rewrites %s into %s(%d arguments) and the arity table "+
						"declares %s, so every query written this way is refused 42883 on a "+
						"spelling this engine PARSES — which is B1's mechanism\n  expression: %s",
						c.name, name, len(fc.Args), sig, c.sql)
				}
			}
			if !sawWanted {
				t.Errorf("%s did not produce a call to %q; it produced %s\n  expression: %s",
					c.name, c.wantFn, grammarCallNames(calls), c.sql)
			}
		})
	}
	if checked < 25 {
		t.Errorf("only %d (rewrite, signature) pairs were checked; the spelling table has "+
			"stopped reaching the parser and this gate is vacuous", checked)
	}

	// THE SECOND DIRECTION. Every function name the PARSER mints as a string
	// literal must appear above: a rewrite added later with no spelling here
	// would be B1 again — a call the table has never been asked about.
	for _, name := range parserMintedFuncNames(t) {
		if !produced[name] {
			t.Errorf("a file under internal/planner mints a call to %q and no spelling "+
				"in grammarRewriteSpellings produces one, so its arity is never compared with "+
				"the table — which is exactly how B1 reached a tip", name)
		}
	}
}

type grammarRewrite struct {
	name, sql string
	// wantFn and wantArgs are the call the spelling is DOCUMENTED to produce,
	// asserted so this table cannot quietly stop exercising its own rewrite: a
	// spelling that silently parsed as something else would make the arity
	// check above pass while measuring nothing.
	wantFn   string
	wantArgs int
}

// grammarRewriteSpellings is every spelling internal/planner/sql turns into a
// FuncCallNode the query did not write as a call. Each is a door onto the arity
// table that neither the registry closure nor the docs closure reaches.
func grammarRewriteSpellings() []grammarRewrite {
	return []grammarRewrite{
		// --- TRIM: eleven spellings, three target names, one or two
		// arguments. B1's own family.
		{"trim_plain", `TRIM(s)`, "trim", 1},
		{"trim_both_from", `TRIM(BOTH ' ' FROM s)`, "trim", 2},
		{"trim_leading_from", `TRIM(LEADING ' ' FROM s)`, "ltrim", 2},
		{"trim_trailing_from", `TRIM(TRAILING ' ' FROM s)`, "rtrim", 2},
		{"trim_both_no_char", `TRIM(BOTH FROM s)`, "trim", 1},
		{"trim_leading_no_char", `TRIM(LEADING FROM s)`, "ltrim", 1},
		{"trim_trailing_no_char", `TRIM(TRAILING FROM s)`, "rtrim", 1},
		{"trim_two_args", `TRIM(s, ' ')`, "trim", 2},
		{"trim_column_cutset", `TRIM(s, s)`, "trim", 2},
		{"ltrim_two_args", `LTRIM(s, '0')`, "ltrim", 2},
		{"rtrim_two_args", `RTRIM(s, '0')`, "rtrim", 2},
		{"ltrim_one_arg", `LTRIM(s)`, "ltrim", 1},
		{"rtrim_one_arg", `RTRIM(s)`, "rtrim", 1},
		// --- POSITION(needle IN haystack) -> strpos(haystack, needle)
		{"position_in", `POSITION('a' IN s)`, "strpos", 2},
		// --- `a ^ b` -> power(a, b). PostgreSQL's exponentiation operator is
		// this engine's POWER() under another spelling (arc PS, #1155), and it
		// is a rewrite in exactly this table's sense: the query writes an
		// OPERATOR and the planner hands the binder a call.
		{"power_operator", `2 ^ 3`, "power", 2},
		{"power_operator_left_associative", `2 ^ 3 ^ 2`, "power", 2},
		{"power_operator_over_columns", `s ^ n`, "power", 2},
		// --- a FULL `JOIN … USING (c)` merges its joined column to
		// COALESCE(left.c, right.c), minted in TWO places: the star
		// expansion's merged output item (logical.mergedUsingItem) and the
		// binding of a bare sort or window key onto the merge
		// (plansql.bindMergedUsingKeys). Both mint exactly two arguments.
		//
		// The mint is STATEMENT-level — it needs a FROM clause with that join,
		// which `ParseExpressionComplete` has no room for — so the spelling
		// here is the call those sites build, which is what this table exists
		// to compare with the arity declaration. The statement shapes
		// themselves are gated on five arms by
		// coordinator.TestArcPSGrammarAnswersTheSameOnEveryArm's `#655` cells.
		{"full_using_merged_key", `COALESCE(s, n)`, "coalesce", 2},
		// --- EXTRACT(field FROM x) -> field(x). The FIELD names the function,
		// so every field this parser maps is its own door onto the table.
		{"extract_year", `EXTRACT(YEAR FROM ts)`, "year", 1},
		{"extract_month", `EXTRACT(MONTH FROM ts)`, "month", 1},
		{"extract_day", `EXTRACT(DAY FROM ts)`, "day", 1},
		{"extract_hour", `EXTRACT(HOUR FROM ts)`, "hour", 1},
		{"extract_minute", `EXTRACT(MINUTE FROM ts)`, "minute", 1},
		{"extract_second", `EXTRACT(SECOND FROM ts)`, "second", 1},
		{"extract_quarter", `EXTRACT(QUARTER FROM ts)`, "quarter", 1},
		{"extract_week", `EXTRACT(WEEK FROM ts)`, "week", 1},
		{"extract_epoch", `EXTRACT(EPOCH FROM ts)`, "epoch", 1},
		{"extract_dow", `EXTRACT(DOW FROM ts)`, "day_of_week", 1},
		{"extract_doy", `EXTRACT(DOY FROM ts)`, "day_of_year", 1},
		// --- the NILADIC spellings: no parentheses in the source text at all.
		{"current_date", `CURRENT_DATE`, "current_date", 0},
		{"current_time", `CURRENT_TIME`, "current_time", 0},
		{"current_timestamp", `CURRENT_TIMESTAMP`, "current_timestamp", 0},
		{"current_user", `CURRENT_USER`, "current_user", 0},
		{"session_user", `SESSION_USER`, "session_user", 0},
		{"user", `USER`, "user", 0},
		{"current_role", `CURRENT_ROLE`, "current_role", 0},
		{"current_catalog", `CURRENT_CATALOG`, "current_catalog", 0},
		{"current_schema", `CURRENT_SCHEMA`, "current_schema", 0},
		// --- AT TIME ZONE -> timezone(zone, instant)
		{"at_time_zone", `ts AT TIME ZONE 'UTC'`, "timezone", 2},
		// --- a field path over a COMPUTED row -> row_field(container, 'name').
		// A BARE `rw.f` is a qualified column reference and stays one; the
		// rewrite fires only after an expression the parser already knows is
		// computed, which is the spelling that reaches the table.
		{"row_field_path", `(SEMVER_PARSE('1.2.3')).major`, "row_field", 2},
		// --- ILIKE -> lower() on BOTH sides
		{"ilike", `s ILIKE 'A%'`, "lower", 1},
		{"not_ilike", `s NOT ILIKE 'A%'`, "lower", 1},
		// --- SIMILAR TO -> regexp_like(left, pattern)
		{"similar_to", `s SIMILAR TO 'a'`, "regexp_like", 2},
		{"not_similar_to", `s NOT SIMILAR TO 'a'`, "regexp_like", 2},
	}
}

func grammarCallNames(calls []*plansql.FuncCallNode) string {
	out := make([]string, 0, len(calls))
	for _, c := range calls {
		out = append(out, fmt.Sprintf("%s/%d", strings.ToLower(c.Name), len(c.Args)))
	}
	sort.Strings(out)
	return strings.Join(out, " ")
}

// parserMintedFuncNames reads the ONE parser file and returns every function
// name it constructs a FuncCallNode for as a STRING LITERAL.
//
// It reads a NAMED file rather than walking a tree, which is what the Go
// toolchain itself would do: a git worktree lives inside the module root and a
// walk sees every file a second time.
func parserMintedFuncNames(t *testing.T) []string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	// EVERY planner file, not one: internal/planner/physical/group_key_binding.go
	// already mints a literal `row_field` call, and a rewrite added in any of
	// these packages is B1's mechanism again. Test files are skipped, and so
	// are directories the Go toolchain itself skips (`.`/`_`), so a worktree
	// under .claude/ is not read twice.
	re := regexp.MustCompile(`FuncCallNode\{Name:\s*"([a-z_][a-z0-9_]*)"`)
	set := map[string]bool{}
	files := 0
	walkErr := filepath.Walk(filepath.Join(root, "internal", "planner"), func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if n := info.Name(); n != "planner" && (strings.HasPrefix(n, ".") || strings.HasPrefix(n, "_")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		files++
		for _, m := range re.FindAllStringSubmatch(string(body), -1) {
			set[m[1]] = true
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk internal/planner: %v", walkErr)
	}
	if files < 20 {
		t.Errorf("only %d planner source files were read; the walk has stopped reaching them", files)
	}
	// TRIM's three names and EXTRACT's fields are built from VARIABLES, so the
	// regex cannot see them; the spelling table covers those and this guard
	// says out loud when the regex has stopped matching anything at all.
	if len(set) < 4 {
		t.Errorf("only %d literal-named rewrites found in select_parser.go; the pattern has "+
			"stopped matching and this half of the closure is vacuous", len(set))
	}
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
