// SPDX-License-Identifier: MIT

package cli

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// The #1218 gate: what the CLI PRINTS is what the wire carries.
//
// A result is an ordered list of columns and rows of values BY POSITION. The
// three render sites in this package handed the formatter rows keyed by
// column NAME, and a result may legally carry two output columns of one name
// — `SELECT abs(a), abs(b)` is two called `abs`, and a star over a
// `JOIN … USING` whose arms share a tail name publishes that name twice. The
// map held the LAST of them.
//
// Measured against PostgreSQL 17.11 and against psql on this engine's own
// pgwire server, over the same fixture these cells build:
//
//	SELECT * FROM la JOIN rb USING (id)
//	  psql:                 2 | 20 | 200 | 300 | 99
//	  wadjet query, base:   2 | 99 | 200 | 300 | 99      <- the first `tail`
//	  wadjet query --format=json, base: {"amt":200,"id":2,"note":300,"tail":99}
//
// so the table and CSV forms printed one column's value under another
// column's heading and the default JSON form dropped a column outright.

// cliRowCase is one query and the exact text each format must print for it.
type cliRowCase struct {
	name  string
	sql   string
	table string
	csv   string
	json  string
}

var cliRowCases = []cliRowCase{
	{
		name: "a star over a USING join whose arms share a tail name",
		sql:  "SELECT * FROM la JOIN rb USING (id)",
		table: "+----+------+-----+------+------+\n" +
			"| id | tail | amt | note | tail |\n" +
			"+----+------+-----+------+------+\n" +
			"| 2  | 20   | 200 | 300  | 99   |\n" +
			"+----+------+-----+------+------+\n" +
			"(1 rows)\n",
		csv: "id,tail,amt,note,tail\n2,20,200,300,99\n",
		json: "[\n  {\n    \"id\": 2,\n    \"tail\": 20,\n    \"amt\": 200,\n" +
			"    \"note\": 300,\n    \"tail\": 99\n  }\n]\n",
	},
	{
		name: "a star over an unqualified join: two names collide",
		sql:  "SELECT * FROM la, rb WHERE la.id = rb.id",
		table: "+----+------+-----+----+------+------+\n" +
			"| id | tail | amt | id | note | tail |\n" +
			"+----+------+-----+----+------+------+\n" +
			"| 2  | 20   | 200 | 2  | 300  | 99   |\n" +
			"+----+------+-----+----+------+------+\n" +
			"(1 rows)\n",
		csv: "id,tail,amt,id,note,tail\n2,20,200,2,300,99\n",
		json: "[\n  {\n    \"id\": 2,\n    \"tail\": 20,\n    \"amt\": 200,\n" +
			"    \"id\": 2,\n    \"note\": 300,\n    \"tail\": 99\n  }\n]\n",
	},
	{
		name: "three output columns of one name",
		sql:  "SELECT tail AS t, tail + 1 AS t, tail + 2 AS t FROM la",
		table: "+----+----+----+\n" +
			"| t  | t  | t  |\n" +
			"+----+----+----+\n" +
			"| 20 | 21 | 22 |\n" +
			"+----+----+----+\n" +
			"(1 rows)\n",
		csv:  "t,t,t\n20,21,22\n",
		json: "[\n  {\n    \"t\": 20,\n    \"t\": 21,\n    \"t\": 22\n  }\n]\n",
	},
	{
		name: "one name over two different types",
		sql:  "SELECT id AS m, 'x' AS m FROM la",
		table: "+---+---+\n" +
			"| m | m |\n" +
			"+---+---+\n" +
			"| 2 | x |\n" +
			"+---+---+\n" +
			"(1 rows)\n",
		csv:  "m,m\n2,x\n",
		json: "[\n  {\n    \"m\": 2,\n    \"m\": \"x\"\n  }\n]\n",
	},
	{
		name: "a NULL in one of them",
		sql:  "SELECT NULL AS n, CAST(7 AS BIGINT) AS n FROM la",
		table: "+------+---+\n" +
			"| n    | n |\n" +
			"+------+---+\n" +
			"| NULL | 7 |\n" +
			"+------+---+\n" +
			"(1 rows)\n",
		csv:  "n,n\nNULL,7\n",
		json: "[\n  {\n    \"n\": null,\n    \"n\": 7\n  }\n]\n",
	},
	{
		// The control: unique names, where the map form was exact.
		name: "unique names are unchanged",
		sql:  "SELECT id, amt FROM la",
		table: "+----+-----+\n" +
			"| id | amt |\n" +
			"+----+-----+\n" +
			"| 2  | 200 |\n" +
			"+----+-----+\n" +
			"(1 rows)\n",
		csv:  "id,amt\n2,200\n",
		json: "[\n  {\n    \"id\": 2,\n    \"amt\": 200\n  }\n]\n",
	},
}

// TestTheQueryDoorPrintsEveryColumnTheWireCarries drives the REAL `query`
// command — root.go's shared-catalog render site — in every format the CLI
// accepts.
func TestTheQueryDoorPrintsEveryColumnTheWireCarries(t *testing.T) {
	storage := newCLIFixture(t)
	for _, tc := range cliRowCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, f := range []struct {
				spelling string
				want     string
			}{
				{"table", tc.table},
				{"csv", tc.csv},
				{"json", tc.json},
			} {
				got := runCLI(t, append(storage, "query", "--format="+f.spelling, tc.sql)...)
				if got != f.want {
					t.Errorf("wadjet query --format=%s %q\n got:\n%s\nwant:\n%s\n"+
						"the renderer read the row by column NAME, so a duplicate name "+
						"collapsed two columns onto one value (#1218)",
						f.spelling, tc.sql, got, f.want)
				}
			}
		})
	}
}

// TestTheCatalogFreeQueryDoorPrintsEveryColumn holds the OTHER render site in
// queryCmd: a statement whose every source is catalog-free opens its own
// MemStore database and returns before the shared one is ever built, so it
// renders through a call of its own.
func TestTheCatalogFreeQueryDoorPrintsEveryColumn(t *testing.T) {
	storage := newCLIStorageArgs(t)
	const sql = "SELECT 1 AS u, 2 AS u"
	if !isCatalogFreeQuery(sql) {
		t.Fatalf("%q no longer takes the catalog-free path — the cell is measuring the "+
			"wrong render site", sql)
	}
	for _, tc := range []struct{ spelling, want string }{
		{"json", "[\n  {\n    \"u\": 1,\n    \"u\": 2\n  }\n]\n"},
		{"csv", "u,u\n1,2\n"},
		{"table", "+---+---+\n| u | u |\n+---+---+\n| 1 | 2 |\n+---+---+\n(1 rows)\n"},
	} {
		got := runCLI(t, append(storage, "query", "--format="+tc.spelling, sql)...)
		if got != tc.want {
			t.Errorf("wadjet query --format=%s %q\n got:\n%s\nwant:\n%s",
				tc.spelling, sql, got, tc.want)
		}
	}
}

// TestTheShellDoorPrintsEveryColumnTheWireCarries holds the third render
// site, runShell's, by driving the real `shell` command with scripted stdin.
func TestTheShellDoorPrintsEveryColumnTheWireCarries(t *testing.T) {
	storage := newCLIFixture(t)
	// The shell writes its history under $HOME.
	t.Setenv("HOME", t.TempDir())

	const sql = "SELECT * FROM la JOIN rb USING (id)"
	for _, tc := range []struct{ spelling, want string }{
		{"csv", "id,tail,amt,note,tail\n2,20,200,300,99\n"},
		{"json", "\"tail\": 20,"},
		{"table", "| 2  | 20   | 200 | 300  | 99   |"},
	} {
		out := runCLIWithStdin(t, sql+";\nexit\n", append(storage, "shell", "--format="+tc.spelling)...)
		if !strings.Contains(out, tc.want) {
			t.Errorf("wadjet shell --format=%s over %q printed\n%s\nwant a line containing %q "+
				"— the shell renders through its own format.WriteDeclared call and it read the "+
				"row by column NAME (#1218)", tc.spelling, sql, out, tc.want)
		}
	}
}

// TestNoRenderSiteHandsTheFormatterANameKeyedRow closes the CLASS the three
// sites belong to: the formatter takes values BY POSITION, and a render call
// that reaches for QueryResult.Rows is one that cannot represent a result
// carrying two output columns of one name. A fourth site added later inherits
// the rule rather than the defect.
func TestNoRenderSiteHandsTheFormatterANameKeyedRow(t *testing.T) {
	src, err := os.ReadFile("root.go")
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, line := range strings.Split(string(src), "\n") {
		if !strings.Contains(line, "format.Write") {
			continue
		}
		found++
		if strings.Contains(line, ".Rows)") {
			t.Errorf("a render site passes the name-keyed map to the formatter:\n  %s\n"+
				"rows are values BY POSITION (resultRows / QueryResult.Cells); a map cannot "+
				"hold two columns of one name (#1218)", strings.TrimSpace(line))
		}
	}
	if found < 3 {
		t.Fatalf("found %d format.Write* call sites in root.go, want at least 3 — "+
			"the walk stopped finding the sites it is meant to hold", found)
	}
}

// --- fixture and driver -------------------------------------------------

// newCLIStorageArgs returns the persistent flags that point a command at a
// private file store and catalog.
func newCLIStorageArgs(t *testing.T) []string {
	t.Helper()
	root := t.TempDir()
	return []string{
		"--storage-type=file",
		"--data-dir=" + filepath.Join(root, "data"),
		"--bucket=crtest",
		"--nats-store-dir=" + filepath.Join(root, "nats"),
		"--background-compaction=false",
	}
}

// newCLIFixture builds the two relations the duplicate-name shapes read,
// through the CLI's own door, and returns the storage flags every later
// command must repeat to reach them.
//
// The values are chosen so the colliding columns DISAGREE — la.tail is 20 and
// rb.tail is 99 — which is what makes the cells discriminating: a renderer
// keyed by name answers the last column's value in every position of that
// name, and a fixture whose colliding columns agreed would pass either way.
func newCLIFixture(t *testing.T) []string {
	t.Helper()
	storage := newCLIStorageArgs(t)
	for _, stmt := range []string{
		"CREATE TABLE la (id BIGINT, tail BIGINT, amt BIGINT)",
		"CREATE TABLE rb (id BIGINT, note BIGINT, tail BIGINT)",
		"INSERT INTO la VALUES (2, 20, 200)",
		"INSERT INTO rb VALUES (2, 300, 99)",
	} {
		runCLI(t, append(storage, "query", stmt)...)
	}
	return storage
}

// runCLI executes the real root command with these arguments and returns what
// it wrote to standard output. The render sites write to os.Stdout directly,
// which is the surface an operator reads, so that is what the cell captures.
func runCLI(t *testing.T, args ...string) string {
	t.Helper()
	out, err := captureStdout(t, func() error { return executeRootCmd(t, args) })
	if err != nil {
		t.Fatalf("wadjet %s: %v\noutput:\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

// runCLIWithStdin is runCLI with a scripted standard input, for `shell`.
func runCLIWithStdin(t *testing.T, stdin string, args ...string) string {
	t.Helper()
	oldIn := os.Stdin
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdin = r
	defer func() { os.Stdin = oldIn; r.Close() }()
	go func() { io.WriteString(w, stdin); w.Close() }()

	out, execErr := captureStdout(t, func() error { return executeRootCmd(t, args) })
	if execErr != nil {
		t.Fatalf("wadjet %s: %v\noutput:\n%s", strings.Join(args, " "), execErr, out)
	}
	return out
}

// executeRootCmd runs one command line through a fresh root command and
// restores every bound flag variable after, the way the config-precedence
// cells do.
func executeRootCmd(t *testing.T, args []string) error {
	t.Helper()
	root := NewRootCmd(EmbeddedServeCmd())
	resolvedConfig.Store(nil)
	t.Cleanup(func() {
		NewRootCmd(EmbeddedServeCmd())
		resolvedConfig.Store(nil)
	})
	root.SetOut(io.Discard)
	root.SetErr(io.Discard)
	root.SetArgs(args)
	root.SilenceUsage = true
	root.SetFlagErrorFunc(func(*cobra.Command, error) error { return nil })
	return root.Execute()
}

// captureStdout runs fn with os.Stdout redirected to a pipe and returns what
// it wrote.
func captureStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	runErr := fn()
	w.Close()
	os.Stdout = old
	out := <-done
	r.Close()
	return out, runErr
}
