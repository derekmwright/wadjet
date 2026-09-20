// SPDX-License-Identifier: MIT

package cli

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// plannerConfigFlags are the persistent planner and engine flags this package
// resolves and must therefore hand to every database it opens. They are
// INSTANCE configuration (#1223): before that arc, `--bushy-join-reorder`
// reached the planner through a package variable that root.go's
// PersistentPreRunE stored for the whole process, so every command inherited
// it whether or not its own database carried it. With the variable gone, a
// `wadjet.Config` literal that omits the field is a command whose typed flag
// silently does nothing — which is exactly what the round-1 review measured
// on `wadjet query`.
//
// The other four were never global at all, so they were inert on these doors
// at BASE too: `wadjet query --memory-budget=…`, `--spill-dir`,
// `--sort-merge-join-bytes` and `--late-materialization` were accepted and
// ignored while `serve` honoured them, and --late-materialization's
// documented default of TRUE was false here because the field nobody sets is
// the zero value (#1226). The list is the whole set `serve` carries, so a
// door added later inherits the requirement for all five.
var plannerConfigFlags = []string{
	"BushyJoinReorder",
	"SortMergeJoinBytes",
	"LateMaterialization",
	"MemoryBudget",
	"SpillDir",
}

// TestEveryCLIDatabaseCarriesTheResolvedPlannerFlags closes the CLASS rather
// than the two doors the review found: it reads this package's own source and
// requires EVERY wadjet.Config composite literal to set each planner flag. A
// door added later inherits the requirement instead of inheriting the defect.
//
// It is a source assertion on purpose. The behavioural cell below can only
// reach the databases a test can open; this one reaches every literal,
// including `query`'s catalog-free path, whose MemStore holds no relation for
// a join to be reordered over and which therefore has no observable plan of
// its own to compare.
func TestEveryCLIDatabaseCarriesTheResolvedPlannerFlags(t *testing.T) {
	fset := token.NewFileSet()
	// The package's own directory, parsed with go/parser rather than walked:
	// a git worktree under .claude/worktrees/ is a second copy of this source
	// and a walking gate would read it too (CLAUDE.md).
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parsing package cli: %v", err)
	}

	found := 0
	for _, pkg := range pkgs {
		for path, file := range pkg.Files {
			ast.Inspect(file, func(n ast.Node) bool {
				lit, ok := n.(*ast.CompositeLit)
				if !ok {
					return true
				}
				sel, ok := lit.Type.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Config" {
					return true
				}
				if id, ok := sel.X.(*ast.Ident); !ok || id.Name != "wadjet" {
					return true
				}
				found++
				set := map[string]bool{}
				for _, elt := range lit.Elts {
					kv, ok := elt.(*ast.KeyValueExpr)
					if !ok {
						continue
					}
					if key, ok := kv.Key.(*ast.Ident); ok {
						set[key.Name] = true
					}
				}
				for _, field := range plannerConfigFlags {
					if !set[field] {
						t.Errorf("%s: this wadjet.Config sets no %s, so the command that opens it "+
							"accepts --%s (a PERSISTENT root flag, printed in its --help) and ignores it. "+
							"Planner configuration is the instance's now — hand it the resolved value (#1223).",
							fset.Position(lit.Pos()), field, flagNameFor(field))
					}
				}
				_ = filepath.Base(path)
				return true
			})
		}
	}
	// Non-vacuity: the three literals are serve_embedded.go's, shared_catalog.go's
	// (query / create-table / drop-table / shell / mcp) and root.go's catalog-free
	// query path. A refactor that leaves none is a gate that proves nothing.
	if found < 3 {
		t.Fatalf("found %d wadjet.Config literals in package cli, want at least 3 — "+
			"the walk stopped finding the doors it is meant to hold", found)
	}
}

// flagNameFor spells a Config field as the flag an operator types.
func flagNameFor(field string) string {
	switch field {
	case "BushyJoinReorder":
		return "bushy-join-reorder"
	case "SortMergeJoinBytes":
		return "sort-merge-join-bytes"
	case "LateMaterialization":
		return "late-materialization"
	case "MemoryBudget":
		return "memory-budget"
	case "SpillDir":
		return "spill-dir"
	default:
		return strings.ToLower(field)
	}
}

// TestTheSharedCLIDatabasePlansByTheResolvedFlag is the behavioural half: the
// database behind `query`, `create-table`, `drop-table`, `shell` and `mcp` —
// all five call openSharedDB, so one cell answers for all five — plans by the
// resolved flag, on and off.
//
// It fails at f1a7319d, where openSharedDB's Config omitted the field: the
// flag-on arm planned left-deep, byte-identical to flag-off, while `--help`
// advertised the flag (measured by the round-1 review).
func TestTheSharedCLIDatabasePlansByTheResolvedFlag(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag bool
		want int64
	}{
		{name: "--bushy-join-reorder", flag: true, want: 1},
		{name: "the shipped default", flag: false, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withCLIPlannerFlags(t, tc.flag)
			ctx := context.Background()
			db, release, err := openSharedDB(ctx, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
			if err != nil {
				t.Fatalf("openSharedDB: %v", err)
			}
			defer release()

			loadExpandingChain(t, db)
			before := logical.BushyJoinsPlanned.Load()
			res, err := db.Query(ctx, expandingChainSQL)
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("expanding chain returned %d rows, want 1", len(res.Rows))
			}
			if got := logical.BushyJoinsPlanned.Load() - before; got != tc.want {
				t.Fatalf("the shared CLI database planned %d bushy joins under %s, want %d — "+
					"the resolved flag did not reach the database the command opens (#1223)",
					got, tc.name, tc.want)
			}
		})
	}
}

// withCLIPlannerFlags resolves a real command line through the real root
// command, which is the point: the cell then measures what an OPERATOR's
// typed --bushy-join-reorder reaches, through the same resolution
// (ADR-0029) the commands use, rather than a package variable a test set by
// hand. resolveThroughTheRealCommand restores every bound variable after.
func withCLIPlannerFlags(t *testing.T, bushy bool) {
	t.Helper()
	var extra []string
	if bushy {
		extra = append(extra, "--bushy-join-reorder")
	}
	withCLICommandLine(t, extra...)
}

// withCLICommandLine resolves a real command line carrying a private store and
// catalog plus these extra flags, through the real root command.
func withCLICommandLine(t *testing.T, extra ...string) {
	t.Helper()
	root := t.TempDir()
	args := []string{
		"--storage-type=file",
		"--data-dir=" + filepath.Join(root, "data"),
		"--bucket=test",
		"--nats-store-dir=" + filepath.Join(root, "nats"),
	}
	args = append(args, extra...)
	if _, err := resolveThroughTheRealCommand(t, args); err != nil {
		t.Fatalf("resolving the command line: %v", err)
	}
}

// expandingChainSQL is the four-relation shape whose cheapest order only a
// bushy plan can express — fact_a ⋈ fact_b is a many-to-many explosion and
// each fact carries a small dimension — so the two regimes choose different
// join orders and the counter moves for one and not the other.
const expandingChainSQL = `SELECT count(*) AS n FROM fact_a ` +
	`JOIN dim_x ON a_x = x_id ` +
	`JOIN fact_b ON a_id = b_id ` +
	`JOIN dim_y ON b_y = y_id`

func loadExpandingChain(t *testing.T, db *wadjet.DB) {
	t.Helper()
	ctx := context.Background()
	two := func(a, b string) parquet.Schema {
		return parquet.Schema{Columns: []parquet.Column{
			{Name: a, Type: parquet.TypeInt64},
			{Name: b, Type: parquet.TypeInt64},
		}}
	}
	load := func(table string, schema parquet.Schema, rows []map[string]any) {
		if err := db.CreateTable(ctx, table, schema, nil); err != nil {
			t.Fatalf("CreateTable %s: %v", table, err)
		}
		ing := db.NewIngester(table, schema, nil, ingest.Config{MaxBufferRows: 10000, RowGroupSize: 500})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatalf("ingest %s: %v", table, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", table, err)
		}
		// The DP cost model reads per-column NDV, which only ANALYZE computes.
		if _, err := db.Query(ctx, "ANALYZE TABLE "+table); err != nil {
			t.Fatalf("ANALYZE %s: %v", table, err)
		}
	}
	const factRows = 1000
	factA := make([]map[string]any, factRows)
	factB := make([]map[string]any, factRows)
	for i := 0; i < factRows; i++ {
		factA[i] = map[string]any{"a_id": int64(i % 10), "a_x": int64(i)}
		factB[i] = map[string]any{"b_id": int64(i % 10), "b_y": int64(i)}
	}
	dimX := make([]map[string]any, 10)
	dimY := make([]map[string]any, 10)
	for i := 0; i < 10; i++ {
		dimX[i] = map[string]any{"x_id": int64(i), "x_v": int64(i)}
		dimY[i] = map[string]any{"y_id": int64(i), "y_v": int64(i)}
	}
	load("fact_a", two("a_id", "a_x"), factA)
	load("dim_x", two("x_id", "x_v"), dimX)
	load("fact_b", two("b_id", "b_y"), factB)
	load("dim_y", two("y_id", "y_v"), dimY)
}

// TestTheSharedCLIDatabaseHonoursEveryResolvedEngineFlag is the behavioural
// half of #1226: the four flags the source gate above added must MEAN
// something on this door, not merely appear in the literal.
//
// One database answers for `query`, `create-table`, `drop-table`, `shell` and
// `mcp` because all five call openSharedDB. Each cell pairs a flag-on arm
// with the arm the shipped default gives, so a cell that measured nothing —
// both arms alike — is a failure rather than a pass.
//
// At base every one of these was inert here while `serve` honoured it:
// `wadjet query --memory-budget=4096` answered a query `serve` refuses,
// `--spill-dir` left the named directory untouched, `--sort-merge-join-bytes`
// planned no sort-merge join, and `--late-materialization` could not even be
// turned off because it was already false — the field nobody set is the zero
// value, so the flag's documented default of true did not hold on this door.
func TestTheSharedCLIDatabaseHonoursEveryResolvedEngineFlag(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	// A two-relation equi-join on INT64 keys: big enough on both sides for
	// the sort-merge estimate to reach a low threshold, and a hash join
	// otherwise, which is what the late-materialization counter needs.
	const joinSQL = `SELECT count(*) AS n FROM fact_a JOIN fact_b ON a_id = b_id`
	// A CROSS JOIN's probe reads every build row, so its build cannot be
	// grace-partitioned and cannot spill: past the budget it REFUSES, which
	// is the loud, deterministic way a memory budget is observable
	// (ADR-0006's routed-probe amendment).
	const crossSQL = `SELECT count(*) AS n FROM fact_a a CROSS JOIN fact_b b`

	t.Run("--sort-merge-join-bytes", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			args []string
			want bool
		}{
			{name: "--sort-merge-join-bytes=1024", args: []string{"--sort-merge-join-bytes=1024"}, want: true},
			{name: "the shipped default", want: false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				withCLICommandLine(t, tc.args...)
				db, release := openFlagDB(t, logger)
				defer release()
				loadExpandingChain(t, db)
				before := physical.SortMergeJoinsPlanned.Load()
				if _, err := db.Query(context.Background(), joinSQL); err != nil {
					t.Fatalf("query: %v", err)
				}
				planned := physical.SortMergeJoinsPlanned.Load() - before
				if (planned > 0) != tc.want {
					t.Fatalf("the shared CLI database planned %d sort-merge joins under %s, "+
						"want %s — the resolved flag did not reach the database the command "+
						"opens (#1226)", planned, tc.name, map[bool]string{true: "at least 1", false: "0"}[tc.want])
				}
			})
		}
	})

	t.Run("--late-materialization", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			args []string
			want bool
		}{
			// The DEFAULT is true, and at base this door ran on the zero
			// value: the default arm is the failing one there.
			{name: "the shipped default (true)", want: true},
			{name: "--late-materialization=false", args: []string{"--late-materialization=false"}, want: false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				withCLICommandLine(t, tc.args...)
				db, release := openFlagDB(t, logger)
				defer release()
				loadExpandingChain(t, db)
				before := physical.LateMatJoinsPlanned.Load()
				if _, err := db.Query(context.Background(), joinSQL); err != nil {
					t.Fatalf("query: %v", err)
				}
				planned := physical.LateMatJoinsPlanned.Load() - before
				if (planned > 0) != tc.want {
					t.Fatalf("the shared CLI database planned %d late-materialized join probes "+
						"under %s, want %s — the resolved flag did not reach the database the "+
						"command opens (#1226)", planned, tc.name,
						map[bool]string{true: "at least 1", false: "0"}[tc.want])
				}
			})
		}
	})

	t.Run("--memory-budget", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			args      []string
			wantRefus bool
		}{
			{name: "--memory-budget=4096", args: []string{"--memory-budget=4096"}, wantRefus: true},
			{name: "the shipped default", wantRefus: false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				withCLICommandLine(t, tc.args...)
				db, release := openFlagDB(t, logger)
				defer release()
				loadExpandingChain(t, db)
				_, err := db.Query(context.Background(), crossSQL)
				if tc.wantRefus {
					if err == nil {
						t.Fatalf("the shared CLI database ANSWERED %q under %s — a budget "+
							"`serve` refuses this query under was accepted and ignored (#1226)",
							crossSQL, tc.name)
					}
					if !strings.Contains(err.Error(), "memory budget exceeded") {
						t.Fatalf("refused under %s, but not for the budget: %v", tc.name, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("the shipped default refused a query it must answer: %v", err)
				}
			})
		}
	})

	t.Run("--spill-dir", func(t *testing.T) {
		dir := t.TempDir()
		named := filepath.Join(dir, "named-spill-volume")
		withCLICommandLine(t, "--spill-dir="+named, "--memory-budget=4096")
		db, release := openFlagDB(t, logger)
		defer release()
		loadExpandingChain(t, db)
		// A pipeline breaker, so a spill manager is actually built: it
		// creates its working directory UNDER the configured one, and that
		// directory's existence is the flag arriving. A query with no
		// breaker never asks for one, so it would prove nothing.
		if _, err := db.Query(context.Background(),
			`SELECT a_id, count(*) AS n FROM fact_a GROUP BY a_id ORDER BY a_id`); err != nil {
			t.Fatalf("query: %v", err)
		}
		if _, err := os.Stat(filepath.Join(named, "wadjet-spill")); err != nil {
			t.Fatalf("nothing under the named --spill-dir %s: %v — the resolved flag did not "+
				"reach the database the command opens, so spill files went to the OS temp "+
				"directory instead (#1226)", named, err)
		}
	})
}

// openFlagDB opens the shared CLI database and fails the cell rather than
// returning an error, so each arm above reads as one statement.
func openFlagDB(t *testing.T, logger *slog.Logger) (*wadjet.DB, func()) {
	t.Helper()
	db, release, err := openSharedDB(context.Background(), logger)
	if err != nil {
		t.Fatalf("openSharedDB: %v", err)
	}
	return db, release
}
