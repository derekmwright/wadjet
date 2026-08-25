package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #550's SQL-level gate: a RIGHT or FULL OUTER join must answer the same over
// a build that EVICTED partitions to disk as over one that stayed resident.
//
// The engine reaches the eviction path only under a memory budget, so the
// budget IS the variable: the same tables, the same SQL, three budgets. The
// unbudgeted arm is the reference answer and is itself checked against an
// expectation computed from the fixture, so two budgets agreeing on a wrong
// answer fails here rather than passing as "invariant".
//
// Before the fix the budgeted arms did not answer at all — FlushUnmatched
// dereferenced the h.buildBatches slot spillOneInMemoryPartition had nil'd and
// the query failed with a recovered nil-pointer panic (ADR-0019 contained it,
// so the process survived and the client got XX000). Making it not panic is
// only half: the evicted partition's build rows still have to come OUT, from
// their spilled form, exactly once.
//
// Key types are the axis that separates the two key encodings and the two
// hash tables behind them — INT64 and DECIMAL take the integer/dual-integer
// paths, STRING/CIDR/UUID the serialized-key path — because the eviction is
// keyed by partition and the partition function differs between them.

const ojBuildRows = 40000

// ojNullEvery makes every seventh build key NULL. A NULL key matches nothing,
// so a RIGHT join owes every one of those rows a NULL-padded output row, and
// every NULL hashes to partition 0 — the partition the eviction picks first
// when it is the largest.
const ojNullEvery = 7

type ojKeyType struct {
	name   string
	col    parquet.Column // the key column, named "k"; "kk" is its NOT NULL twin
	keyFor func(i int) any
}

func ojKeyTypes() []ojKeyType {
	return []ojKeyType{
		{"int64", parquet.Column{Type: parquet.TypeInt64}, func(i int) any { return int64(i) }},
		{"string", parquet.Column{Type: parquet.TypeString}, func(i int) any { return fmt.Sprintf("key-%08d", i) }},
		{"decimal", parquet.Column{Type: parquet.TypeDecimal, Precision: 18, Scale: 4},
			func(i int) any { return float64(i) + 0.25 }},
		{"cidr", parquet.Column{Type: parquet.TypeCIDR},
			func(i int) any { return fmt.Sprintf("10.%d.%d.%d/32", i>>16&0xff, i>>8&0xff, i&0xff) }},
		{"uuid", parquet.Column{Type: parquet.TypeUUID},
			func(i int) any { return fmt.Sprintf("00000000-0000-4000-8000-%012x", i) }},
	}
}

// ojProbeEvery picks the build rows the probe side carries. Coprime with
// ojNullEvery and with the partition count, so matched rows land in resident
// and evicted partitions alike.
const ojProbeEvery = 977

func ojIsNullKeyed(i int) bool { return i%ojNullEvery == 0 }

// ojProbed reports whether build row i has a probe partner on the NULLABLE
// key. A NULL-keyed row never does — NULL equals nothing, not even another
// NULL (#459).
func ojProbed(i int) bool { return i%ojProbeEvery == 0 && !ojIsNullKeyed(i) }

// ojProbedNotNull is the same question for the NOT NULL twin key, where every
// probed row matches.
func ojProbedNotNull(i int) bool { return i%ojProbeEvery == 0 }

func ojOpen(t *testing.T, budget int64) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test", MemoryBudget: budget})
	if err != nil {
		t.Fatalf("open (budget=%d): %v", budget, err)
	}
	t.Cleanup(func() { db.Close() })

	load := func(name string, cols []parquet.Column, rows []map[string]any) {
		t.Helper()
		sch := parquet.Schema{Columns: cols}
		if err := db.CreateTable(ctx, name, sch, nil); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		ing := db.NewIngester(name, sch, nil, ingest.Config{MaxBufferRows: len(rows) + 1, RowGroupSize: 4096})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatalf("ingest %s: %v", name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", name, err)
		}
	}

	for _, kt := range ojKeyTypes() {
		k := kt.col
		k.Name, k.Nullable = "k", true
		kk := kt.col
		kk.Name, kk.Nullable = "kk", false
		buildCols := []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			k, kk,
			// The payload is what makes the build big enough to evict at a
			// 4 MiB budget. Without it the whole fixture is resident and this
			// gate silently tests nothing.
			{Name: "pad", Type: parquet.TypeString},
		}
		buildRows := make([]map[string]any, 0, ojBuildRows)
		for i := 0; i < ojBuildRows; i++ {
			row := map[string]any{
				"id":  int64(i),
				"kk":  kt.keyFor(i),
				"pad": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
			}
			if ojIsNullKeyed(i) {
				row["k"] = nil
			} else {
				row["k"] = kt.keyFor(i)
			}
			buildRows = append(buildRows, row)
		}
		pk := kt.col
		pk.Name, pk.Nullable = "pk", true
		probeCols := []parquet.Column{{Name: "pid", Type: parquet.TypeInt64}, pk}
		var probeRows []map[string]any
		for i := 0; i < ojBuildRows; i += ojProbeEvery {
			probeRows = append(probeRows, map[string]any{"pid": int64(i), "pk": kt.keyFor(i)})
		}
		load("ojb_"+kt.name, buildCols, buildRows)
		load("ojp_"+kt.name, probeCols, probeRows)
	}
	return db
}

func TestOuterJoinOverASpilledBuildAnswersLikeAResidentOne(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate ingests a fixture large enough to force grace-partition eviction")
	}
	ctx := context.Background()

	// The reference arm. No budget means no SpillManager on the join, so the
	// build never partitions on arrival and never evicts.
	resident := ojOpen(t, 0)

	type arm struct {
		name   string
		db     *DB
		budget int64
	}
	arms := []arm{
		{"budget_4MiB", ojOpen(t, 4<<20), 4 << 20},
		{"budget_16MiB", ojOpen(t, 16<<20), 16 << 20},
	}

	for _, kt := range ojKeyTypes() {
		b, p := "ojb_"+kt.name, "ojp_"+kt.name
		queries := []struct {
			name string
			sql  string
			// want is the reference expectation, computed from the fixture
			// rather than read off the engine.
			wantRows  int
			wantPairs int
		}{
			{"right_nullable_key",
				fmt.Sprintf("SELECT b.id AS id, p.pid AS pid FROM %s p RIGHT JOIN %s b ON b.k = p.pk ORDER BY id", p, b),
				ojBuildRows, ojCount(ojProbed)},
			{"right_notnull_key",
				fmt.Sprintf("SELECT b.id AS id, p.pid AS pid FROM %s p RIGHT JOIN %s b ON b.kk = p.pk ORDER BY id", p, b),
				ojBuildRows, ojCount(ojProbedNotNull)},
			{"full_outer_nullable_key",
				fmt.Sprintf("SELECT b.id AS id, p.pid AS pid FROM %s p FULL OUTER JOIN %s b ON b.k = p.pk ORDER BY id, pid", p, b),
				// The probe rows that matched nothing are owed a row of their
				// own by FULL OUTER: those are exactly the probed build rows
				// whose NULLABLE key is NULL.
				ojBuildRows + ojCount(ojProbedNotNull) - ojCount(ojProbed), ojCount(ojProbed)},
			{"full_outer_notnull_key",
				fmt.Sprintf("SELECT b.id AS id, p.pid AS pid FROM %s p FULL OUTER JOIN %s b ON b.kk = p.pk ORDER BY id, pid", p, b),
				ojBuildRows, ojCount(ojProbedNotNull)},
			// The LEFT control. Its build side is the one a distributed plan
			// REPLICATES, and flushing unmatched build rows is unsound there
			// (#348) — a LEFT join must not acquire any build-side flush from
			// this fix, so its answer must be exactly the probe's own rows.
			{"left_control",
				fmt.Sprintf("SELECT p.pid AS pid, b.id AS id FROM %s p LEFT JOIN %s b ON b.kk = p.pk ORDER BY pid", p, b),
				ojCount(ojProbedNotNull), ojCount(ojProbedNotNull)},
		}

		for _, q := range queries {
			t.Run(kt.name+"/"+q.name, func(t *testing.T) {
				want := ojRun(t, ctx, resident, q.sql)
				if len(want) != q.wantRows {
					t.Fatalf("the RESIDENT arm returned %d rows, want %d — the reference answer is "+
						"wrong, so agreeing with it proves nothing", len(want), q.wantRows)
				}
				var pairs int
				for _, r := range want {
					if r["pid"] != nil && r["id"] != nil {
						pairs++
					}
				}
				if pairs != q.wantPairs {
					t.Fatalf("the RESIDENT arm matched %d pairs, want %d", pairs, q.wantPairs)
				}

				for _, a := range arms {
					evictedBefore := exec.JoinPartitionsEvicted.Load()
					got := ojRun(t, ctx, a.db, q.sql)
					evicted := exec.JoinPartitionsEvicted.Load() - evictedBefore
					if a.budget == 4<<20 && evicted == 0 {
						t.Fatalf("%s: no grace partition was evicted over %d build rows — this arm is "+
							"not exercising the spilled build the gate exists for", a.name, ojBuildRows)
					}
					if len(got) != len(want) {
						t.Fatalf("%s: %d rows, want %d (the resident answer); %d partitions evicted",
							a.name, len(got), len(want), evicted)
					}
					for i := range want {
						if !ojSameRow(want[i], got[i]) {
							t.Fatalf("%s: row %d = %v, want %v (the resident answer); %d partitions evicted",
								a.name, i, got[i], want[i], evicted)
						}
					}
				}
			})
		}
	}
}

func ojCount(pred func(int) bool) int {
	n := 0
	for i := 0; i < ojBuildRows; i++ {
		if pred(i) {
			n++
		}
	}
	return n
}

func ojRun(t *testing.T, ctx context.Context, db *DB, sql string) []map[string]any {
	t.Helper()
	res, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("query error: %v\n  SQL: %s", err, sql)
	}
	return res.Rows
}

func ojSameRow(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, av := range a {
		bv, ok := b[k]
		if !ok {
			return false
		}
		if fmt.Sprintf("%v", av) != fmt.Sprintf("%v", bv) {
			return false
		}
	}
	return true
}
