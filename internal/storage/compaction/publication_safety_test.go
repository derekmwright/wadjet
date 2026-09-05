package compaction

// Publication safety: a compaction commit is ONE conditional manifest
// transaction, validated against the input file identities and the
// delete-marker snapshot it was cut from (ADR-0020, "Compaction publication is
// one validated transaction"). It either publishes a replacement that reflects
// every committed delete, or it fails and leaves the previous snapshot intact;
// and an object is physically retired only with proof that no live manifest
// references it.
//
// Every cell here drives a DETERMINISTIC schedule — a second operation is run
// from inside the object store's Put or the KV's Get, at the exact instant the
// defect needs — so there are no sleeps and nothing probabilistic. The four
// defects are #893 (publication failure emptied the table), #894 (a committed
// DELETE was undone by the publication), #895 (competing compactors published
// duplicate rows), #896 (deferred cleanup deleted a live table's object).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// ---------------------------------------------------------------- #893

// TestAFailedReplacementPublicationLeavesTheOldSnapshotQueryable is #893.
//
// The publication used to be two CAS writes — RemoveFiles, then AddNewFiles —
// and a failure at the second left the table with the inputs gone and the
// replacement unpublished: zero visible rows, and unrecoverable by retrying,
// because the compactor selects its inputs from the manifest it just emptied.
//
// The fault is injected at the write that PUBLISHES THE REPLACEMENT (the
// payload carrying a compacted_/rewrite_ output), which is the same instant
// under either design: under the old one it is the second of two writes and
// the first has already emptied the table; under the one transaction there is
// nothing to half-do.
func TestAFailedReplacementPublicationLeavesTheOldSnapshotQueryable(t *testing.T) {
	t.Run("ordinary", func(t *testing.T) {
		f := pubSetup(t, 2)
		f.kv.beforeUpdate = pubFailAtReplacementPublication(t)

		_, err := f.compactor().CompactTable(context.Background(), "events")
		if err == nil {
			t.Fatal("the injected publication failure was not reached")
		}
		t.Logf("compaction error=%v", err)
		pubWant(t, f.ids(t), []int64{1, 2, 3, 4}, "after a failed publication")
		if got := f.manifestPaths(t); !reflect.DeepEqual(got, f.paths) {
			t.Errorf("the manifest must still name its inputs: got %v, want %v", got, f.paths)
		}
		for _, p := range f.paths {
			if _, hErr := f.store.Head(context.Background(), "test-bucket", p); hErr != nil {
				t.Errorf("input %q must still exist: %v", p, hErr)
			}
		}
	})

	t.Run("rewrite", func(t *testing.T) {
		f := pubSetup(t, 2)
		f.kv.beforeUpdate = pubFailAtReplacementPublication(t)

		_, err := f.compactor().RewriteTable(context.Background(), "events")
		if err == nil {
			t.Fatal("the injected publication failure was not reached")
		}
		pubWant(t, f.ids(t), []int64{1, 2, 3, 4}, "after a failed rewrite publication")
		if got := f.manifestPaths(t); !reflect.DeepEqual(got, f.paths) {
			t.Errorf("the manifest must still name its inputs: got %v, want %v", got, f.paths)
		}
	})

	t.Run("all_rows_deleted", func(t *testing.T) {
		// Nothing is uploaded when every row is delete-filtered away, so the
		// publication is a pure removal — and it is still one transaction that
		// either happens or does not.
		f := pubSetup(t, 2)
		ctx := context.Background()
		for _, p := range f.paths {
			if err := f.cat.CommitDML(ctx, "events", nil,
				[]catalog.DeleteMarker{{FilePath: p, RowIndices: []int64{0, 1}}}); err != nil {
				t.Fatal(err)
			}
		}
		f.kv.beforeUpdate = func(key string, payload []byte) error {
			if !strings.Contains(key, "manifest.events") {
				return nil
			}
			var m catalog.PartitionManifest
			if err := json.Unmarshal(payload, &m); err != nil {
				t.Fatal(err)
			}
			n := 0
			for _, p := range m.Partitions {
				n += len(p.Files)
			}
			if n < len(f.paths) {
				return errors.New("injected publication failure")
			}
			return nil
		}
		if _, err := f.compactor().CompactTable(ctx, "events"); err == nil {
			t.Fatal("the injected publication failure was not reached")
		}
		if got := f.manifestPaths(t); !reflect.DeepEqual(got, f.paths) {
			t.Errorf("the manifest must still name its inputs: got %v, want %v", got, f.paths)
		}
		m, err := f.cat.GetManifest(ctx, "events")
		if err != nil {
			t.Fatal(err)
		}
		marked := catalog.DeletedRowsByFile(m.DeleteMarkers)
		for _, p := range f.paths {
			if len(marked[p]) != 2 {
				t.Errorf("the markers on %q must survive a failed publication: got %v", p, marked[p])
			}
		}
	})
}

// pubFailAtReplacementPublication refuses the manifest write that carries a
// compaction or rewrite OUTPUT.
func pubFailAtReplacementPublication(t *testing.T) func(string, []byte) error {
	t.Helper()
	return func(key string, payload []byte) error {
		if !strings.Contains(key, "manifest.events") {
			return nil
		}
		var m catalog.PartitionManifest
		if err := json.Unmarshal(payload, &m); err != nil {
			t.Fatal(err)
		}
		for _, p := range m.Partitions {
			for _, e := range p.Files {
				if strings.Contains(e.Path, "compacted_") || strings.Contains(e.Path, "rewrite_") {
					return errors.New("injected replacement publication failure")
				}
			}
		}
		return nil
	}
}

// TestNoReaderEverSeesAPartialCompactionPublication is #893's other half: even
// when every write SUCCEEDS, the intermediate manifest between a removal and
// an addition is a revision a reader can land on and answer from.
//
// Every manifest payload written while compaction runs is decoded and read as
// a reader at that revision would read it. The row set must never move.
func TestNoReaderEverSeesAPartialCompactionPublication(t *testing.T) {
	for _, mode := range []string{"compact", "rewrite"} {
		t.Run(mode, func(t *testing.T) {
			f := pubSetup(t, 2)
			want := []int64{1, 2, 3, 4}
			revisions := 0
			f.kv.beforeUpdate = func(key string, payload []byte) error {
				if !strings.Contains(key, "manifest.events") {
					return nil
				}
				var m catalog.PartitionManifest
				if err := json.Unmarshal(payload, &m); err != nil {
					t.Fatal(err)
				}
				revisions++
				pubWant(t, pubIDsOf(t, f.store, &m), want,
					fmt.Sprintf("manifest revision %d written during %s", revisions, mode))
				return nil
			}
			var err error
			if mode == "rewrite" {
				_, err = f.compactor().RewriteTable(context.Background(), "events")
			} else {
				_, err = f.compactor().CompactTable(context.Background(), "events")
			}
			if err != nil {
				t.Fatal(err)
			}
			if revisions == 0 {
				t.Fatal("no manifest revision was observed: the schedule did not run")
			}
			pubWant(t, f.ids(t), want, "after "+mode)
		})
	}
}

// ---------------------------------------------------------------- #894

// TestACommittedDeleteSurvivesTheCompactionThatRacedIt is #894.
//
// The DELETE commits through Catalog.CommitDML — which validates its own
// markers against the manifest it commits into and correctly accepts them,
// because the file it names is still live — from inside the compactor's
// output Put. So the output already contains the row the DELETE removed, and
// the publication that follows used to undo the DELETE while both operations
// reported success.
func TestACommittedDeleteSurvivesTheCompactionThatRacedIt(t *testing.T) {
	for _, mode := range []string{"ordinary", "rewrite", "forced_gc"} {
		t.Run(mode, func(t *testing.T) {
			f := pubSetup(t, 2)
			ctx := context.Background()
			want := []int64{1, 3, 4}
			if mode == "forced_gc" {
				// An aged marker gives the GC sweep something to rewrite.
				if err := f.cat.CommitDML(ctx, "events", nil,
					[]catalog.DeleteMarker{{FilePath: f.paths[0], RowIndices: []int64{0}}}); err != nil {
					t.Fatal(err)
				}
				want = []int64{3, 4}
			}
			f.store.afterPut = func() {
				if err := f.cat.CommitDML(ctx, "events", nil,
					[]catalog.DeleteMarker{{FilePath: f.paths[0], RowIndices: []int64{1}}}); err != nil {
					t.Fatal(err)
				}
				t.Logf("DELETE committed; IDs before the compactor publishes=%v", f.ids(t))
			}

			c := f.compactor()
			var err error
			switch mode {
			case "forced_gc":
				err = c.ForceCompactFile(ctx, "events", f.paths[0], map[int64]bool{0: true})
			case "rewrite":
				_, err = c.RewriteTable(ctx, "events")
			default:
				_, err = c.CompactTable(ctx, "events")
			}
			if err != nil {
				t.Fatal(err)
			}
			pubWant(t, f.ids(t), want, mode)

			// Rows alone cannot tell "the compactor detected the conflict and
			// replanned" from "the compactor happened not to run". The counter
			// is the routed-flag equivalent.
			if c.PublicationConflicts() == 0 {
				t.Error("the racing DELETE must have refused at least one publication")
			}

			// The next GC sweep: no marker may be left naming a file that is
			// gone, and the row set must not move.
			targets, orphans, gErr := f.cat.GCDeleteMarkers(ctx, "events", -time.Second)
			if gErr != nil {
				t.Fatal(gErr)
			}
			t.Logf("next GC: rewrite targets=%v orphan paths=%v", targets, orphans)
			if len(orphans) > 0 {
				t.Errorf("a committed DELETE was left as an orphan marker, which makes the loss permanent: %v", orphans)
			}
			pubWant(t, f.ids(t), want, mode+" after the next GC sweep")
		})
	}
}

// ---------------------------------------------------------------- #895

// TestCompetingCompactorsPublishEachRowExactlyOnce is #895.
//
// Two independent Compactor instances over one Catalog and one store, the
// second scheduled between the first's upload and its publication. Both used
// to succeed: RemoveFiles treated the already-consumed inputs as success and
// AddNewFiles accepted the second distinct output beside the first, so the
// next pass merged both copies and every row appeared twice.
func TestCompetingCompactorsPublishEachRowExactlyOnce(t *testing.T) {
	type arm struct{ first, second string }
	for _, a := range []arm{
		{"ordinary", "ordinary"},
		{"forced_gc", "forced_gc"},
		{"ordinary", "forced_gc"},
		{"forced_gc", "ordinary"},
		{"rewrite", "ordinary"},
		{"ordinary", "rewrite"},
	} {
		t.Run(a.first+"_vs_"+a.second, func(t *testing.T) {
			f := pubSetup(t, 2)
			// Retire compacted-away inputs inline, so every engine-written
			// object still in the store at the end is either live in the
			// manifest or a leak.
			f.grace = -time.Second
			ctx := context.Background()
			want := []int64{1, 2, 3, 4}
			needsMarker := a.first == "forced_gc" || a.second == "forced_gc"
			if needsMarker {
				if err := f.cat.CommitDML(ctx, "events", nil,
					[]catalog.DeleteMarker{{FilePath: f.paths[0], RowIndices: []int64{0}}}); err != nil {
					t.Fatal(err)
				}
				want = []int64{2, 3, 4}
			}
			run := func(c *Compactor, mode string) error {
				switch mode {
				case "forced_gc":
					return c.ForceCompactFile(ctx, "events", f.paths[0], map[int64]bool{0: true})
				case "rewrite":
					_, e := c.RewriteTable(ctx, "events")
					return e
				default:
					_, e := c.CompactTable(ctx, "events")
					return e
				}
			}

			second := f.compactor()
			f.store.afterPut = func() {
				if err := run(second, a.second); err != nil {
					t.Fatal(err)
				}
				t.Logf("the second compactor committed; visible IDs=%v", f.ids(t))
			}
			first := f.compactor()
			if err := run(first, a.first); err != nil {
				t.Fatal(err)
			}

			pubWant(t, f.ids(t), want, a.first+" vs "+a.second)

			if first.PublicationConflicts()+second.PublicationConflicts() == 0 {
				t.Error("one of the two compactors must have had its publication refused")
			}

			// The loser deleted its own output: a conflict is decided before
			// the CAS, so nothing was published and the bytes are unreachable.
			live := f.manifestPaths(t)
			for _, obj := range f.engineWrittenObjects(t) {
				found := false
				for _, p := range live {
					if p == obj {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("the refused output %q was left in the store; a losing compactor must discard it", obj)
				}
			}
		})
	}
}

// TestALostPublicationRaceIsReportedToTheOperator is the reporting half of
// #895/#894, and it is a gate on a LINE rather than on a row set for the
// reason the counter exists at all: a run that lost a publication race leaves
// the same rows behind as a run that had nothing to do, so the only thing that
// can tell an operator the two apart is what the run SAYS.
//
// The shape the round-1 review reproduced: `wadjet compact --rewrite` whose
// only group loses the race returns a nil error with an empty Failed, and the
// CLI printed "0 merges, 0 files removed, 0 created, 0 rows, 0 -> 0 bytes" —
// character for character what an already-migrated table prints. A rewrite
// reads its file list exactly once, so the skipped group is not retried inside
// the call; the operator has to run it again and nothing said so.
//
// The race here is real, not a hand-built Result: a second compactor commits
// from inside the first's output Put, exactly as the competing-writers cell
// drives it. Result.Summary is the function cmd/wadjet prints, and
// TestPrintCompactResultPrintsEverySummaryLine (cmd/wadjet) closes the other
// half of the chain.
func TestALostPublicationRaceIsReportedToTheOperator(t *testing.T) {
	for _, mode := range []string{"rewrite", "ordinary"} {
		t.Run(mode, func(t *testing.T) {
			f := pubSetup(t, 2)
			ctx := context.Background()

			second := f.compactor()
			f.store.afterPut = func() {
				if _, e := second.CompactTable(ctx, "events"); e != nil {
					t.Fatal(e)
				}
			}
			first := f.compactor()
			var result *Result
			var err error
			if mode == "rewrite" {
				result, err = first.RewriteTable(ctx, "events")
			} else {
				result, err = first.CompactTable(ctx, "events")
			}
			if err != nil {
				t.Fatal(err)
			}
			if result.PublicationConflicts == 0 {
				t.Fatal("the schedule did not produce a refused publication")
			}

			lines := result.Summary()
			joined := strings.Join(lines, "\n")
			if len(lines) < 2 {
				t.Fatalf("a run that lost a publication race must say so; got only %q", joined)
			}
			if !strings.Contains(joined, "refused because another writer changed the same files first") {
				t.Errorf("the summary does not name the refusal: %q", joined)
			}
			if result.PartitionsCompacted == 0 && !strings.Contains(joined, "run again") {
				t.Errorf("a run that published nothing must tell the operator to run again: %q", joined)
			}
			t.Logf("%s summary:\n%s", mode, joined)
		})
	}
}

// TestASuccessfulRunDoesNotTellTheOperatorToRunAgain is the boundary of the
// cell above, attempted from the other side: CompactTable REPLANS after a
// refusal, so a run that conflicted once and then compacted the partition has
// finished its work. Conditioning the advice on the conflict count alone would
// send an operator round a loop that has nothing left to do.
func TestASuccessfulRunDoesNotTellTheOperatorToRunAgain(t *testing.T) {
	f := pubSetup(t, 2)
	ctx := context.Background()

	// A DELETE from inside the output's Put refuses the first publication;
	// the pass loop then replans and compacts for real.
	f.store.afterPut = func() {
		if e := f.cat.CommitDML(ctx, "events", nil,
			[]catalog.DeleteMarker{{FilePath: f.paths[0], RowIndices: []int64{1}}}); e != nil {
			t.Fatal(e)
		}
	}
	c := f.compactor()
	result, err := c.CompactTable(ctx, "events")
	if err != nil {
		t.Fatal(err)
	}
	if result.PublicationConflicts == 0 {
		t.Fatal("the schedule did not produce a refused publication")
	}
	if result.PartitionsCompacted == 0 {
		t.Fatal("the replan did not compact the partition")
	}
	joined := strings.Join(result.Summary(), "\n")
	if !strings.Contains(joined, "refused because another writer changed the same files first") {
		t.Errorf("the refusal is still worth reporting: %q", joined)
	}
	if strings.Contains(joined, "run again") {
		t.Errorf("this run finished its work; it must not ask for a re-run: %q", joined)
	}
	t.Logf("summary:\n%s", joined)
}

// TestAnUneventfulRunSaysOnlyWhatItDid keeps the note off the ordinary path —
// a line that appears every time is a line an operator stops reading.
func TestAnUneventfulRunSaysOnlyWhatItDid(t *testing.T) {
	f := pubSetup(t, 2)
	result, err := f.compactor().CompactTable(context.Background(), "events")
	if err != nil {
		t.Fatal(err)
	}
	if result.PublicationConflicts != 0 {
		t.Fatalf("nothing raced this run, got %d conflicts", result.PublicationConflicts)
	}
	if lines := result.Summary(); len(lines) != 1 {
		t.Errorf("an uneventful run prints one line, got %q", lines)
	}
}
