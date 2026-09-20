// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// ARC FR — AN INPUT THAT CAN BE READ ONCE IS READ ONCE.
//
// The plan-time schema read OPENS the reader's input and the execution opens
// it again. For a regular file that is a second read of the same bytes. For a
// FIFO, a character device (`/dev/stdin`), a socket or a process substitution
// (`/dev/fd/63`) there is no second read: the plan-time open consumes the one
// stream, and the execution's open either meets a different stream (zero rows
// where the statement answered three) or BLOCKS FOREVER on a writer that will
// never come — `open(2)` is not interruptible by the statement's context.
//
// So an input that is not a REGULAR file publishes no plan-time schema and is
// opened exactly once, which is what every reader did through v0.23.0. The
// measurement is the statement's own answer inside a deadline; a hang is a
// failure of this gate, not a timeout of the suite.
//
// At `2c6e56eb` (the arc's tip before this repair) `read_json` hung on all
// three replicates and `read_csv` hung on two of three and answered COUNT=0
// on the third; at `0c0d33b6` and here both answer 3.
func TestArcFRAnInputThatCanBeReadOnceIsReadOnce(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, kind := range []string{"csv", "json"} {
		kind := kind
		t.Run("a_fifo_fed_read_"+kind, func(t *testing.T) {
			fifo := filepath.Join(dir, "once."+kind)
			if err := syscall.Mkfifo(fifo, 0o600); err != nil {
				t.Skipf("this platform has no FIFO: %v", err)
			}
			body := "a\n1\n2\n3\n"
			if kind == "json" {
				body = "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n"
			}
			// ONE writer with the content; afterwards a few opens that write
			// nothing, so a second reader gets an immediate EOF rather than
			// blocking the test itself.
			go func() {
				if f, err := os.OpenFile(fifo, os.O_WRONLY, 0); err == nil {
					_, _ = f.WriteString(body)
					_ = f.Close()
				}
				for i := 0; i < 4; i++ {
					if f, err := os.OpenFile(fifo, os.O_WRONLY, 0); err == nil {
						_ = f.Close()
					}
				}
			}()

			done := make(chan string, 1)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						done <- fmt.Sprintf("PANIC %v", r)
					}
				}()
				res, err := db.Query(ctx,
					fmt.Sprintf("SELECT COUNT(*) AS n FROM read_%s('%s')", kind, fifo))
				if err != nil {
					done <- "ERR " + strings.Join(strings.Fields(err.Error()), " ")
					return
				}
				done <- fmt.Sprintf("COUNT=%v", res.Cells(0)[0])
			}()

			select {
			case got := <-done:
				if got != "COUNT=3" {
					t.Errorf("a FIFO holding three rows answered %s, want COUNT=3 "+
						"(the same answer at 0c0d33b6, before a plan-time read existed)", got)
				}
			case <-time.After(20 * time.Second):
				t.Errorf("the statement did not return in 20s: the plan-time read consumed " +
					"the stream and the execution's open is waiting for a writer that will " +
					"never come")
			}
		})
	}

	// The control that makes the two cells above mean something: a REGULAR
	// file still gets its plan-time schema, so the repair narrows the read
	// rather than removing it.
	t.Run("a_regular_file_still_publishes_its_schema", func(t *testing.T) {
		p := filepath.Join(dir, "regular.csv")
		if err := os.WriteFile(p, []byte("a,b\n1,p\n2,q\n3,r\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		before := readerSchemaReadsForTest()
		got, err := ptRenderQuery(ctx, db, `SELECT f.* FROM read_csv('`+p+`') AS f ORDER BY f.a`)
		if err != nil {
			t.Fatalf("a regular file lost its plan-time schema: %v", err)
		}
		if got != "[a,b] 1|p;2|q;3|r" {
			t.Errorf("answered %s, want [a,b] 1|p;2|q;3|r", got)
		}
		if readerSchemaReadsForTest() == before {
			t.Error("the planner read NO schema for a regular file, so the FIFO cells above " +
				"prove nothing — the repair removed the read instead of narrowing it")
		}
	})
}
