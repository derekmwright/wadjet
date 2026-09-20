// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// ARC FR — AN INPUT THAT CAN BE READ ONCE IS READ ONCE, ON THE WIRE.
//
// `wadjet.TestArcFRAnInputThatCanBeReadOnceIsReadOnce` holds this on the
// embedded door. It is asserted here too because the failure it guards is a
// BLOCKING `open(2)`: on the wire that is a connection the client cannot get
// an answer or an error out of, and the statement's context does not
// interrupt it. A door that hangs is not the same defect as a door that
// answers wrongly, and only the wire shows which one a client sees.
//
// At `2c6e56eb` this hangs; at `0c0d33b6` and here it answers 3.
func TestArcFRAFifoFedReaderAnswersOnTheWire(t *testing.T) {
	ctx := context.Background()
	rig := sec4NewRig(t, ctx)
	dir := t.TempDir()

	conn, err := pgx.Connect(ctx, fmt.Sprintf(
		"postgres://wadjet:%s@%s/wadjet?sslmode=disable", sec4Ops, rig.pgAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)

	for _, kind := range []string{"csv", "json"} {
		kind := kind
		t.Run("a_fifo_fed_read_"+kind, func(t *testing.T) {
			fifo := filepath.Join(dir, "wire_once."+kind)
			if err := syscall.Mkfifo(fifo, 0o600); err != nil {
				t.Skipf("this platform has no FIFO: %v", err)
			}
			body := "a\n1\n2\n3\n"
			if kind == "json" {
				body = "{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n"
			}
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
				var n int64
				err := conn.QueryRow(ctx,
					fmt.Sprintf("SELECT COUNT(*) AS n FROM read_%s('%s')", kind, fifo)).Scan(&n)
				if err != nil {
					done <- "ERR " + strings.Join(strings.Fields(err.Error()), " ")
					return
				}
				done <- fmt.Sprintf("COUNT=%d", n)
			}()

			select {
			case got := <-done:
				if got != "COUNT=3" {
					t.Errorf("the wire answered %s over a FIFO holding three rows, want COUNT=3",
						got)
				}
			case <-time.After(20 * time.Second):
				t.Errorf("the wire did not answer in 20s: the plan-time read consumed the " +
					"stream and the execution's open is waiting for a writer that will never come")
			}
		})
	}
}
