// SPDX-License-Identifier: AGPL-3.0-only

// Command wadjetd is the Wadjet server daemon: the distributed run modes —
// standalone, coordinator and worker — on top of the same command tree the
// wadjet CLI carries.
package main

import (
	"os"

	"github.com/derekmwright/wadjet/internal/cli"
	"github.com/derekmwright/wadjet/internal/clid"
)

func main() {
	root := cli.NewRootCmd(clid.ServeCmd())
	// Same tree, different binary: the usage text has to name the program a
	// user actually typed, or every error message sends them to the other one.
	root.Use = "wadjetd"
	root.Short = "wadjetd — the Wadjet distributed server"
	root.Long = "The Wadjet distributed server: a coordinator that plans and dispatches, " +
		"workers that execute fragments, and NATS for coordination with object storage for results.\n\n" +
		"`wadjet` is the same command line over the engine in one process."
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
