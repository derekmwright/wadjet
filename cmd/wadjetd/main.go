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
	if err := cli.NewRootCmd(clid.ServeCmd()).Execute(); err != nil {
		os.Exit(1)
	}
}
