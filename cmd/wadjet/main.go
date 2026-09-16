// Command wadjet is the Wadjet command-line interface.
package main

import (
	"os"

	"github.com/derekmwright/wadjet/internal/cli"
)

func main() {
	if err := cli.NewRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}
