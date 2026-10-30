// vectra-cli entrypoint. Placeholder so the package builds before the REPL
// (client and rendering land first); the follow-up task replaces this file
// with the real flag parsing and REPL loop.
package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "vectra-cli: REPL not yet implemented")
	os.Exit(1)
}
