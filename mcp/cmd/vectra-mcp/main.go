// vectra-mcp is the Vectra MCP server binary: embeds the engine directly
// (no network dependency).
//
// Transport modes:
//
//	vectra-mcp                      # stdio (local mount for Claude Desktop / Claude Code)
//	vectra-mcp -http :8080          # Streamable HTTP (/mcp path, remote sharing)
//
// Persistence: -snapshot path.vg loads at startup and saves after each seal.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/projanvil/vectra"
	mcpserver "github.com/projanvil/vectra/mcp"
)

func main() {
	httpAddr := flag.String("http", "", "HTTP listen address (empty = stdio mode)")
	snapshot := flag.String("snapshot", "", "snapshot file path (loaded at startup, saved after seal)")
	flag.Parse()

	opts := mcpserver.Options{SnapshotPath: *snapshot}
	g := vectra.New()
	if *snapshot != "" {
		if loaded, err := vectra.LoadFile(*snapshot); err == nil {
			g = loaded.Clone() // restore to writable for further evolution
		}
	}

	if *httpAddr == "" {
		s := mcpserver.New(g, opts)
		if err := s.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	mux := http.NewServeMux()
	mux.Handle("/mcp", mcpserver.Handler(g, opts))
	fmt.Fprintf(os.Stderr, "vectra-mcp listening on http://%s/mcp\n", *httpAddr)
	if err := http.ListenAndServe(*httpAddr, mux); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
