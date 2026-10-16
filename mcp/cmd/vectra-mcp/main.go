// vectra-mcp is the Vectra MCP server binary: embeds the engine directly
// (no network dependency).
//
// Transport modes:
//
//	vectra-mcp                      # stdio (local mount for Claude Desktop / Claude Code)
//	vectra-mcp -http :8080          # Streamable HTTP (/mcp path, remote sharing)
//
// Persistence:
//
//	-snapshot path.vg   # snapshot mode: loaded at startup, saved after each seal (default)
//	-wal dir            # durable WAL session: every write logged (fsync per batch),
//	                    # seal = merge + checkpoint; restart on the same dir recovers
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
	walDir := flag.String("wal", "", "WAL directory: durable session (takes precedence over -snapshot; the snapshot path then names the db file)")
	flag.Parse()

	opts := mcpserver.Options{SnapshotPath: *snapshot, WALDir: *walDir}
	// newService performs the load (snapshot) / open (WAL) itself; the
	// placeholder is only the fallback for a missing snapshot file.
	g := vectra.New()

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
