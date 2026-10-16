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
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	srv := &http.Server{
		Addr:              *httpAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second, // slowloris guard; MCP sessions set their own deadlines
	}
	done := make(chan struct{})
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "vectra-mcp: shutdown: %v\n", err)
		}
		close(done)
	}()
	fmt.Fprintf(os.Stderr, "vectra-mcp listening on http://%s/mcp\n", *httpAddr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	<-done
}
