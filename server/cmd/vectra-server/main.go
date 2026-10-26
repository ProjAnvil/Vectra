// vectra-server starts a Vectra server node (raft replication + HTTP API).
//
// First node:  vectra-server -id n1 -http :8080 -raft :9000 -data /data/n1
// Join cluster: vectra-server -id n2 -http :8081 -raft :9001 -data /data/n2 -join http://n1:8080
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

	"github.com/projanvil/vectra/server"
)

func main() {
	id := flag.String("id", "", "node ID (required, bound to the data directory)")
	httpAddr := flag.String("http", ":8080", "HTTP API listen address")
	raftAddr := flag.String("raft", ":9000", "raft peer address")
	dataDir := flag.String("data", "", "data directory (required: raft.db/snapshots/meta.json)")
	join := flag.String("join", "", "seed node HTTP address (empty = single-node bootstrap; anti-split-brain: non-empty Join never bootstraps)")
	authTokenFile := flag.String("auth-token-file", "", "file of bearer tokens, one per line (empty = auth disabled)")
	sealInterval := flag.Duration("seal-interval", server.DefaultSealInterval, "publish throttle interval for large graphs")
	flag.Parse()

	if *id == "" || *dataDir == "" {
		fmt.Fprintln(os.Stderr, "usage: vectra-server -id n1 -data /data/n1 [-http :8080 -raft :9000 -join SEED]")
		os.Exit(2)
	}

	n, err := server.New(server.Config{
		NodeID:        *id,
		HTTPAddr:      *httpAddr,
		RaftAddr:      *raftAddr,
		DataDir:       *dataDir,
		Join:          *join,
		AuthTokenFile: *authTokenFile,
		SealInterval:  *sealInterval,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "vectra-server %s http=%s raft=%s data=%s\n", *id, *httpAddr, *raftAddr, *dataDir)

	srv := &http.Server{
		Addr:              *httpAddr,
		Handler:           n.Handler(),
		ReadHeaderTimeout: 10 * time.Second, // slowloris guard; handlers set their own body/ctx deadlines
	}
	// SIGINT/SIGTERM: drain in-flight requests, then close raft + stores —
	// crash tolerance is the backstop, not the plan.
	done := make(chan struct{})
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "vectra-server: shutdown: %v\n", err)
		}
		n.Close()
		close(done)
	}()
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		n.Close()
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	<-done
}
