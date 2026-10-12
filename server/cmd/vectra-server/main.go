// vectra-server starts a Vectra server node (raft replication + HTTP API).
//
// First node:  vectra-server -id n1 -http :8080 -raft :9000 -data /data/n1
// Join cluster: vectra-server -id n2 -http :8081 -raft :9001 -data /data/n2 -join http://n1:8080
package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/projanvil/vectra/server"
)

func main() {
	id := flag.String("id", "", "node ID (required, bound to the data directory)")
	httpAddr := flag.String("http", ":8080", "HTTP API listen address")
	raftAddr := flag.String("raft", ":9000", "raft peer address")
	dataDir := flag.String("data", "", "data directory (required: raft.db/snapshots/meta.json)")
	join := flag.String("join", "", "seed node HTTP address (empty = single-node bootstrap; anti-split-brain: non-empty Join never bootstraps)")
	sealInterval := flag.Duration("seal-interval", server.DefaultSealInterval, "publish throttle interval for large graphs")
	flag.Parse()

	if *id == "" || *dataDir == "" {
		fmt.Fprintln(os.Stderr, "usage: vectra-server -id n1 -data /data/n1 [-http :8080 -raft :9000 -join SEED]")
		os.Exit(2)
	}

	n, err := server.New(server.Config{
		NodeID:       *id,
		HTTPAddr:     *httpAddr,
		RaftAddr:     *raftAddr,
		DataDir:      *dataDir,
		Join:         *join,
		SealInterval: *sealInterval,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "vectra-server %s http=%s raft=%s data=%s\n", *id, *httpAddr, *raftAddr, *dataDir)
	if err := http.ListenAndServe(*httpAddr, n.Handler()); err != nil {
		n.Close()
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
