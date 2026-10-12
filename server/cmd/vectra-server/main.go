// vectra-server 启动一个 Vectra 服务节点（raft 复制 + HTTP API）。
//
// 首节点：  vectra-server -id n1 -http :8080 -raft :9000 -data /data/n1
// 加入集群：vectra-server -id n2 -http :8081 -raft :9001 -data /data/n2 -join http://n1:8080
package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"

	"github.com/projanvil/vectra/server"
)

func main() {
	id := flag.String("id", "", "节点 ID（必填，与数据目录绑定）")
	httpAddr := flag.String("http", ":8080", "HTTP API 监听地址")
	raftAddr := flag.String("raft", ":9000", "raft 对等地址")
	dataDir := flag.String("data", "", "数据目录（必填：raft.db/snapshots/meta.json）")
	join := flag.String("join", "", "seed 节点 HTTP 地址（空 = 单节点引导；防脑裂：Join 非空绝不引导）")
	sealInterval := flag.Duration("seal-interval", server.DefaultSealInterval, "大图发布节流间隔")
	flag.Parse()

	if *id == "" || *dataDir == "" {
		fmt.Fprintln(os.Stderr, "用法: vectra-server -id n1 -data /data/n1 [-http :8080 -raft :9000 -join SEED]")
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
