// vectra-mcp 是 Vectra 的 MCP 服务二进制：直接嵌入引擎（无网络依赖）。
//
// 传输模式：
//
//	vectra-mcp                      # stdio（Claude Desktop / Claude Code 本地挂载）
//	vectra-mcp -http :8080          # Streamable HTTP（/mcp 路径，远程共享）
//
// 持久化：-snapshot path.vg 启动加载、每次 seal 后保存。
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
	httpAddr := flag.String("http", "", "HTTP 监听地址（空 = stdio 模式）")
	snapshot := flag.String("snapshot", "", "快照文件路径（启动加载、seal 后保存）")
	flag.Parse()

	opts := mcpserver.Options{SnapshotPath: *snapshot}
	g := vectra.New()
	if *snapshot != "" {
		if loaded, err := vectra.LoadFile(*snapshot); err == nil {
			g = loaded.Clone() // 恢复为可写继续演进
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
