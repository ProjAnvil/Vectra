# Vectra 开发工作流
#
# 快速压测（结束后数据目录与报告自动删除，仓库零残留）：
#
#	make bench                          # small（1K 节点/1万边），引擎模式
#	make bench SIZE=medium              # 10万节点/100万边
#	make bench SIZE=large MODE=http     # 服务模式（raft + HTTP），100万/1000万
#	make bench SIZE=small WORKLOADS=neighbors,bfs2 DURATION=1s
#
# 调试需要保留数据时：make bench BENCH_EXTRA='-keep -datadir benchdata/debug'

.DEFAULT_GOAL := help

# bench 可调参数（make bench SIZE=... 命令行覆盖）：
#   SIZE        图规模 small|medium|large = 1K/1万、10万/100万、100万/1000万（边）
#   MODE        engine（进程内）| http（raft + HTTP 服务模式）
#   WORKERS     并发 worker 数
#   DURATION    每负载压测时长
#   WORKLOADS   负载子集，逗号分隔：neighbors,bfs2,bfs3,bfsall,path,reach（空=全部）
#   BENCH_EXTRA 其余 flag 透传给 vectra-bench，如 -seed 7 -warmup 1s
SIZE ?= small
MODE ?= engine
WORKERS ?= 8
DURATION ?= 5s
WORKLOADS ?=
BENCH_EXTRA ?=

# 仓库含三个 Go module：根（引擎）、server/（服务+压测）、mcp/（agent 网关）
MODULES := . server mcp

.PHONY: help bench build test vet fmt clean deepclean

help: ## 显示本帮助
	@awk -F ':.*## ' '/^[a-zA-Z][a-zA-Z_-]*:.*## / {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

bench: ## 压测：make bench [SIZE=medium] [MODE=http] [WORKERS=16] [DURATION=10s] [WORKLOADS=...] [BENCH_EXTRA='-seed 7']
	@case '$(SIZE)' in small|medium|large) ;; *) \
		printf '错误：SIZE=%s 无效（可选 small|medium|large）\n' '$(SIZE)' >&2; exit 2;; esac
	@case '$(MODE)' in engine|http) ;; *) \
		printf '错误：MODE=%s 无效（可选 engine|http）\n' '$(MODE)' >&2; exit 2;; esac
	@root=$$(pwd); mkdir -p benchdata; \
	dir=$$(mktemp -d "$$root/benchdata/run.XXXXXX"); \
	trap 'rm -rf "$$dir"; rmdir "$$root/benchdata" 2>/dev/null || true' EXIT INT TERM; \
	(cd server && go run ./cmd/vectra-bench \
	    -size $(SIZE) -mode $(MODE) -workers $(WORKERS) -duration $(DURATION) \
	    $(if $(WORKLOADS),-workloads $(WORKLOADS)) \
	    -datadir "$$dir/data" -out "$$dir/report.md" $(BENCH_EXTRA)) \
	  && { cat "$$dir/report.md" 2>/dev/null || true; }

build: ## 构建全部二进制到 bin/（vectra-server、vectra-bench、vectra-mcp）
	cd server && go build -o ../bin/vectra-server ./cmd/vectra-server
	cd server && go build -o ../bin/vectra-bench  ./cmd/vectra-bench
	cd mcp && go build -o ../bin/vectra-mcp ./cmd/vectra-mcp

test: ## 跑全部测试（三个 module）
	@for m in $(MODULES); do echo "== go test $$m =="; (cd $$m && go test ./...) || exit 1; done

vet: ## go vet（三个 module）
	@for m in $(MODULES); do echo "== go vet $$m =="; (cd $$m && go vet ./...) || exit 1; done

fmt: ## gofmt（三个 module）
	@for m in $(MODULES); do (cd $$m && go fmt ./...); done

clean: ## 清理产物：bin/、benchdata/、mcp/ts/dist、__pycache__、*.tmp/*.snap、server/vectra-server
	rm -rf bin benchdata mcp/ts/dist
	rm -f server/vectra-server *.tmp *.snap
	rm -rf mcp/python/vectra_mcp/__pycache__ mcp/python/__pycache__

deepclean: clean ## 深度清理：另删 node_modules 与 .venv（下次使用需重装依赖）
	rm -rf mcp/ts/node_modules mcp/python/.venv
