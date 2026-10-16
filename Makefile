# Vectra development workflow
#
# Quick stress benchmark (data dir and report are deleted when the run
# ends — zero residue in the repo):
#
#	make bench                          # small (1K nodes / 10K edges), engine mode
#	make bench SIZE=medium              # 100K nodes / 1M edges
#	make bench SIZE=large MODE=http     # server mode (raft + HTTP), 1M / 10M
#	make bench SIZE=small WORKLOADS=neighbors,bfs2 DURATION=1s
#
# To keep data for debugging: make bench BENCH_EXTRA='-keep -datadir benchdata/debug'

.DEFAULT_GOAL := help

# Tunable bench parameters (override on the command line: make bench SIZE=...):
#   SIZE        graph size small|medium|large = 1K/10K, 100K/1M, 1M/10M (edges)
#   MODE        engine (in-process) | http (raft + HTTP server mode)
#   WORKERS     number of concurrent workers
#   DURATION    per-workload duration
#   WORKLOADS   comma-separated workload subset: neighbors,bfs2,bfs3,bfsall,path,reach (empty = all)
#   BENCH_EXTRA remaining flags passed through to vectra-bench, e.g. -seed 7 -warmup 1s
SIZE ?= small
MODE ?= engine
WORKERS ?= 8
DURATION ?= 5s
WORKLOADS ?=
BENCH_EXTRA ?=

# The repo contains three Go modules: root (engine), server/ (server + bench), mcp/ (agent gateway)
MODULES := . server mcp

# MCP implementation version: git-derived when available, source default
# (mcp/server.go) otherwise — `go install`/plain `go build` builds carry no
# flags and fall back to the source value, which release commits keep current.
MCP_VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null)

.PHONY: help bench build test vet fmt clean deepclean

help: ## show this help
	@awk -F ':.*## ' '/^[a-zA-Z][a-zA-Z_-]*:.*## / {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

bench: ## run the stress benchmark, e.g. make bench [SIZE=medium] [MODE=http] [WORKERS=16] [DURATION=10s] [WORKLOADS=...] [BENCH_EXTRA='-seed 7']
	@case '$(SIZE)' in small|medium|large) ;; *) \
		printf 'error: invalid SIZE=%s (expected small|medium|large)\n' '$(SIZE)' >&2; exit 2;; esac
	@case '$(MODE)' in engine|http) ;; *) \
		printf 'error: invalid MODE=%s (expected engine|http)\n' '$(MODE)' >&2; exit 2;; esac
	@root=$$(pwd); mkdir -p benchdata; \
	dir=$$(mktemp -d "$$root/benchdata/run.XXXXXX"); \
	trap 'rm -rf "$$dir"; rmdir "$$root/benchdata" 2>/dev/null || true' EXIT INT TERM; \
	(cd server && go run ./cmd/vectra-bench \
	    -size $(SIZE) -mode $(MODE) -workers $(WORKERS) -duration $(DURATION) \
	    $(if $(WORKLOADS),-workloads $(WORKLOADS)) \
	    -datadir "$$dir/data" -out "$$dir/report.md" $(BENCH_EXTRA)) \
	  && { cat "$$dir/report.md" 2>/dev/null || true; }

build: ## build all binaries into bin/ (vectra-server, vectra-bench, vectra-mcp)
	cd server && go build -o ../bin/vectra-server ./cmd/vectra-server
	cd server && go build -o ../bin/vectra-bench  ./cmd/vectra-bench
	cd mcp && if [ -n '$(MCP_VERSION)' ]; then \
	    go build -ldflags "-X github.com/projanvil/vectra/mcp.Version=$(MCP_VERSION)" -o ../bin/vectra-mcp ./cmd/vectra-mcp; \
	else \
	    go build -o ../bin/vectra-mcp ./cmd/vectra-mcp; \
	fi

test: ## run all tests (three modules)
	@for m in $(MODULES); do echo "== go test $$m =="; (cd $$m && go test ./...) || exit 1; done

vet: ## go vet (three modules)
	@for m in $(MODULES); do echo "== go vet $$m =="; (cd $$m && go vet ./...) || exit 1; done

fmt: ## go fmt (three modules)
	@for m in $(MODULES); do (cd $$m && go fmt ./...); done

clean: ## remove artifacts: bin/, benchdata/, mcp/ts/dist, __pycache__, *.tmp/*.snap, server/vectra-server
	rm -rf bin benchdata mcp/ts/dist
	rm -f server/vectra-server *.tmp *.snap
	rm -rf mcp/python/vectra_mcp/__pycache__ mcp/python/__pycache__

deepclean: clean ## deep clean: also remove node_modules and .venv (deps must be reinstalled)
	rm -rf mcp/ts/node_modules mcp/python/.venv
