// vectra-cli is a Cypher REPL / one-shot client for a vectra-server node's
// HTTP API. This file holds flag parsing, source selection (terminal editor
// vs plain line reader), and exit codes.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/term"
)

// historyMax caps the persisted statement history.
const historyMax = 500

// envOr returns the environment value for key when set (and non-empty),
// else def.
func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// defaultHistoryPath is $HOME/.vectra_history, or "" (persistence off) when
// the home directory cannot be determined.
func defaultHistoryPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".vectra_history")
}

func main() {
	os.Exit(run())
}

// run parses flags and drives either one-shot or REPL mode. Exit codes:
// 0 ok, 1 execution error (one-shot) or startup error, 2 usage.
func run() int {
	url := flag.String("url", envOr("VECTRA_URL", "http://127.0.0.1:8080"),
		"vectra-server base URL (env VECTRA_URL)")
	token := flag.String("token", envOr("VECTRA_TOKEN", ""),
		"bearer token, sent as Authorization (env VECTRA_TOKEN)")
	query := flag.String("c", "",
		"one-shot query: execute, render, and exit (no REPL)")
	timeout := flag.Duration("timeout", 30*time.Second,
		"per-request timeout")
	histPath := flag.String("history", defaultHistoryPath(),
		"history file (\"\" disables persistence)")
	flag.Parse()
	if flag.NArg() > 0 {
		flag.Usage()
		return 2
	}

	c, err := newCypherClient(*url, *token, *timeout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "vectra-cli:", err)
		return 1
	}

	if *query != "" {
		return runOneShot(*query, c, *timeout)
	}

	h := loadHistory(*histPath, historyMax)
	var src lineSource
	if term.IsTerminal(int(os.Stdin.Fd())) {
		ts, err := newTermSource(os.Stdin, "> ", "... ", h)
		if err != nil {
			fmt.Fprintln(os.Stderr, "vectra-cli:", err)
			return 1
		}
		defer ts.Close()
		src = ts
	} else {
		src = newPipeSource(os.Stdin)
	}

	code := runREPL(context.Background(), c, src, os.Stdout)
	if err := h.save(); err != nil {
		fmt.Fprintln(os.Stderr, "vectra-cli: warning: history not saved:", err)
	}
	return code
}

// runOneShot executes a single statement (with the same cleanup the REPL
// applies) and renders it: 0 on success, 1 on an execution error.
func runOneShot(query string, c *cypherClient, timeout time.Duration) int {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	stmt := strings.TrimRight(strings.TrimSpace(query), "; \t\r\n")
	if stmt == "" {
		return 0
	}
	if err := execute(ctx, c, stmt, os.Stdout); err != nil {
		return 1
	}
	return 0
}

// pipeSource is the non-tty lineSource: plain lines from stdin, so pipes
// (and redirected files) drive the same REPL statement rules. The 1 MiB
// line cap mirrors the server's request body limit.
type pipeSource struct {
	sc *bufio.Scanner
}

func newPipeSource(r io.Reader) *pipeSource {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	return &pipeSource{sc: sc}
}

func (p *pipeSource) Next() (string, bool, error) {
	if p.sc.Scan() {
		return p.sc.Text(), true, nil
	}
	return "", false, p.sc.Err() // nil at EOF
}
