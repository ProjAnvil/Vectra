// vectra-cli's REPL loop: statement accumulation and command handling over
// any lineSource (terminal editor, pipe, or test script). No terminal
// knowledge lives here.
package main

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// lineSource yields input lines. Next returns the next line (without its
// newline); ok=false on EOF, err non-nil on a read failure.
type lineSource interface {
	Next() (string, bool, error)
}

// promptSwitcher is implemented by lineSources that render their own prompts
// (termSource). runREPL reports whether the next line continues the current
// statement so the source can switch to its continuation prompt.
type promptSwitcher interface {
	setContinuation(cont bool)
}

// exitCommands are recognized (case-insensitively, only on an empty buffer)
// as leave-the-REPL commands.
var exitCommands = map[string]bool{
	":q": true, ":quit": true, ":exit": true, "quit": true, "exit": true,
}

func isExitCommand(line string) bool { return exitCommands[strings.ToLower(line)] }

// printUsage writes the REPL's :help text.
func printUsage(w io.Writer) {
	fmt.Fprintln(w, "Statements end with ';' and may span multiple lines.")
	fmt.Fprintln(w, ":help                         show this help")
	fmt.Fprintln(w, ":q, :quit, :exit, quit, exit  leave the REPL")
}

// runREPL drives the read-eval-print loop. Lines accumulate (trimmed,
// space-joined) until the buffer ends with ';'; the submit strips ALL
// trailing ';' and whitespace and sends the statement via c. Commands
// (see exitCommands, plus :help) are recognized only on an empty buffer.
// Reads render a table plus a "(N rows)" line; writes render a table only
// when the statement carried a RETURN clause, plus the summary line. Errors
// print as "error: <code>: <message>" and the loop continues. Returns 0 on
// EOF or exit command, 1 only on a source error.
func runREPL(ctx context.Context, c *cypherClient, src lineSource, out io.Writer) int {
	var buf string
	for {
		if ps, ok := src.(promptSwitcher); ok {
			ps.setContinuation(buf != "")
		}
		line, ok, err := src.Next()
		if err != nil {
			fmt.Fprintf(out, "error: %v\n", err)
			return 1
		}
		if !ok {
			return 0
		}
		t := strings.TrimSpace(line)
		if buf == "" {
			if isExitCommand(t) {
				return 0
			}
			if strings.EqualFold(t, ":help") {
				printUsage(out)
				continue
			}
		}
		if t == "" {
			continue // empty line or bare ";": empty submit → no-op
		}
		if buf == "" {
			buf = t
		} else {
			buf += " " + t
		}
		if !strings.HasSuffix(buf, ";") {
			continue
		}
		stmt := strings.TrimRight(buf, "; \t\r\n")
		buf = ""
		if stmt == "" {
			continue // the buffer held only ';' and whitespace
		}
		execute(ctx, c, stmt, out) // errors are printed, never fatal
	}
}

// execute sends one statement and renders the response (shared by the REPL
// and one-shot mode). The returned error has already been printed to out as
// an "error: ..." line.
func execute(ctx context.Context, c *cypherClient, stmt string, out io.Writer) error {
	resp, err := c.cypher(ctx, stmt)
	if err != nil {
		// *apiError formats itself as "<code>: <message>".
		fmt.Fprintf(out, "error: %v\n", err)
		return err
	}
	if resp.Summary != nil { // write
		if len(resp.Columns) > 0 {
			if err := renderTable(out, resp.Columns, resp.Rows); err != nil {
				return err
			}
		}
		renderSummary(out, resp.Summary)
		return nil
	}
	// read
	if err := renderTable(out, resp.Columns, resp.Rows); err != nil {
		return err
	}
	fmt.Fprintf(out, "(%d rows)\n", len(resp.Rows))
	return nil
}
