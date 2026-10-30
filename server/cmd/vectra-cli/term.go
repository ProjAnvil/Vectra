// Terminal line editor for the REPL: raw mode via x/term, rune-wise editing,
// history navigation, and prompt rendering. Everything tty-specific lives in
// this file so the REPL loop (repl.go) stays testable without a terminal.
//
// Handled keys: printable runes (UTF-8), Backspace/Delete, Left/Right and
// Home/End (ESC [ C/D/H/F and ESC [ 1~/4~), Up/Down history navigation
// (ESC [ A/B), Ctrl+U (clear line), Ctrl+C (discard line), Ctrl+D (EOF on an
// empty line), Enter (\r or \n).
package main

import (
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"golang.org/x/term"
)

// ============================================================
// History
// ============================================================

// history is a capped, optionally file-backed statement history.
type history struct {
	entries []string
	path    string
	max     int
}

// loadHistory reads path into a new history. A missing (or unreadable) file
// yields an empty history — persistence is best-effort, never fatal.
func loadHistory(path string, max int) *history {
	h := &history{path: path, max: max}
	if path == "" {
		return h
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return h
	}
	for _, line := range strings.Split(string(b), "\n") {
		if line = strings.TrimRight(line, "\r"); line != "" {
			h.entries = append(h.entries, line)
		}
	}
	if overflow := len(h.entries) - max; overflow > 0 {
		h.entries = h.entries[overflow:]
	}
	return h
}

// add appends line, skipping empty lines and duplicates of the last entry,
// and keeps the list capped at max (oldest dropped first).
func (h *history) add(line string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return
	}
	if n := len(h.entries); n > 0 && h.entries[n-1] == line {
		return
	}
	h.entries = append(h.entries, line)
	if len(h.entries) > h.max {
		h.entries = h.entries[len(h.entries)-h.max:]
	}
}

// save writes the history back to its file (one statement per line). It is a
// no-op when path is empty.
func (h *history) save() error {
	if h.path == "" {
		return nil
	}
	var b strings.Builder
	for _, e := range h.entries {
		b.WriteString(e)
		b.WriteByte('\n')
	}
	return os.WriteFile(h.path, []byte(b.String()), 0o600)
}

// ============================================================
// Terminal source
// ============================================================

// termSource is a lineSource over a terminal file in raw mode: it renders
// the prompt, echoes and edits the line, and restores cooked mode on EOF or
// Close.
type termSource struct {
	f          *os.File
	oldState   *term.State
	inRaw      bool
	prompt     string
	contPrompt string
	cont       bool
	h          *history

	// Per-line editing state (reset at the top of Next).
	buf   []rune // line being edited
	pos   int    // cursor position as a rune index into buf
	nav   int    // history index while navigating, -1 otherwise
	draft string // pre-navigation buffer, restored when navigating past the newest entry
}

// newTermSource returns a terminal line source. When f is a terminal it is
// switched to raw mode; the IsTerminal gate keeps non-tty files usable
// (calls normally route those to a pipe source instead).
func newTermSource(f *os.File, prompt, contPrompt string, h *history) (*termSource, error) {
	t := &termSource{f: f, prompt: prompt, contPrompt: contPrompt, h: h, nav: -1}
	if !term.IsTerminal(int(f.Fd())) {
		return t, nil
	}
	st, err := term.MakeRaw(int(f.Fd()))
	if err != nil {
		return nil, fmt.Errorf("raw mode: %w", err)
	}
	t.oldState = st
	t.inRaw = true
	return t, nil
}

// setContinuation switches the prompt for the next line (see promptSwitcher
// in repl.go).
func (t *termSource) setContinuation(cont bool) { t.cont = cont }

func (t *termSource) curPrompt() string {
	if t.cont {
		return t.contPrompt
	}
	return t.prompt
}

// Next reads one edited line. On Ctrl+D over an empty line (or a read EOF)
// it restores cooked mode and reports EOF.
func (t *termSource) Next() (string, bool, error) {
	t.buf = t.buf[:0]
	t.pos = 0
	t.nav = -1
	t.draft = ""
	t.writeStr(t.curPrompt())
	for {
		r, err := t.readRune()
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				t.restore()
				return "", false, nil
			}
			return "", false, err
		}
		switch r {
		case '\r', '\n': // submit
			line := string(t.buf)
			t.writeStr("\r\n")
			t.h.add(line)
			return line, true, nil
		case 0x7f, 0x08: // Backspace (DEL; some terminals send BS)
			if t.pos > 0 {
				t.buf = append(t.buf[:t.pos-1], t.buf[t.pos:]...)
				t.pos--
				t.redraw()
			}
		case 0x15: // Ctrl+U: clear line
			t.buf = t.buf[:0]
			t.pos = 0
			t.redraw()
		case 0x03: // Ctrl+C: discard line, start over
			t.buf = t.buf[:0]
			t.pos = 0
			t.nav = -1
			t.writeStr("^C\r\n")
			t.writeStr(t.curPrompt())
		case 0x04: // Ctrl+D: EOF on empty line, no-op otherwise
			if len(t.buf) == 0 {
				t.restore()
				t.writeStr("\r\n")
				return "", false, nil
			}
		case 0x1b: // ESC: arrow / Home / End / Delete sequences
			t.handleEscape()
		default:
			if r >= 0x20 { // printable rune (0x7f handled above)
				t.insertRune(r)
			}
		}
	}
}

// Close restores cooked mode if the source put the file into raw mode.
func (t *termSource) Close() error {
	t.restore()
	return nil
}

// restore puts the terminal back into cooked mode (idempotent).
func (t *termSource) restore() {
	if t.inRaw {
		term.Restore(int(t.f.Fd()), t.oldState)
		t.inRaw = false
	}
}

// ============================================================
// Input decoding
// ============================================================

// readRune reads one key, assembling multi-byte UTF-8 sequences. Malformed
// sequences yield rune 0, which Next ignores.
func (t *termSource) readRune() (rune, error) {
	var b [1]byte
	if _, err := io.ReadFull(t.f, b[:]); err != nil {
		return 0, err
	}
	c := b[0]
	if c < utf8.RuneSelf {
		return rune(c), nil
	}
	n := 0 // expected sequence length from the lead byte
	switch {
	case c&0xE0 == 0xC0:
		n = 2
	case c&0xF0 == 0xE0:
		n = 3
	case c&0xF8 == 0xF0:
		n = 4
	default:
		return 0, nil
	}
	seq := make([]byte, 0, n)
	seq = append(seq, c)
	for len(seq) < n {
		if _, err := io.ReadFull(t.f, b[:]); err != nil {
			return 0, err
		}
		seq = append(seq, b[0])
	}
	r, size := utf8.DecodeRune(seq)
	if r == utf8.RuneError && size <= 1 {
		return 0, nil
	}
	return r, nil
}

// handleEscape consumes one CSI sequence (ESC [ final) and applies it.
// Unsupported sequences are consumed and ignored.
func (t *termSource) handleEscape() {
	var b [1]byte
	if _, err := io.ReadFull(t.f, b[:]); err != nil || b[0] != '[' {
		return // SS3 (ESC O ...) and bare ESC: unsupported
	}
	if _, err := io.ReadFull(t.f, b[:]); err != nil {
		return
	}
	switch b[0] {
	case 'A': // up
		t.historyPrev()
	case 'B': // down
		t.historyNext()
	case 'C': // right
		if t.pos < len(t.buf) {
			t.pos++
			t.reposition()
		}
	case 'D': // left
		if t.pos > 0 {
			t.pos--
			t.reposition()
		}
	case 'H': // home
		t.pos = 0
		t.reposition()
	case 'F': // end
		t.pos = len(t.buf)
		t.reposition()
	case '1': // ESC [ 1 ~ : home
		t.skipTilde()
		t.pos = 0
		t.reposition()
	case '4': // ESC [ 4 ~ : end
		t.skipTilde()
		t.pos = len(t.buf)
		t.reposition()
	case '3': // ESC [ 3 ~ : delete forward
		t.skipTilde()
		if t.pos < len(t.buf) {
			t.buf = append(t.buf[:t.pos], t.buf[t.pos+1:]...)
			t.redraw()
		}
	}
}

// skipTilde consumes the '~' that terminates ESC [ n ~ sequences.
func (t *termSource) skipTilde() {
	var b [1]byte
	_, _ = io.ReadFull(t.f, b[:])
}

// ============================================================
// Editing and rendering
// ============================================================

// insertRune inserts r at the cursor.
func (t *termSource) insertRune(r rune) {
	t.buf = append(t.buf, 0)
	copy(t.buf[t.pos+1:], t.buf[t.pos:])
	t.buf[t.pos] = r
	t.pos++
	t.redraw()
}

// historyPrev replaces the line with the previous history entry.
func (t *termSource) historyPrev() {
	if len(t.h.entries) == 0 {
		return
	}
	if t.nav == -1 {
		t.draft = string(t.buf)
		t.nav = len(t.h.entries) - 1
	} else if t.nav > 0 {
		t.nav--
	}
	t.buf = []rune(t.h.entries[t.nav])
	t.pos = len(t.buf)
	t.redraw()
}

// historyNext moves back toward the newest entry, restoring the pre-nav
// draft once past it.
func (t *termSource) historyNext() {
	if t.nav == -1 {
		return
	}
	if t.nav < len(t.h.entries)-1 {
		t.nav++
		t.buf = []rune(t.h.entries[t.nav])
	} else {
		t.nav = -1
		t.buf = []rune(t.draft)
	}
	t.pos = len(t.buf)
	t.redraw()
}

// redraw repaints the whole line: clear to end of line, prompt, buffer, then
// pulls the cursor back left of the tail beyond it. Documented limitation:
// repositioning counts runes, so double-width (CJK) runes skew it.
func (t *termSource) redraw() {
	t.writeStr("\r\x1b[K")
	t.writeStr(t.curPrompt())
	t.writeStr(string(t.buf))
	t.reposition()
}

// reposition moves the cursor left over the unconfirmed tail of the buffer.
func (t *termSource) reposition() {
	if n := len(t.buf) - t.pos; n > 0 {
		t.writeStr(fmt.Sprintf("\x1b[%dD", n))
	}
}

func (t *termSource) writeStr(s string) {
	_, _ = io.WriteString(t.f, s)
}
