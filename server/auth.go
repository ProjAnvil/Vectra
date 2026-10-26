package server

import (
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
)

// Static bearer-token auth. A node configured with AuthTokenFile accepts only
// requests presenting one of the file's tokens. Tokens are loaded once at New
// and fail-fast: a malformed or unreadable token file aborts startup rather
// than degrading into a silently open database.

// loadAuthTokens reads a bearer-token file: one token per line. Blank lines
// (including the trailing newline) and CRLF endings are tolerated; surrounding
// ASCII space/tab/\r is trimmed. A line with internal whitespace is rejected —
// a token split by a copy-paste artifact must not become two silently
// different credentials. At least one token is required.
func loadAuthTokens(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("auth: read token file %s: %w", path, err)
	}
	var tokens []string
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.Trim(line, " \t\r")
		if line == "" {
			continue
		}
		if strings.ContainsAny(line, " \t\r") {
			return nil, fmt.Errorf("auth: token file %s line %d contains whitespace", path, i+1)
		}
		tokens = append(tokens, line)
	}
	if len(tokens) == 0 {
		return nil, fmt.Errorf("auth: token file %s contains no tokens", path)
	}
	return tokens, nil
}

// authDigestsFrom hashes each token with SHA-256. Incoming credentials are
// hashed and compared against these digests, so request handling never touches
// the raw token list.
func authDigestsFrom(tokens []string) [][32]byte {
	digests := make([][32]byte, len(tokens))
	for i, tok := range tokens {
		digests[i] = sha256.Sum256([]byte(tok))
	}
	return digests
}
