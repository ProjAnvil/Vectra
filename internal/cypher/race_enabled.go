//go:build race

package cypher

// raceEnabled guards allocation-count assertions (see root's copy).
const raceEnabled = true
