//go:build race

package traverse

// raceEnabled guards allocation-count assertions (see root's copy).
const raceEnabled = true
