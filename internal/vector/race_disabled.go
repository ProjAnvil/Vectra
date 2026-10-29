//go:build !race

package vector

// raceEnabled guards runtime-budget assertions; see race_enabled.go.
const raceEnabled = false
