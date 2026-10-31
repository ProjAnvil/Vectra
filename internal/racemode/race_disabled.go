//go:build !race

package racemode

// Enabled reports whether the race detector build tag is set (see
// race_enabled.go for the package doc).
const Enabled = false
