//go:build race

package bench

// raceEnabled guards timing-sensitive wait windows (see root's copy): the
// race detector multiplies the vector pack lane's work many-fold, so the
// convergence deadlines scale up under -race instead of flaking on slow
// hosts.
const raceEnabled = true
