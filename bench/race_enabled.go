package bench

import "github.com/projanvil/vectra/internal/racemode"

// raceEnabled guards timing-sensitive wait windows: the race detector
// multiplies the vector pack lane's work many-fold, so the convergence
// deadlines scale up under -race instead of flaking on slow hosts. The
// build-tag pair lives once in internal/racemode (audit F-07).
const raceEnabled = racemode.Enabled
