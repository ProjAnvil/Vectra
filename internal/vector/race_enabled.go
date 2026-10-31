package vector

import "github.com/projanvil/vectra/internal/racemode"

// raceEnabled guards runtime-budget assertions: the race detector
// instruments every 768-dim Distance loop, and the big construction
// tests run ~30x slower under it (32s -> 15m+), blowing the package's
// default 600s test budget. Those tests are single-goroutine, so -race
// adds no coverage for them; the Pack/merge paths stay race-covered by
// the tier-cascade and death-compaction tests. The build-tag pair lives
// once in internal/racemode (audit F-07).
const raceEnabled = racemode.Enabled
