package cypher

import "github.com/projanvil/vectra/internal/racemode"

// raceEnabled guards allocation-count assertions (see the root package's
// copy for the rationale); the build-tag pair lives once in
// internal/racemode (audit F-07).
const raceEnabled = racemode.Enabled
