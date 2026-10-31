package vectra

import "github.com/projanvil/vectra/internal/racemode"

// raceEnabled guards allocation-count assertions: the race detector adds
// its own bookkeeping allocations, so exact alloc bounds are meaningless
// under -race. The build-tag pair lives once in internal/racemode (audit
// F-07); this alias keeps the local spelling the test gates use.
const raceEnabled = racemode.Enabled
