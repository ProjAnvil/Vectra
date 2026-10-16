//go:build race

package vectra

// raceEnabled guards allocation-count assertions: the race detector adds
// its own bookkeeping allocations, so exact alloc bounds are meaningless
// under -race.
const raceEnabled = true
