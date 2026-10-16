//go:build race

package bench

// raceBuild shrinks the acceptance benchmarks' heavy tiers under the race
// detector: -race multiplies merge/inject costs ~5-10x, which would push the
// full-scale tiers past the default test timeout without adding detection
// value (the race detector needs concurrency, not scale). Full-scale numbers
// come from the non-race runs recorded in docs/benchmarks.
const raceBuild = true
