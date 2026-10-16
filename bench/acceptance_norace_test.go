//go:build !race

package bench

// raceBuild is false in normal builds; see acceptance_race_test.go.
const raceBuild = false
