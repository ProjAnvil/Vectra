package vectra_test

import (
	"fmt"
	"os"
	"slices"

	"github.com/projanvil/vectra"
)

// Example_quickstart demonstrates the full lifecycle: build → Seal → query → snapshot.
func Example_quickstart() {
	g := vectra.New()
	for _, n := range []string{"gateway", "auth", "ledger", "db", "kafka"} {
		_, _ = g.AddNode(n, "Service", nil)
	}
	for _, e := range [][3]string{
		{"gateway", "auth", "CALLS"}, {"gateway", "ledger", "CALLS"},
		{"auth", "db", "QUERY"}, {"ledger", "db", "QUERY"}, {"db", "kafka", "EVENT"},
	} {
		_, _ = g.AddEdge(e[0], e[1], e[2], nil)
	}
	if err := g.Seal(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return
	}

	reach, _ := g.Reachable("gateway")
	fmt.Println("reachable:", reach)

	path, found, _ := g.ShortestPath("gateway", "kafka")
	fmt.Println("path found:", found, "hops:", path.Len())

	hops, _ := g.Query().From("gateway").Out("CALLS").MaxDepth(1).Run()
	fmt.Println("1-hop CALLS targets:", hops.Count()-1)

	cycles := g.Cycles()
	fmt.Println("cycles:", len(cycles))

	_ = g.SaveFile(os.TempDir() + "/vectra-demo.snap")
	// Output:
	// reachable: [gateway auth ledger db kafka]
	// path found: true hops: 3
	// 1-hop CALLS targets: 2
	// cycles: 0
}

// ExampleGraph_Cycles demonstrates circular-dependency discovery.
func ExampleGraph_Cycles() {
	g := vectra.New()
	for _, k := range []string{"order", "pay", "risk", "notify"} {
		_, _ = g.AddNode(k, "Service", nil)
	}
	for _, e := range [][2]string{{"order", "pay"}, {"pay", "risk"}, {"risk", "pay"}, {"order", "notify"}} {
		_, _ = g.AddEdge(e[0], e[1], "CALLS", nil)
	}
	_ = g.Seal()
	for _, c := range g.Cycles() {
		names := make([]string, len(c.Nodes))
		for i, id := range c.Nodes {
			n, _ := g.NodeByID(id)
			names[i] = n.Key
		}
		slices.Sort(names)
		fmt.Println("cycle:", names)
	}
	// Output:
	// cycle: [pay risk]
}
