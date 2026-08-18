// Command consensus-raft is a CLI demonstration of the Raft consensus library.
// It runs a simulated cluster of N nodes, proposes values, and prints the
// committed log.
//
// Usage:
//
//	consensus-raft --nodes 3 --proposals 10
package main

import (
	"flag"
	"fmt"
	"os"

	"consensus-raft/internal/cluster"
)

func main() {
	nodes := flag.Int("nodes", 3, "number of nodes in the cluster")
	proposals := flag.Int("proposals", 5, "number of values to propose")
	seed := flag.Int64("seed", 1, "random seed for determinism")
	flag.Parse()

	if *nodes < 1 {
		fmt.Fprintln(os.Stderr, "error: need at least 1 node")
		os.Exit(1)
	}

	c := cluster.New(*nodes, *seed)

	// Elect a leader by ticking until one emerges.
	for i := 0; i < 200; i++ {
		c.TickAll()
		c.DeliverAll()
		if c.Leader() > 0 {
			break
		}
	}
	if c.Leader() == 0 {
		fmt.Fprintln(os.Stderr, "error: no leader elected after 200 ticks")
		os.Exit(1)
	}
	fmt.Printf("Leader elected: node %d\n", c.Leader())

	// Propose values.
	for i := 1; i <= *proposals; i++ {
		cmd := fmt.Sprintf("set key%d value%d", i, i)
		if err := c.Propose([]byte(cmd)); err != nil {
			fmt.Fprintf(os.Stderr, "propose failed: %v\n", err)
			continue
		}
		// Drive cluster until committed.
		for j := 0; j < 50; j++ {
			c.TickAll()
			c.DeliverAll()
		}
	}

	// Print committed log.
	fmt.Printf("\nCommitted log (node %d):\n", c.Leader())
	entries := c.CommittedEntries(c.Leader())
	for _, e := range entries {
		fmt.Printf("  [term=%d idx=%d] %s\n", e.Term, e.Index, e.Command)
	}
	fmt.Printf("\nTotal committed: %d entries\n", len(entries))
}
