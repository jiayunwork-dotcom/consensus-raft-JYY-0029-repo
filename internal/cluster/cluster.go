// Package cluster provides a simulated multi-node Raft cluster for testing
// and demonstration. It handles message routing between nodes without any
// network I/O.
package cluster

import (
	"consensus-raft/internal/log"
	"consensus-raft/internal/node"
)

// Cluster is a simulated Raft cluster.
type Cluster struct {
	nodes  []*node.Node
	queues map[uint64][]node.Message
	size   int
}

// New creates a cluster with n nodes using the given random seed.
func New(n int, seed int64) *Cluster {
	c := &Cluster{
		nodes:  make([]*node.Node, n),
		queues: make(map[uint64][]node.Message),
		size:   n,
	}
	peers := make([]uint64, n)
	for i := range peers {
		peers[i] = uint64(i + 1)
	}
	for i := 0; i < n; i++ {
		id := uint64(i + 1)
		rng := node.NewSeededRNG(seed + int64(i))
		c.nodes[i] = node.New(id, peers, rng)
		c.queues[id] = nil
	}
	return c
}

// TickAll advances every node by one tick.
func (c *Cluster) TickAll() {
	for _, n := range c.nodes {
		msgs := n.Tick()
		c.route(msgs)
	}
}

// DeliverAll delivers all pending messages to their destinations.
func (c *Cluster) DeliverAll() {
	for id, msgs := range c.queues {
		if len(msgs) == 0 {
			continue
		}
		n := c.nodeByID(id)
		if n == nil {
			c.queues[id] = nil
			continue
		}
		for _, m := range msgs {
			replies := n.Step(m)
			c.route(replies)
		}
		c.queues[id] = nil
	}
}

// Leader returns the ID of the current leader, or 0 if none.
func (c *Cluster) Leader() uint64 {
	for _, n := range c.nodes {
		if n.State() == node.StateLeader {
			return n.ID()
		}
	}
	return 0
}

// Propose submits a command to the current leader.
func (c *Cluster) Propose(cmd []byte) error {
	leader := c.Leader()
	if leader == 0 {
		return node.ErrNotLeader
	}
	n := c.nodeByID(leader)
	if err := n.Propose(cmd); err != nil {
		return err
	}
	// Immediately replicate.
	msgs := n.ReplicateNow()
	c.route(msgs)
	return nil
}

// CommittedEntries returns all committed log entries for a node.
func (c *Cluster) CommittedEntries(id uint64) []log.Entry {
	n := c.nodeByID(id)
	if n == nil {
		return nil
	}
	return n.CommittedEntries()
}

// Size returns the cluster size.
func (c *Cluster) Size() int { return c.size }

// Node returns the node with the given ID.
func (c *Cluster) Node(id uint64) *node.Node {
	return c.nodeByID(id)
}

// AllNodes returns all nodes.
func (c *Cluster) AllNodes() []*node.Node {
	out := make([]*node.Node, len(c.nodes))
	copy(out, c.nodes)
	return out
}

// DrainAllEvents returns events from all nodes.
func (c *Cluster) DrainAllEvents() map[uint64][]string {
	events := make(map[uint64][]string)
	for _, n := range c.nodes {
		e := n.DrainEvents()
		if len(e) > 0 {
			events[n.ID()] = e
		}
	}
	return events
}

func (c *Cluster) route(msgs []node.Message) {
	for _, m := range msgs {
		c.queues[m.To] = append(c.queues[m.To], m)
	}
}

func (c *Cluster) nodeByID(id uint64) *node.Node {
	for _, n := range c.nodes {
		if n.ID() == id {
			return n
		}
	}
	return nil
}
