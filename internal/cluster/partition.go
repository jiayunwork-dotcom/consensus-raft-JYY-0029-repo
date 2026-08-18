package cluster

import "consensus-raft/internal/node"

// PartitionedCluster extends Cluster with network partition simulation.
type PartitionedCluster struct {
	*Cluster
	partitions map[uint64]map[uint64]bool // from -> to -> blocked
}

// NewPartitioned creates a partitioned cluster.
func NewPartitioned(n int, seed int64) *PartitionedCluster {
	return &PartitionedCluster{
		Cluster:    New(n, seed),
		partitions: make(map[uint64]map[uint64]bool),
	}
}

// Partition isolates a node: all messages to/from it are dropped.
func (pc *PartitionedCluster) Partition(id uint64) {
	if pc.partitions[id] == nil {
		pc.partitions[id] = make(map[uint64]bool)
	}
	for _, n := range pc.nodes {
		if n.ID() != id {
			pc.partitions[id][n.ID()] = true
			if pc.partitions[n.ID()] == nil {
				pc.partitions[n.ID()] = make(map[uint64]bool)
			}
			pc.partitions[n.ID()][id] = true
		}
	}
}

// Heal removes all partitions for a node.
func (pc *PartitionedCluster) Heal(id uint64) {
	delete(pc.partitions, id)
	for _, m := range pc.partitions {
		delete(m, id)
	}
}

// HealAll removes all partitions.
func (pc *PartitionedCluster) HealAll() {
	pc.partitions = make(map[uint64]map[uint64]bool)
}

// IsBlocked reports whether messages from src to dst are blocked.
func (pc *PartitionedCluster) IsBlocked(src, dst uint64) bool {
	if m, ok := pc.partitions[src]; ok {
		return m[dst]
	}
	return false
}

// DeliverAllFiltered delivers messages respecting partitions.
func (pc *PartitionedCluster) DeliverAllFiltered() {
	for id, msgs := range pc.queues {
		if len(msgs) == 0 {
			continue
		}
		n := pc.nodeByID(id)
		if n == nil {
			pc.queues[id] = nil
			continue
		}
		var kept []node.Message
		for _, m := range msgs {
			if pc.IsBlocked(m.From, m.To) {
				continue // drop
			}
			replies := n.Step(m)
			// Filter replies through partition.
			for _, r := range replies {
				if !pc.IsBlocked(r.From, r.To) {
					pc.queues[r.To] = append(pc.queues[r.To], r)
				}
			}
		}
		pc.queues[id] = kept
	}
}
