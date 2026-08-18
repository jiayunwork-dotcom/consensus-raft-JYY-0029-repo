package node

import "sort"

// PeerState tracks replication progress for a single follower.
type PeerState struct {
	ID         uint64
	NextIndex  uint64
	MatchIndex uint64
}

// PeerTracker manages replication state for all peers.
type PeerTracker struct {
	peers map[uint64]*PeerState
}

// NewPeerTracker creates a tracker for the given peer IDs.
func NewPeerTracker(ids []uint64, lastIndex uint64) *PeerTracker {
	pt := &PeerTracker{peers: make(map[uint64]*PeerState, len(ids))}
	for _, id := range ids {
		pt.peers[id] = &PeerState{
			ID:        id,
			NextIndex: lastIndex + 1,
		}
	}
	return pt
}

// Get returns the state for a peer.
func (pt *PeerTracker) Get(id uint64) *PeerState {
	return pt.peers[id]
}

// UpdateMatch updates the match index for a peer.
func (pt *PeerTracker) UpdateMatch(id, matchIndex uint64) {
	if p, ok := pt.peers[id]; ok {
		p.MatchIndex = matchIndex
		if matchIndex+1 > p.NextIndex {
			p.NextIndex = matchIndex + 1
		}
	}
}

// DecrementNext decreases next index for a peer (on rejection).
func (pt *PeerTracker) DecrementNext(id uint64) {
	if p, ok := pt.peers[id]; ok {
		if p.NextIndex > 1 {
			p.NextIndex--
		}
	}
}

// MatchIndexes returns all match indexes sorted.
func (pt *PeerTracker) MatchIndexes() []uint64 {
	out := make([]uint64, 0, len(pt.peers))
	for _, p := range pt.peers {
		out = append(out, p.MatchIndex)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// QuorumMatch returns the highest index replicated on a majority (including leader).
func (pt *PeerTracker) QuorumMatch(leaderMatch uint64) uint64 {
	matches := pt.MatchIndexes()
	matches = append(matches, leaderMatch)
	sort.Slice(matches, func(i, j int) bool { return matches[i] < matches[j] })
	// Majority is len/2 + 1 for the full cluster.
	n := len(matches)
	quorumIdx := n - (n/2 + 1)
	if quorumIdx < 0 {
		quorumIdx = 0
	}
	// We want the (n - quorum)th element from sorted desc.
	return matches[n-n/2-1]
}

// IDs returns all tracked peer IDs sorted.
func (pt *PeerTracker) IDs() []uint64 {
	out := make([]uint64, 0, len(pt.peers))
	for id := range pt.peers {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Len returns the number of tracked peers.
func (pt *PeerTracker) Len() int { return len(pt.peers) }
