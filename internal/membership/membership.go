// Package membership manages cluster membership and configuration changes
// for the Raft consensus protocol. It tracks the current set of peers and
// provides helpers for quorum calculation.
package membership

import "sort"

// Config represents the cluster membership configuration.
type Config struct {
	Peers []uint64
}

// NewConfig creates a configuration from a list of peer IDs.
func NewConfig(peers []uint64) *Config {
	sorted := make([]uint64, len(peers))
	copy(sorted, peers)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return &Config{Peers: sorted}
}

// Contains reports whether id is a member.
func (c *Config) Contains(id uint64) bool {
	for _, p := range c.Peers {
		if p == id {
			return true
		}
	}
	return false
}

// Size returns the number of peers.
func (c *Config) Size() int { return len(c.Peers) }

// Quorum returns the minimum number of nodes needed for a majority.
func (c *Config) Quorum() int {
	return len(c.Peers)/2 + 1
}

// OtherPeers returns all peers except the given id.
func (c *Config) OtherPeers(id uint64) []uint64 {
	var out []uint64
	for _, p := range c.Peers {
		if p != id {
			out = append(out, p)
		}
	}
	return out
}

// AddPeer returns a new config with the peer added.
func (c *Config) AddPeer(id uint64) *Config {
	if c.Contains(id) {
		return c
	}
	peers := make([]uint64, len(c.Peers)+1)
	copy(peers, c.Peers)
	peers[len(c.Peers)] = id
	return NewConfig(peers)
}

// RemovePeer returns a new config with the peer removed.
func (c *Config) RemovePeer(id uint64) *Config {
	var peers []uint64
	for _, p := range c.Peers {
		if p != id {
			peers = append(peers, p)
		}
	}
	return NewConfig(peers)
}

// Equal reports whether two configs have the same peers.
func (c *Config) Equal(other *Config) bool {
	if len(c.Peers) != len(other.Peers) {
		return false
	}
	for i := range c.Peers {
		if c.Peers[i] != other.Peers[i] {
			return false
		}
	}
	return true
}

// IsMajority reports whether count constitutes a majority.
func (c *Config) IsMajority(count int) bool {
	return count >= c.Quorum()
}
