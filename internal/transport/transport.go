// Package transport defines the message transport interface and provides an
// in-memory implementation for testing. In a real system, this would be replaced
// by TCP/gRPC, but the in-memory transport lets us test the full Raft protocol
// without network I/O.
package transport

import (
	"errors"
	"sync"

	"consensus-raft/internal/node"
)

// ErrClosed is returned when sending on a closed transport.
var ErrClosed = errors.New("transport: closed")

// ErrUnknownPeer is returned when the destination is not known.
var ErrUnknownPeer = errors.New("transport: unknown peer")

// Transport is an interface for sending messages between Raft nodes.
type Transport interface {
	Send(msg node.Message) error
	Receive(id uint64) ([]node.Message, error)
	Close() error
}

// InMemory is a channel-based in-memory transport.
type InMemory struct {
	mu      sync.Mutex
	mailbox map[uint64][]node.Message
	closed  bool
}

// NewInMemory creates an in-memory transport for the given peer IDs.
func NewInMemory(peers []uint64) *InMemory {
	t := &InMemory{
		mailbox: make(map[uint64][]node.Message),
	}
	for _, id := range peers {
		t.mailbox[id] = nil
	}
	return t
}

// Send delivers a message to the recipient's mailbox.
func (t *InMemory) Send(msg node.Message) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrClosed
	}
	if _, ok := t.mailbox[msg.To]; !ok {
		return ErrUnknownPeer
	}
	t.mailbox[msg.To] = append(t.mailbox[msg.To], msg)
	return nil
}

// Receive drains and returns all pending messages for a node.
func (t *InMemory) Receive(id uint64) ([]node.Message, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, ErrClosed
	}
	msgs := t.mailbox[id]
	t.mailbox[id] = nil
	return msgs, nil
}

// Close shuts down the transport.
func (t *InMemory) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return nil
}

// PendingCount returns the number of undelivered messages for a peer.
func (t *InMemory) PendingCount(id uint64) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.mailbox[id])
}

// TotalPending returns total undelivered messages across all peers.
func (t *InMemory) TotalPending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	total := 0
	for _, msgs := range t.mailbox {
		total += len(msgs)
	}
	return total
}

// Reset clears all pending messages without closing.
func (t *InMemory) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id := range t.mailbox {
		t.mailbox[id] = nil
	}
}

// Drop discards all pending messages for a peer (simulates partition).
func (t *InMemory) Drop(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mailbox[id] = nil
}
