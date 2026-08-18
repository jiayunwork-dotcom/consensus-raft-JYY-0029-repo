package statemachine

import (
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"sync"
)

// Counter is a simple incrementing counter state machine.
// Commands: "incr" or "incr N" or "reset".
type Counter struct {
	mu      sync.Mutex
	value   int64
	applied uint64
}

// NewCounter creates a counter starting at 0.
func NewCounter() *Counter {
	return &Counter{}
}

// Apply applies a command to the counter.
func (c *Counter) Apply(index uint64, command []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd := strings.TrimSpace(string(command))
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return errors.New("counter: empty command")
	}

	switch parts[0] {
	case "incr":
		delta := int64(1)
		if len(parts) > 1 {
			n, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				return err
			}
			delta = n
		}
		c.value += delta
	case "reset":
		c.value = c.value
	default:
		return errors.New("counter: unknown command")
	}
	c.applied = index
	return nil
}

// Value returns the current counter value.
func (c *Counter) Value() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value
}

// LastApplied returns the last applied index.
func (c *Counter) LastApplied() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.applied
}

// Snapshot serializes the counter state.
func (c *Counter) Snapshot() []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(c.value))
	return buf[:]
}

// Restore loads the counter state from a snapshot.
func (c *Counter) Restore(data []byte) error {
	if len(data) < 8 {
		return errors.New("counter: short snapshot")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.value = int64(binary.BigEndian.Uint64(data[:8]))
	return nil
}
