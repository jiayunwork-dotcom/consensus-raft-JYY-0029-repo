// Package statemachine implements the application state machine driven by the
// committed Raft log. The KV store is the canonical example: each committed
// entry is a set/delete command applied to an in-memory map.
package statemachine

import (
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
)

// ErrBadCommand is returned when a command cannot be parsed.
var ErrBadCommand = errors.New("statemachine: bad command")

// KV is a simple key-value state machine.
type KV struct {
	mu      sync.RWMutex
	data    map[string]string
	applied uint64
}

// NewKV creates an empty KV state machine.
func NewKV() *KV {
	return &KV{data: make(map[string]string)}
}

// Apply applies a command. Supported formats:
//   - "set key value"
//   - "delete key"
func (kv *KV) Apply(index uint64, command []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	parts := strings.SplitN(string(command), " ", 3)
	if len(parts) < 2 {
		return ErrBadCommand
	}

	switch parts[0] {
	case "set":
		if len(parts) < 3 {
			return ErrBadCommand
		}
		kv.data[parts[1]] = parts[2]
	case "delete":
		delete(kv.data, parts[1])
	default:
		return ErrBadCommand
	}
	kv.applied = index
	return nil
}

// Get returns the value for a key.
func (kv *KV) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.data[key]
	return v, ok
}

// Keys returns all keys sorted.
func (kv *KV) Keys() []string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	out := make([]string, 0, len(kv.data))
	for k := range kv.data {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Len returns the number of keys.
func (kv *KV) Len() int {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return len(kv.data)
}

// LastApplied returns the index of the last applied entry.
func (kv *KV) LastApplied() uint64 {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.applied
}

// Snapshot serializes the current state.
func (kv *KV) Snapshot() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return json.Marshal(kv.data)
}

// Restore replaces the state from a snapshot.
func (kv *KV) Restore(data []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	m := make(map[string]string)
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	kv.data = m
	return nil
}

// Clear removes all data.
func (kv *KV) Clear() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data = make(map[string]string)
	kv.applied = 0
}
