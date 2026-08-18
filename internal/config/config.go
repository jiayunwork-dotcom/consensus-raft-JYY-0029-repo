// Package config defines tunable Raft parameters.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrInvalid is returned for bad config values.
var ErrInvalid = errors.New("config: invalid")

// Config holds Raft timing and cluster parameters.
type Config struct {
	// ElectionTimeoutMin is the minimum ticks before starting an election.
	ElectionTimeoutMin int `json:"election_timeout_min"`
	// ElectionTimeoutMax is the maximum ticks (random in [min, max]).
	ElectionTimeoutMax int `json:"election_timeout_max"`
	// HeartbeatInterval is ticks between leader heartbeats.
	HeartbeatInterval int `json:"heartbeat_interval"`
	// MaxEntriesPerAppend limits batch size in AppendEntries.
	MaxEntriesPerAppend int `json:"max_entries_per_append"`
	// SnapshotThreshold triggers a snapshot after this many committed entries.
	SnapshotThreshold int `json:"snapshot_threshold"`
}

// Default returns production defaults.
func Default() Config {
	return Config{
		ElectionTimeoutMin:  10,
		ElectionTimeoutMax:  20,
		HeartbeatInterval:   3,
		MaxEntriesPerAppend: 100,
		SnapshotThreshold:   1000,
	}
}

// Validate checks constraints.
func (c *Config) Validate() error {
	if c.ElectionTimeoutMin <= 0 {
		return fmt.Errorf("%w: election_timeout_min must be positive", ErrInvalid)
	}
	if c.ElectionTimeoutMax < c.ElectionTimeoutMin {
		return fmt.Errorf("%w: election_timeout_max < min", ErrInvalid)
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("%w: heartbeat_interval must be positive", ErrInvalid)
	}
	if c.HeartbeatInterval >= c.ElectionTimeoutMin {
		return fmt.Errorf("%w: heartbeat must be less than election timeout", ErrInvalid)
	}
	if c.MaxEntriesPerAppend <= 0 {
		return fmt.Errorf("%w: max_entries_per_append must be positive", ErrInvalid)
	}
	if c.SnapshotThreshold <= 0 {
		return fmt.Errorf("%w: snapshot_threshold must be positive", ErrInvalid)
	}
	return nil
}

// Save writes config to dir/config.json.
func (c *Config) Save(dir string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "config.json"), data, 0o644)
}

// Load reads config from dir.
func Load(dir string) (Config, error) {
	path := filepath.Join(dir, "config.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Default(), nil
		}
		return Config{}, err
	}
	var c Config
	if err := json.Unmarshal(data, &c); err != nil {
		return Config{}, err
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}
