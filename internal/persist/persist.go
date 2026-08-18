// Package persist implements durable state persistence for Raft nodes.
// It stores the current term, voted-for, and log entries to a file so that
// a node can recover after a crash.
package persist

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"consensus-raft/internal/log"
)

// ErrCorrupt is returned when persisted state cannot be decoded.
var ErrCorrupt = errors.New("persist: corrupt state file")

// magic identifies a state file.
var magic = [4]byte{'R', 'F', 'S', '1'}

// State is the durable state of a Raft node.
type State struct {
	CurrentTerm uint64
	VotedFor    uint64
	Entries     []log.Entry
}

// Save writes the state to a file.
func Save(path string, s *State) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(magic[:]); err != nil {
		return err
	}
	if err := writeUint64(f, s.CurrentTerm); err != nil {
		return err
	}
	if err := writeUint64(f, s.VotedFor); err != nil {
		return err
	}
	if err := writeUint64(f, uint64(len(s.Entries))); err != nil {
		return err
	}
	for _, e := range s.Entries {
		if err := writeEntry(f, e); err != nil {
			return err
		}
	}
	return f.Sync()
}

// Load reads the state from a file. Returns a zero state if the file does not
// exist.
func Load(path string) (*State, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &State{}, nil
		}
		return nil, err
	}
	defer f.Close()

	var m [4]byte
	if _, err := io.ReadFull(f, m[:]); err != nil {
		return nil, fmt.Errorf("%w: read magic: %v", ErrCorrupt, err)
	}
	if m != magic {
		return nil, fmt.Errorf("%w: bad magic", ErrCorrupt)
	}

	term, err := readUint64(f)
	if err != nil {
		return nil, err
	}
	votedFor, err := readUint64(f)
	if err != nil {
		return nil, err
	}
	count, err := readUint64(f)
	if err != nil {
		return nil, err
	}
	if count > 1<<24 {
		return nil, fmt.Errorf("%w: absurd entry count", ErrCorrupt)
	}

	entries := make([]log.Entry, count)
	for i := range entries {
		e, err := readEntry(f)
		if err != nil {
			return nil, err
		}
		entries[i] = e
	}

	return &State{
		CurrentTerm: term,
		VotedFor:    votedFor,
		Entries:     entries,
	}, nil
}

// Exists reports whether a state file exists at path.
func Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func writeUint64(w io.Writer, v uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	_, err := w.Write(buf[:])
	return err
}

func readUint64(r io.Reader) (uint64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}

func writeEntry(w io.Writer, e log.Entry) error {
	if err := writeUint64(w, e.Term); err != nil {
		return err
	}
	if err := writeUint64(w, e.Index); err != nil {
		return err
	}
	if err := writeUint64(w, uint64(len(e.Command))); err != nil {
		return err
	}
	if len(e.Command) > 0 {
		if _, err := w.Write(e.Command); err != nil {
			return err
		}
	}
	return nil
}

func readEntry(r io.Reader) (log.Entry, error) {
	term, err := readUint64(r)
	if err != nil {
		return log.Entry{}, err
	}
	index, err := readUint64(r)
	if err != nil {
		return log.Entry{}, err
	}
	cmdLen, err := readUint64(r)
	if err != nil {
		return log.Entry{}, err
	}
	if cmdLen > 1<<24 {
		return log.Entry{}, fmt.Errorf("%w: absurd command length", ErrCorrupt)
	}
	var cmd []byte
	if cmdLen > 0 {
		cmd = make([]byte, cmdLen)
		if _, err := io.ReadFull(r, cmd); err != nil {
			return log.Entry{}, err
		}
	}
	return log.Entry{Term: term, Index: index, Command: cmd}, nil
}
