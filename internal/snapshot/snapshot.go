// Package snapshot implements log compaction via snapshots. When the committed
// log grows beyond a threshold, the state machine can be snapshotted and old
// entries discarded, reducing memory and recovery time.
package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

// ErrCorrupt is returned when a snapshot file is malformed.
var ErrCorrupt = errors.New("snapshot: corrupt")

var magic = [4]byte{'S', 'N', 'A', 'P'}

// Metadata describes the snapshot's position in the log.
type Metadata struct {
	LastIndex uint64
	LastTerm  uint64
	// Peers is the cluster configuration at the time of the snapshot.
	Peers []uint64
}

// Snapshot is a point-in-time state machine snapshot.
type Snapshot struct {
	Meta Metadata
	Data []byte
}

// Save writes a snapshot to a file.
func Save(path string, snap *Snapshot) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(magic[:]); err != nil {
		return err
	}
	if err := writeU64(f, snap.Meta.LastIndex); err != nil {
		return err
	}
	if err := writeU64(f, snap.Meta.LastTerm); err != nil {
		return err
	}
	if err := writeU64(f, uint64(len(snap.Meta.Peers))); err != nil {
		return err
	}
	for _, p := range snap.Meta.Peers {
		if err := writeU64(f, p); err != nil {
			return err
		}
	}
	if err := writeU64(f, uint64(len(snap.Data))); err != nil {
		return err
	}
	if _, err := f.Write(snap.Data); err != nil {
		return err
	}
	return f.Sync()
}

// Load reads a snapshot from a file.
func Load(path string) (*Snapshot, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	var m [4]byte
	if _, err := io.ReadFull(f, m[:]); err != nil {
		return nil, fmt.Errorf("%w: magic: %v", ErrCorrupt, err)
	}
	if m != magic {
		return nil, fmt.Errorf("%w: bad magic", ErrCorrupt)
	}
	lastIndex, err := readU64(f)
	if err != nil {
		return nil, err
	}
	lastTerm, err := readU64(f)
	if err != nil {
		return nil, err
	}
	peerCount, err := readU64(f)
	if err != nil {
		return nil, err
	}
	peers := make([]uint64, peerCount)
	for i := range peers {
		p, err := readU64(f)
		if err != nil {
			return nil, err
		}
		peers[i] = p
	}
	dataLen, err := readU64(f)
	if err != nil {
		return nil, err
	}
	if dataLen > 1<<30 {
		return nil, fmt.Errorf("%w: absurd data length", ErrCorrupt)
	}
	data := make([]byte, dataLen)
	if _, err := io.ReadFull(f, data); err != nil {
		return nil, err
	}
	return &Snapshot{
		Meta: Metadata{LastIndex: lastIndex, LastTerm: lastTerm, Peers: peers},
		Data: data,
	}, nil
}

func writeU64(w io.Writer, v uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	_, err := w.Write(buf[:])
	return err
}

func readU64(r io.Reader) (uint64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}
