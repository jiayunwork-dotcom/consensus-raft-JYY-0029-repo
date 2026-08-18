// Package log implements the replicated log used by the Raft consensus core.
//
// Log indices are 1-based; index 0 is an implicit empty entry with term 0 that
// every node always "has". This keeps the math of Raft's Log Matching Property
// and the previous-log-term checks simple and uniform.
//
// The package is deterministic and free of any I/O or clock usage. It is safe to
// step from a single goroutine (the parent node drives it), and it performs no
// allocations that depend on wall-clock time.
package log

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrIndexOutOfRange is returned when an index is not currently present in the
// log.
var ErrIndexOutOfRange = errors.New("log: index out of range")

// ErrCommitRegression is returned when CommitTo is asked to move the commit
// index backwards. The commit index must only ever advance.
var ErrCommitRegression = errors.New("log: commit index cannot move backwards")

// ErrCorruptState is returned when persisted state cannot be decoded.
var ErrCorruptState = errors.New("log: corrupt persisted state")

// Entry is a single replicated log entry.
type Entry struct {
	Term    uint64
	Index   uint64
	Command []byte
}

// Log is the replicated log of a single Raft peer.
//
// Internally entries[0] is the dummy index-0 entry (term 0) so that the real
// entry with index i lives at entries[i]. commitIndex is the highest index
// known to be committed; it only moves forward (see CommitTo).
type Log struct {
	entries     []Entry
	commitIndex uint64
}

// New returns an empty log with just the dummy index-0 entry.
func New() *Log {
	return &Log{
		entries:     []Entry{{Term: 0, Index: 0, Command: nil}},
		commitIndex: 0,
	}
}

// Len returns the number of real (index >= 1) entries in the log.
func (l *Log) Len() int { return len(l.entries) - 1 }

// Last returns the index and term of the last entry, or (0, 0) for an empty log.
func (l *Log) Last() (index uint64, term uint64) {
	if len(l.entries) == 1 {
		return 0, 0
	}
	last := l.entries[len(l.entries)-1]
	return last.Index, last.Term
}

// At returns the entry stored at the given index.
//
// Index 0 returns the dummy entry (term 0) without error. Any other index
// outside [1, Last().Index] returns ErrIndexOutOfRange.
func (l *Log) At(index uint64) (Entry, error) {
	if index >= uint64(len(l.entries)) {
		return Entry{}, ErrIndexOutOfRange
	}
	return l.entries[index], nil
}

// TermAt returns the term of the entry at the given index.
//
// Index 0 returns term 0 (the dummy entry). An index beyond the last entry
// returns ErrIndexOutOfRange.
func (l *Log) TermAt(index uint64) (uint64, error) {
	if index >= uint64(len(l.entries)) {
		return 0, ErrIndexOutOfRange
	}
	return l.entries[index].Term, nil
}

// Slice returns the contiguous entries in the half-open range [lo, hi).
//
// The range is clamped to the available entries: lo is bounded below by 1 and hi
// is bounded above by Last().Index+1. An empty range yields a nil slice.
func (l *Log) Slice(lo, hi uint64) []Entry {
	if lo < 1 {
		lo = 1
	}
	lastIdx, _ := l.Last()
	if hi > lastIdx+1 {
		hi = lastIdx + 1
	}
	if lo >= hi {
		return nil
	}
	out := make([]Entry, 0, hi-lo)
	for i := lo; i < hi; i++ {
		out = append(out, l.entries[i])
	}
	return out
}

// MatchesPrevLogTerm implements the Raft Log Matching Property's previous-entry
// check used by AppendEntries.
//
// prevIndex == 0 always matches (an empty prefix). Otherwise the caller must
// have an entry at prevIndex whose term equals prevTerm.
func (l *Log) MatchesPrevLogTerm(prevIndex, prevTerm uint64) bool {
	if prevIndex == 0 {
		return true
	}
	if prevIndex >= uint64(len(l.entries)) {
		return false
	}
	return l.entries[prevIndex].Term == prevTerm
}

// Append appends entries received from the leader, performing conflict
// resolution exactly as the Raft paper specifies.
//
// The supplied entries must be contiguous and in ascending index order. If an
// existing entry at the same index has a different term, every entry from that
// index onward is discarded (Truncate) and the new entries are appended in its
// place. Entries whose index the log already holds with a matching term are
// skipped (the leader may resend a prefix).
func (l *Log) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	// Validate contiguity and ascending order before mutating.
	for i := 1; i < len(entries); i++ {
		if entries[i].Index != entries[i-1].Index+1 {
			return fmt.Errorf("log: non-contiguous append at index %d (previous %d)",
				entries[i].Index, entries[i-1].Index)
		}
	}
	// Find the first conflict (same index, different term) or the first entry
	// that extends beyond the current log.
	for i, e := range entries {
		if e.Index >= uint64(len(l.entries)) {
			// e extends past the end: append the remainder.
			l.appendTail(entries[i:])
			return nil
		}
		if l.entries[e.Index].Term != e.Term {
			// Conflict: truncate from e.Index and append the remainder.
			l.Truncate(e.Index)
			l.appendTail(entries[i:])
			return nil
		}
	}
	// No conflict and all entries already present: nothing to append.
	return nil
}

// appendTail appends already-validated entries to the end of the log.
func (l *Log) appendTail(entries []Entry) {
	for _, e := range entries {
		cmd := make([]byte, len(e.Command))
		copy(cmd, e.Command)
		l.entries = append(l.entries, Entry{Term: e.Term, Index: e.Index, Command: cmd})
	}
}

// Truncate discards every entry with index >= fromIndex. It is the conflict
// resolution step of the Raft log.
//
// Truncating at index 0 is rejected (the dummy entry is immutable). Truncating
// past the end of the log (fromIndex > Last().Index+1) is rejected because it
// would leave a gap. Truncating at Last().Index+1 is a no-op.
func (l *Log) Truncate(fromIndex uint64) error {
	if fromIndex == 0 {
		return errors.New("log: cannot truncate the dummy index-0 entry")
	}
	lastIdx, _ := l.Last()
	if fromIndex > lastIdx+1 {
		return fmt.Errorf("log: truncate at %d leaves a gap (last index %d)", fromIndex, lastIdx)
	}
	// Keep entries[0:fromIndex]; fromIndex is the count of kept entries.
	l.entries = l.entries[:fromIndex]
	// A truncation can never move the commit index backwards beyond what remains,
	// but it may need to clamp it down to the new last index.
	if l.commitIndex > lastIdx {
		l.commitIndex = lastIdx
	}
	if l.commitIndex > fromIndex-1 {
		l.commitIndex = fromIndex - 1
	}
	return nil
}

// CommitIndex returns the highest committed index.
func (l *Log) CommitIndex() uint64 { return l.commitIndex }

// CommitTo advances the commit index to the supplied value.
//
// The commit index is monotonic: requesting a value <= the current commit index
// returns ErrCommitRegression. Requesting a value beyond the last entry is
// rejected because an entry cannot be committed before it exists.
func (l *Log) CommitTo(index uint64) error {
	lastIdx, _ := l.Last()
	if index > lastIdx {
		return fmt.Errorf("log: cannot commit index %d (last index %d)", index, lastIdx)
	}
	if index < l.commitIndex {
		return ErrCommitRegression
	}
	if index == l.commitIndex {
		return nil
	}
	l.commitIndex = index
	return nil
}

// CommittedEntries returns the entries whose index is <= the commit index.
func (l *Log) CommittedEntries() []Entry {
	lastIdx, _ := l.Last()
	hi := l.commitIndex
	if hi > lastIdx {
		hi = lastIdx
	}
	return l.Slice(1, hi+1)
}

// ---- Persistent state serialization ----

// PersistedState is the subset of a peer's state that must survive a restart:
// the current term, who this peer voted for, and the replicated log entries.
type PersistedState struct {
	CurrentTerm uint64
	VotedFor    uint64
	Entries     []Entry
}

// Serialize turns the persisted state into a deterministic byte slice.
//
// Layout (all multi-byte integers little-endian):
//
//	[currentTerm:8][votedFor:8][numEntries:8]
//	repeated numEntries times:
//	  [term:8][index:8][cmdLen:8][command:cmdLen]
func Serialize(s PersistedState) []byte {
	var buf bytes.Buffer
	var scratch [8]byte
	putU64 := func(v uint64) {
		binary.LittleEndian.PutUint64(scratch[:], v)
		buf.Write(scratch[:])
	}
	putU64(s.CurrentTerm)
	putU64(s.VotedFor)
	putU64(uint64(len(s.Entries)))
	for _, e := range s.Entries {
		putU64(e.Term)
		putU64(e.Index)
		putU64(uint64(len(e.Command)))
		buf.Write(e.Command)
	}
	return buf.Bytes()
}

// Deserialize reconstructs persisted state from a byte slice produced by
// Serialize. It returns a descriptive ErrCorruptState on any malformed input
// (truncated buffers, impossible lengths, or mismatched index ordering).
func Deserialize(data []byte) (PersistedState, error) {
	var s PersistedState
	pos := 0
	readU64 := func() (uint64, bool) {
		if pos+8 > len(data) {
			return 0, false
		}
		v := binary.LittleEndian.Uint64(data[pos : pos+8])
		pos += 8
		return v, true
	}

	cur, ok := readU64()
	if !ok {
		return s, fmt.Errorf("%w: truncated header (currentTerm)", ErrCorruptState)
	}
	s.CurrentTerm = cur

	vf, ok := readU64()
	if !ok {
		return s, fmt.Errorf("%w: truncated header (votedFor)", ErrCorruptState)
	}
	s.VotedFor = vf

	n, ok := readU64()
	if !ok {
		return s, fmt.Errorf("%w: truncated header (numEntries)", ErrCorruptState)
	}
	if n > uint64(len(data))/24+1 {
		return s, fmt.Errorf("%w: implausible entry count %d", ErrCorruptState, n)
	}

	s.Entries = make([]Entry, 0, n)
	var prevIndex uint64
	for i := uint64(0); i < n; i++ {
		term, ok1 := readU64()
		index, ok2 := readU64()
		cmdLen, ok3 := readU64()
		if !ok1 || !ok2 || !ok3 {
			return s, fmt.Errorf("%w: truncated entry %d", ErrCorruptState, i)
		}
		if index != prevIndex+1 {
			return s, fmt.Errorf("%w: entry %d has non-contiguous index %d (expected %d)",
				ErrCorruptState, i, index, prevIndex+1)
		}
		prevIndex = index
		if uint64(len(data)-pos) < cmdLen {
			return s, fmt.Errorf("%w: truncated command for entry %d", ErrCorruptState, i)
		}
		cmd := make([]byte, cmdLen)
		copy(cmd, data[pos:pos+int(cmdLen)])
		pos += int(cmdLen)
		s.Entries = append(s.Entries, Entry{Term: term, Index: index, Command: cmd})
	}
	return s, nil
}
