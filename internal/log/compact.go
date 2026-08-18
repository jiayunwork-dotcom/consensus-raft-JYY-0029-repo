package log

// Compact discards all entries up to and including compactIndex. The entry at
// compactIndex becomes the new base entry (index 0 sentinel equivalent).
// Returns an error if compactIndex is beyond the commit index.
func (l *Log) Compact(compactIndex uint64) error {
	if compactIndex > l.commitIndex {
		return ErrIndexOutOfRange
	}
	if compactIndex == 0 {
		return nil
	}

	// Find the position in our entries slice.
	base := l.entries[0].Index
	pos := int(compactIndex - base)
	if pos <= 0 || pos >= len(l.entries) {
		return ErrIndexOutOfRange
	}

	// Keep a sentinel at position 0 with the compacted entry's term/index.
	sentinel := Entry{
		Term:  l.entries[pos].Term,
		Index: l.entries[pos].Index,
	}
	remaining := make([]Entry, 1+len(l.entries)-pos-1)
	remaining[0] = sentinel
	copy(remaining[1:], l.entries[pos+1:])
	l.entries = remaining
	return nil
}

// CompactedIndex returns the index of the compaction base (the sentinel).
func (l *Log) CompactedIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[0].Index
}

// CompactedTerm returns the term of the compaction base.
func (l *Log) CompactedTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[0].Term
}

// EntrySince returns entries from startIndex onwards (inclusive).
func (l *Log) EntrySince(startIndex uint64) []Entry {
	base := l.entries[0].Index
	if startIndex <= base {
		startIndex = base + 1
	}
	pos := int(startIndex - base)
	if pos >= len(l.entries) {
		return nil
	}
	out := make([]Entry, len(l.entries)-pos)
	copy(out, l.entries[pos:])
	return out
}
