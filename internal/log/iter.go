package log

// Iterator provides sequential access to log entries.
type Iterator struct {
	log *Log
	pos int
}

// Iter returns an iterator starting from the first real entry.
func (l *Log) Iter() *Iterator {
	return &Iterator{log: l, pos: 1}
}

// Valid reports whether the iterator points to a valid entry.
func (it *Iterator) Valid() bool {
	return it.pos < len(it.log.entries)
}

// Next advances to the next entry.
func (it *Iterator) Next() {
	it.pos++
}

// Entry returns the current entry.
func (it *Iterator) Entry() Entry {
	if !it.Valid() {
		return Entry{}
	}
	return it.log.entries[it.pos]
}

// Index returns the current entry's index.
func (it *Iterator) Index() uint64 {
	if !it.Valid() {
		return 0
	}
	return it.log.entries[it.pos].Index
}

// Rewind resets to the beginning.
func (it *Iterator) Rewind() {
	it.pos = 1
}

// Seek positions at the entry with the given index.
func (it *Iterator) Seek(index uint64) {
	base := it.log.entries[0].Index
	it.pos = int(index - base)
	if it.pos < 1 {
		it.pos = 1
	}
}

// Count returns the total number of real entries.
func (it *Iterator) Count() int {
	n := len(it.log.entries) - 1
	if n < 0 {
		return 0
	}
	return n
}
