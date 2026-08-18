package log

// Batch accumulates entries before appending them all at once.
type Batch struct {
	entries []Entry
}

// NewBatch creates an empty batch.
func NewBatch() *Batch {
	return &Batch{}
}

// Add appends an entry to the batch.
func (b *Batch) Add(e Entry) {
	b.entries = append(b.entries, e)
}

// Len returns the number of entries in the batch.
func (b *Batch) Len() int { return len(b.entries) }

// Entries returns the batch entries.
func (b *Batch) Entries() []Entry {
	out := make([]Entry, len(b.entries))
	copy(out, b.entries)
	return out
}

// Apply writes the batch to the log.
func (b *Batch) Apply(l *Log) error {
	if len(b.entries) == 0 {
		return nil
	}
	return l.Append(b.entries)
}

// Reset clears the batch.
func (b *Batch) Reset() {
	b.entries = b.entries[:0]
}

// TotalBytes returns the sum of all command sizes in the batch.
func (b *Batch) TotalBytes() int {
	total := 0
	for _, e := range b.entries {
		total += len(e.Command)
	}
	return total
}
