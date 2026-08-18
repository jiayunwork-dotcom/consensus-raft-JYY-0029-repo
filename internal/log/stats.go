package log

// Stats holds log statistics.
type Stats struct {
	EntryCount  int
	CommitIndex uint64
	LastIndex   uint64
	LastTerm    uint64
	TotalBytes  int
}

// ComputeStats returns current log statistics.
func (l *Log) ComputeStats() Stats {
	lastIdx, lastTerm := l.Last()
	totalBytes := 0
	for _, e := range l.entries[1:] {
		totalBytes += len(e.Command)
	}
	return Stats{
		EntryCount:  len(l.entries) - 1,
		CommitIndex: l.commitIndex,
		LastIndex:   lastIdx,
		LastTerm:    lastTerm,
		TotalBytes:  totalBytes,
	}
}

// TermDistribution returns a map of term -> count of entries in that term.
func (l *Log) TermDistribution() map[uint64]int {
	dist := make(map[uint64]int)
	for _, e := range l.entries[1:] {
		dist[e.Term]++
	}
	return dist
}

// AverageEntrySize returns the average command size in bytes.
func (l *Log) AverageEntrySize() float64 {
	n := len(l.entries) - 1
	if n == 0 {
		return 0
	}
	total := 0
	for _, e := range l.entries[1:] {
		total += len(e.Command)
	}
	return float64(total) / float64(n)
}
