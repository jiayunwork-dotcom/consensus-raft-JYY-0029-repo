package log

import (
	"bytes"
	"errors"
	"testing"
)

// makeEntries builds n contiguous entries with the given term pattern.
func makeEntries(start, n uint64, term uint64) []Entry {
	out := make([]Entry, 0, n)
	for i := uint64(0); i < n; i++ {
		out = append(out, Entry{Term: term, Index: start + i, Command: []byte{byte(start + i)}})
	}
	return out
}

func TestLogAppendAndTermAt(t *testing.T) {
	l := New()
	if got, _ := l.Last(); got != 0 {
		t.Fatalf("empty log Last().Index = %d, want 0", got)
	}

	if err := l.Append(makeEntries(1, 3, 1)); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if idx, term := l.Last(); idx != 3 || term != 1 {
		t.Fatalf("after append Last() = (%d,%d), want (3,1)", idx, term)
	}

	// TermAt for every present index.
	for i := uint64(1); i <= 3; i++ {
		term, err := l.TermAt(i)
		if err != nil {
			t.Fatalf("TermAt(%d) error: %v", i, err)
		}
		if term != 1 {
			t.Fatalf("TermAt(%d) = %d, want 1", i, term)
		}
	}

	// TermAt(0) is the dummy term 0.
	if term, _ := l.TermAt(0); term != 0 {
		t.Fatalf("TermAt(0) = %d, want 0", term)
	}

	// Out-of-range returns ErrIndexOutOfRange.
	if _, err := l.TermAt(4); !errors.Is(err, ErrIndexOutOfRange) {
		t.Fatalf("TermAt(4) err = %v, want ErrIndexOutOfRange", err)
	}
	if _, err := l.At(99); !errors.Is(err, ErrIndexOutOfRange) {
		t.Fatalf("At(99) err = %v, want ErrIndexOutOfRange", err)
	}

	// At() returns a copy with the right command bytes.
	e, err := l.At(2)
	if err != nil {
		t.Fatalf("At(2) error: %v", err)
	}
	if !bytes.Equal(e.Command, []byte{2}) {
		t.Fatalf("At(2).Command = %v, want [2]", e.Command)
	}

	// Slice is half-open and clamped.
	sl := l.Slice(1, 4)
	if len(sl) != 3 || sl[0].Index != 1 || sl[2].Index != 3 {
		t.Fatalf("Slice(1,4) = %d entries, want 3 with indices 1..3", len(sl))
	}
	if got := l.Slice(2, 2); got != nil {
		t.Fatalf("Slice(2,2) = %v, want nil", got)
	}
	if got := l.Slice(100, 200); got != nil {
		t.Fatalf("Slice(100,200) = %v, want nil", got)
	}
}

func TestLogTruncateConflict(t *testing.T) {
	l := New()
	// Log: 1(t1) 2(t1) 3(t1) 4(t1)
	if err := l.Append(makeEntries(1, 4, 1)); err != nil {
		t.Fatalf("seed Append: %v", err)
	}

	// Conflict at index 3 (term 1 -> term 2). Everything from 3 onward is dropped
	// and the new entries replace it.
	conflict := []Entry{
		{Term: 2, Index: 3, Command: []byte("x")},
		{Term: 2, Index: 4, Command: []byte("y")},
		{Term: 2, Index: 5, Command: []byte("z")},
	}
	if err := l.Append(conflict); err != nil {
		t.Fatalf("conflict Append: %v", err)
	}
	if idx, term := l.Last(); idx != 5 || term != 2 {
		t.Fatalf("after conflict Last() = (%d,%d), want (5,2)", idx, term)
	}
	if term, _ := l.TermAt(2); term != 1 {
		t.Fatalf("TermAt(2) = %d, want 1 (prefix must be preserved)", term)
	}
	if term, _ := l.TermAt(3); term != 2 {
		t.Fatalf("TermAt(3) = %d, want 2 (conflict overwritten)", term)
	}

	// Truncate at index 2 removes 2..5 and leaves only index 1.
	if err := l.Truncate(2); err != nil {
		t.Fatalf("Truncate(2): %v", err)
	}
	if idx, _ := l.Last(); idx != 1 {
		t.Fatalf("after Truncate(2) Last().Index = %d, want 1", idx)
	}

	// Truncating the dummy index is invalid.
	if err := l.Truncate(0); err == nil {
		t.Fatalf("Truncate(0) should error")
	}

	// Truncating past the end leaves a gap and is invalid.
	fresh := New()
	_ = fresh.Append(makeEntries(1, 2, 1))
	if err := fresh.Truncate(5); err == nil {
		t.Fatalf("Truncate(5) on a 2-entry log should error (gap)")
	}

	// Truncate at Last()+1 is a no-op.
	if err := fresh.Truncate(3); err != nil {
		t.Fatalf("Truncate(3) no-op should succeed, got %v", err)
	}
	if idx, _ := fresh.Last(); idx != 2 {
		t.Fatalf("after no-op Truncate(3) Last().Index = %d, want 2", idx)
	}
}

func TestLogMatchingProperty(t *testing.T) {
	l := New()
	_ = l.Append(makeEntries(1, 5, 1))

	// prevIndex 0 always matches (empty prefix).
	if !l.MatchesPrevLogTerm(0, 0) {
		t.Fatalf("MatchesPrevLogTerm(0,0) = false, want true")
	}

	// Matching term at a present index.
	if !l.MatchesPrevLogTerm(3, 1) {
		t.Fatalf("MatchesPrevLogTerm(3,1) = false, want true")
	}

	// Wrong term at a present index.
	if l.MatchesPrevLogTerm(3, 2) {
		t.Fatalf("MatchesPrevLogTerm(3,2) = true, want false (term mismatch)")
	}

	// Index beyond the log cannot match.
	if l.MatchesPrevLogTerm(6, 1) {
		t.Fatalf("MatchesPrevLogTerm(6,1) = true, want false (out of range)")
	}

	// A realistic AppendEntries flow: leader sends entries from prevIndex+1.
	// Follower has 1..5(t1). Leader's prevIndex=5, prevTerm=1, entries 6(t2).
	if !l.MatchesPrevLogTerm(5, 1) {
		t.Fatalf("follower should match prev (5,1)")
	}
	if err := l.Append(makeEntries(6, 1, 2)); err != nil {
		t.Fatalf("Append(6): %v", err)
	}
	if term, _ := l.TermAt(6); term != 2 {
		t.Fatalf("TermAt(6) = %d, want 2", term)
	}

	// Now a divergent leader claims prevIndex=4, prevTerm=1, with entries 5(t3).
	// Follower has 5(t1): the match check at prevIndex=4 passes (term 1), but the
	// entry 5 conflicts (t1 vs t3) so Append must truncate and overwrite.
	follower := New()
	_ = follower.Append(makeEntries(1, 5, 1))
	if !follower.MatchesPrevLogTerm(4, 1) {
		t.Fatalf("divergent match at prevIndex 4 should pass")
	}
	if err := follower.Append([]Entry{{Term: 3, Index: 5, Command: []byte("q")}}); err != nil {
		t.Fatalf("divergent Append: %v", err)
	}
	if idx, term := follower.Last(); idx != 5 || term != 3 {
		t.Fatalf("after divergent overwrite Last() = (%d,%d), want (5,3)", idx, term)
	}
}

func TestLogCommitMonotonic(t *testing.T) {
	l := New()
	_ = l.Append(makeEntries(1, 5, 1))

	if l.CommitIndex() != 0 {
		t.Fatalf("initial CommitIndex = %d, want 0", l.CommitIndex())
	}

	// Advancing is allowed.
	if err := l.CommitTo(3); err != nil {
		t.Fatalf("CommitTo(3): %v", err)
	}
	if l.CommitIndex() != 3 {
		t.Fatalf("CommitIndex = %d, want 3", l.CommitIndex())
	}

	// Moving forward again is allowed.
	if err := l.CommitTo(4); err != nil {
		t.Fatalf("CommitTo(4): %v", err)
	}

	// Moving backwards is an error and leaves the index unchanged.
	if err := l.CommitTo(2); !errors.Is(err, ErrCommitRegression) {
		t.Fatalf("CommitTo(2) err = %v, want ErrCommitRegression", err)
	}
	if l.CommitIndex() != 4 {
		t.Fatalf("after regression attempt CommitIndex = %d, want 4", l.CommitIndex())
	}

	// Same value is a no-op (not an error).
	if err := l.CommitTo(4); err != nil {
		t.Fatalf("CommitTo(4) idempotent should be nil, got %v", err)
	}

	// Committing beyond the last entry is rejected.
	if err := l.CommitTo(6); err == nil {
		t.Fatalf("CommitTo(6) beyond last entry should error")
	}

	// CommittedEntries returns exactly the committed prefix.
	committed := l.CommittedEntries()
	if len(committed) != 4 {
		t.Fatalf("CommittedEntries len = %d, want 4", len(committed))
	}
	for i, e := range committed {
		if e.Index != uint64(i+1) {
			t.Fatalf("CommittedEntries[%d].Index = %d, want %d", i, e.Index, i+1)
		}
	}
}

func TestPersistentStateRoundtrip(t *testing.T) {
	cases := []struct {
		name string
		in   PersistedState
	}{
		{"empty", PersistedState{CurrentTerm: 0, VotedFor: 0, Entries: nil}},
		{"term-only", PersistedState{CurrentTerm: 7, VotedFor: 0, Entries: nil}},
		{
			name: "with-entries",
			in: PersistedState{
				CurrentTerm: 5,
				VotedFor:    2,
				Entries: []Entry{
					{Term: 1, Index: 1, Command: []byte("a")},
					{Term: 1, Index: 2, Command: []byte("bb")},
					{Term: 3, Index: 3, Command: []byte("ccc")},
				},
			},
		},
		{
			name: "empty-command-entries",
			in: PersistedState{
				CurrentTerm: 2,
				VotedFor:    1,
				Entries: []Entry{
					{Term: 1, Index: 1, Command: nil},
					{Term: 2, Index: 2, Command: []byte{}},
				},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			data := Serialize(c.in)
			out, err := Deserialize(data)
			if err != nil {
				t.Fatalf("Deserialize: %v", err)
			}
			if out.CurrentTerm != c.in.CurrentTerm {
				t.Fatalf("CurrentTerm = %d, want %d", out.CurrentTerm, c.in.CurrentTerm)
			}
			if out.VotedFor != c.in.VotedFor {
				t.Fatalf("VotedFor = %d, want %d", out.VotedFor, c.in.VotedFor)
			}
			if len(out.Entries) != len(c.in.Entries) {
				t.Fatalf("entries len = %d, want %d", len(out.Entries), len(c.in.Entries))
			}
			for i := range c.in.Entries {
				want, got := c.in.Entries[i], out.Entries[i]
				if want.Term != got.Term || want.Index != got.Index {
					t.Fatalf("entry %d metadata mismatch: want (t%d,i%d) got (t%d,i%d)",
						i, want.Term, want.Index, got.Term, got.Index)
				}
				if !bytes.Equal(want.Command, got.Command) {
					t.Fatalf("entry %d command mismatch: want %q got %q", i, want.Command, got.Command)
				}
			}
			// The re-serialized form must be byte-identical (deterministic).
			if !bytes.Equal(data, Serialize(out)) {
				t.Fatalf("re-serialized bytes differ from original")
			}
		})
	}

	// Corruption cases must all return ErrCorruptState.
	valid := Serialize(PersistedState{CurrentTerm: 1, VotedFor: 0, Entries: makeEntries(1, 2, 1)})
	corruptInputs := []struct {
		name string
		data []byte
	}{
		{"truncated-header", valid[:5]},
		{"truncated-entry", valid[:len(valid)-1]},
		{"empty", []byte{}},
		{"bad-count", func() []byte {
			b := Serialize(PersistedState{CurrentTerm: 1, VotedFor: 0, Entries: nil})
			// Overwrite numEntries (last 8 bytes of header) with a huge value.
			binaryPatch := make([]byte, len(b))
			copy(binaryPatch, b)
			for i := 0; i < 8; i++ {
				binaryPatch[len(b)-8+i] = 0xff
			}
			return binaryPatch
		}()},
	}
	for _, c := range corruptInputs {
		t.Run("corrupt-"+c.name, func(t *testing.T) {
			if _, err := Deserialize(c.data); !errors.Is(err, ErrCorruptState) {
				t.Fatalf("Deserialize corrupt %q err = %v, want ErrCorruptState", c.name, err)
			}
		})
	}

	// Non-contiguous indices must be detected.
	bad := Serialize(PersistedState{CurrentTerm: 1, VotedFor: 0, Entries: nil})
	// Build a buffer with two contiguous-looking-but-gapped entries.
	// Use the low-level layout: term8 index8 cmdLen8 command.
	// entry1: term=1 index=1 cmdLen=0 ; entry2: term=1 index=3 cmdLen=0 (gap at 2)
	var buf bytes.Buffer
	put := func(v uint64) {
		var b [8]byte
		for i := 0; i < 8; i++ {
			b[i] = byte(v >> (8 * i))
		}
		buf.Write(b[:])
	}
	put(1) // currentTerm
	put(0) // votedFor
	put(2) // numEntries
	put(1) // entry1 term
	put(1) // entry1 index
	put(0) // entry1 cmdLen
	put(1) // entry2 term
	put(3) // entry2 index (gap: expected 2)
	put(0) // entry2 cmdLen
	if _, err := Deserialize(buf.Bytes()); !errors.Is(err, ErrCorruptState) {
		t.Fatalf("non-contiguous deserialize err = %v, want ErrCorruptState", err)
	}
	_ = bad
}
