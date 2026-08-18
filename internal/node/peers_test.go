package node

import "testing"

func TestPeerTrackerBasic(t *testing.T) {
	pt := NewPeerTracker([]uint64{2, 3}, 10)
	if pt.Len() != 2 {
		t.Fatalf("expected 2, got %d", pt.Len())
	}
	p := pt.Get(2)
	if p.NextIndex != 11 {
		t.Fatalf("expected next=11, got %d", p.NextIndex)
	}
}

func TestUpdateMatch(t *testing.T) {
	pt := NewPeerTracker([]uint64{2, 3}, 5)
	pt.UpdateMatch(2, 5)
	p := pt.Get(2)
	if p.MatchIndex != 5 {
		t.Fatalf("expected match=5, got %d", p.MatchIndex)
	}
}

func TestDecrementNext(t *testing.T) {
	pt := NewPeerTracker([]uint64{2}, 10)
	pt.DecrementNext(2)
	p := pt.Get(2)
	if p.NextIndex != 10 {
		t.Fatalf("expected 10, got %d", p.NextIndex)
	}
}

func TestMatchIndexes(t *testing.T) {
	pt := NewPeerTracker([]uint64{2, 3, 4}, 0)
	pt.UpdateMatch(2, 5)
	pt.UpdateMatch(3, 3)
	pt.UpdateMatch(4, 7)
	indexes := pt.MatchIndexes()
	if indexes[0] != 3 || indexes[1] != 5 || indexes[2] != 7 {
		t.Fatalf("unexpected: %v", indexes)
	}
}
