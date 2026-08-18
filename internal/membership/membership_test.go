package membership

import "testing"

func TestQuorum(t *testing.T) {
	c := NewConfig([]uint64{1, 2, 3})
	if c.Quorum() != 2 {
		t.Fatalf("expected 2, got %d", c.Quorum())
	}
	c5 := NewConfig([]uint64{1, 2, 3, 4, 5})
	if c5.Quorum() != 3 {
		t.Fatalf("expected 3, got %d", c5.Quorum())
	}
}

func TestContains(t *testing.T) {
	c := NewConfig([]uint64{1, 2, 3})
	if !c.Contains(2) {
		t.Fatal("expected contains 2")
	}
	if c.Contains(99) {
		t.Fatal("should not contain 99")
	}
}

func TestAddPeer(t *testing.T) {
	c := NewConfig([]uint64{1, 2, 3})
	c2 := c.AddPeer(4)
	if c2.Size() != 4 {
		t.Fatalf("expected 4, got %d", c2.Size())
	}
}

func TestRemovePeer(t *testing.T) {
	c := NewConfig([]uint64{1, 2, 3})
	c2 := c.RemovePeer(2)
	if c2.Size() != 2 {
		t.Fatalf("expected 2, got %d", c2.Size())
	}
	if c2.Contains(2) {
		t.Fatal("should not contain 2 after remove")
	}
}

func TestOtherPeers(t *testing.T) {
	c := NewConfig([]uint64{1, 2, 3})
	others := c.OtherPeers(1)
	if len(others) != 2 {
		t.Fatalf("expected 2, got %d", len(others))
	}
}

func TestEqual(t *testing.T) {
	a := NewConfig([]uint64{1, 2, 3})
	b := NewConfig([]uint64{3, 1, 2})
	if !a.Equal(b) {
		t.Fatal("should be equal")
	}
}

func TestIsMajority(t *testing.T) {
	c := NewConfig([]uint64{1, 2, 3})
	if !c.IsMajority(2) {
		t.Fatal("2/3 is majority")
	}
	if c.IsMajority(1) {
		t.Fatal("1/3 is not majority")
	}
}
