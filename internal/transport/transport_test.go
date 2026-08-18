package transport

import (
	"testing"

	"consensus-raft/internal/node"
)

func TestSendAndReceive(t *testing.T) {
	tr := NewInMemory([]uint64{1, 2, 3})
	msg := node.Message{From: 1, To: 2}
	if err := tr.Send(msg); err != nil {
		t.Fatal(err)
	}
	msgs, err := tr.Receive(2)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
}

func TestReceiveEmpty(t *testing.T) {
	tr := NewInMemory([]uint64{1, 2})
	msgs, _ := tr.Receive(1)
	if len(msgs) != 0 {
		t.Fatal("expected empty")
	}
}

func TestSendToUnknown(t *testing.T) {
	tr := NewInMemory([]uint64{1})
	err := tr.Send(node.Message{To: 99})
	if err != ErrUnknownPeer {
		t.Fatalf("expected ErrUnknownPeer, got %v", err)
	}
}

func TestClose(t *testing.T) {
	tr := NewInMemory([]uint64{1})
	tr.Close()
	err := tr.Send(node.Message{To: 1})
	if err != ErrClosed {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestDrop(t *testing.T) {
	tr := NewInMemory([]uint64{1, 2})
	tr.Send(node.Message{To: 2})
	tr.Send(node.Message{To: 2})
	tr.Drop(2)
	if tr.PendingCount(2) != 0 {
		t.Fatal("expected 0 after drop")
	}
}
