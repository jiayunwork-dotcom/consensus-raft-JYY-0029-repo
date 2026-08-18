package statemachine

import (
	"testing"
)

func TestSetAndGet(t *testing.T) {
	kv := NewKV()
	if err := kv.Apply(1, []byte("set name alice")); err != nil {
		t.Fatal(err)
	}
	v, ok := kv.Get("name")
	if !ok || v != "alice" {
		t.Fatalf("got %q %v", v, ok)
	}
}

func TestDelete(t *testing.T) {
	kv := NewKV()
	kv.Apply(1, []byte("set k v"))
	kv.Apply(2, []byte("delete k"))
	_, ok := kv.Get("k")
	if ok {
		t.Fatal("expected deleted")
	}
}

func TestBadCommand(t *testing.T) {
	kv := NewKV()
	if err := kv.Apply(1, []byte("invalid")); err != ErrBadCommand {
		t.Fatalf("expected ErrBadCommand, got %v", err)
	}
}

func TestSnapshotRestore(t *testing.T) {
	kv := NewKV()
	kv.Apply(1, []byte("set a 1"))
	kv.Apply(2, []byte("set b 2"))

	data, err := kv.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	kv2 := NewKV()
	if err := kv2.Restore(data); err != nil {
		t.Fatal(err)
	}
	if kv2.Len() != 2 {
		t.Fatalf("expected 2, got %d", kv2.Len())
	}
	v, _ := kv2.Get("a")
	if v != "1" {
		t.Fatal("bad value after restore")
	}
}

func TestLastApplied(t *testing.T) {
	kv := NewKV()
	kv.Apply(5, []byte("set x y"))
	if kv.LastApplied() != 5 {
		t.Fatalf("expected 5, got %d", kv.LastApplied())
	}
}

func TestKeys(t *testing.T) {
	kv := NewKV()
	kv.Apply(1, []byte("set b 2"))
	kv.Apply(2, []byte("set a 1"))
	keys := kv.Keys()
	if len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
		t.Fatalf("unexpected keys: %v", keys)
	}
}
