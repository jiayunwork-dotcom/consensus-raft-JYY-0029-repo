package persist

import (
	"path/filepath"
	"testing"

	"consensus-raft/internal/log"
)

func TestSaveAndLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.bin")
	s := &State{
		CurrentTerm: 5,
		VotedFor:    2,
		Entries: []log.Entry{
			{Term: 1, Index: 1, Command: []byte("cmd1")},
			{Term: 2, Index: 2, Command: []byte("cmd2")},
		},
	}
	if err := Save(path, s); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.CurrentTerm != 5 || loaded.VotedFor != 2 {
		t.Fatalf("unexpected: %+v", loaded)
	}
	if len(loaded.Entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(loaded.Entries))
	}
	if string(loaded.Entries[0].Command) != "cmd1" {
		t.Fatal("bad command")
	}
}

func TestLoadMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.bin")
	s, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if s.CurrentTerm != 0 {
		t.Fatal("expected zero state")
	}
}

func TestExists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.bin")
	if Exists(path) {
		t.Fatal("should not exist")
	}
	Save(path, &State{})
	if !Exists(path) {
		t.Fatal("should exist")
	}
}
