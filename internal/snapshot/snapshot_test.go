package snapshot

import (
	"path/filepath"
	"testing"
)

func TestSaveAndLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snap.bin")
	snap := &Snapshot{
		Meta: Metadata{LastIndex: 100, LastTerm: 5, Peers: []uint64{1, 2, 3}},
		Data: []byte("state machine data"),
	}
	if err := Save(path, snap); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Meta.LastIndex != 100 || loaded.Meta.LastTerm != 5 {
		t.Fatalf("bad meta: %+v", loaded.Meta)
	}
	if len(loaded.Meta.Peers) != 3 {
		t.Fatal("bad peers")
	}
	if string(loaded.Data) != "state machine data" {
		t.Fatal("bad data")
	}
}

func TestLoadMissing(t *testing.T) {
	snap, err := Load(filepath.Join(t.TempDir(), "no.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if snap != nil {
		t.Fatal("expected nil for missing file")
	}
}
