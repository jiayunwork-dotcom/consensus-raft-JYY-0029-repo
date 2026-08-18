package codec

import (
	"bytes"
	"testing"

	"consensus-raft/internal/log"
)

func TestEntryRoundTrip(t *testing.T) {
	e := log.Entry{Term: 3, Index: 7, Command: []byte("hello")}
	var buf bytes.Buffer
	if err := WriteEntry(&buf, e); err != nil {
		t.Fatal(err)
	}
	got, err := ReadEntry(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Term != 3 || got.Index != 7 || string(got.Command) != "hello" {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestEntriesRoundTrip(t *testing.T) {
	entries := []log.Entry{
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 2, Index: 2, Command: []byte("b")},
	}
	var buf bytes.Buffer
	if err := WriteEntries(&buf, entries); err != nil {
		t.Fatal(err)
	}
	got, err := ReadEntries(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d", len(got))
	}
}

func TestEmptyCommand(t *testing.T) {
	e := log.Entry{Term: 1, Index: 1}
	var buf bytes.Buffer
	WriteEntry(&buf, e)
	got, err := ReadEntry(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Command != nil {
		t.Fatal("expected nil command")
	}
}
