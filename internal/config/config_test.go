package config

import "testing"

func TestDefaultValid(t *testing.T) {
	c := Default()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateRejects(t *testing.T) {
	c := Default()
	c.HeartbeatInterval = 100 // >= election timeout
	if err := c.Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestSaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	c := Default()
	c.HeartbeatInterval = 2
	if err := c.Save(dir); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.HeartbeatInterval != 2 {
		t.Fatalf("expected 2, got %d", loaded.HeartbeatInterval)
	}
}
