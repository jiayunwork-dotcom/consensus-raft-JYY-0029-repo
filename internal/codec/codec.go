// Package codec provides binary serialization for Raft messages and log entries.
package codec

import (
	"encoding/binary"
	"errors"
	"io"

	"consensus-raft/internal/log"
)

// ErrFormat is returned when data cannot be decoded.
var ErrFormat = errors.New("codec: format error")

// WriteEntry serializes a log entry to a writer.
func WriteEntry(w io.Writer, e log.Entry) error {
	if err := writeU64(w, e.Term); err != nil {
		return err
	}
	if err := writeU64(w, e.Index); err != nil {
		return err
	}
	if err := writeU32(w, uint32(len(e.Command))); err != nil {
		return err
	}
	if len(e.Command) > 0 {
		_, err := w.Write(e.Command)
		return err
	}
	return nil
}

// ReadEntry deserializes a log entry from a reader.
func ReadEntry(r io.Reader) (log.Entry, error) {
	term, err := readU64(r)
	if err != nil {
		return log.Entry{}, err
	}
	index, err := readU64(r)
	if err != nil {
		return log.Entry{}, err
	}
	cmdLen, err := readU32(r)
	if err != nil {
		return log.Entry{}, err
	}
	if cmdLen > 1<<24 {
		return log.Entry{}, ErrFormat
	}
	var cmd []byte
	if cmdLen > 0 {
		cmd = make([]byte, cmdLen)
		if _, err := io.ReadFull(r, cmd); err != nil {
			return log.Entry{}, err
		}
	}
	return log.Entry{Term: term, Index: index, Command: cmd}, nil
}

// WriteEntries writes a count-prefixed sequence of entries.
func WriteEntries(w io.Writer, entries []log.Entry) error {
	if err := writeU32(w, uint32(len(entries))); err != nil {
		return err
	}
	for _, e := range entries {
		if err := WriteEntry(w, e); err != nil {
			return err
		}
	}
	return nil
}

// ReadEntries reads a count-prefixed sequence of entries.
func ReadEntries(r io.Reader) ([]log.Entry, error) {
	count, err := readU32(r)
	if err != nil {
		return nil, err
	}
	if count > 1<<20 {
		return nil, ErrFormat
	}
	entries := make([]log.Entry, count)
	for i := range entries {
		e, err := ReadEntry(r)
		if err != nil {
			return nil, err
		}
		entries[i] = e
	}
	return entries, nil
}

func writeU64(w io.Writer, v uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	_, err := w.Write(buf[:])
	return err
}

func readU64(r io.Reader) (uint64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}

func writeU32(w io.Writer, v uint32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	_, err := w.Write(buf[:])
	return err
}

func readU32(r io.Reader) (uint32, error) {
	var buf [4]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(buf[:]), nil
}
