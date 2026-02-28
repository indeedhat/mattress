package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

type EntryType byte

const (
	Put EntryType = iota
	Delete
)

const (
	compactDbName = "./tmp.mtrsgb"
	backupDbName  = "./tmp.mtrsbk"
)

// DB implements a bare bones append only file backed key value store with an in memory index
//
// # Entry Structure
// | type (byte) | keySize (int32) | valueSize (int32) | key ([]byte) | value ([]byte) |
type DB struct {
	fh    *os.File
	index map[string]int64
	mux   sync.Mutex
}

func NewDB() *DB {
	return &DB{}
}

// Len returns the number of entries in the index
func (d *DB) Len() int {
	return len(d.index)
}

// Open opens the database from a file and rebuilds the in memory index
func (d *DB) Open(path string) error {
	var err error

	if d.fh != nil {
		return errors.New("database already open")
	}

	d.mux.Lock()
	defer d.mux.Unlock()

	d.index = make(map[string]int64)

	if d.fh, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644); err != nil {
		return err
	}

	stat, err := d.fh.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	if stat.Size() == 0 {
		return nil
	}

	d.index, err = d.rebuildIndex()

	return err
}

// Close closes the underlying file handler for the write log
func (d *DB) Close() error {
	d.mux.Lock()
	defer d.mux.Unlock()

	err := d.fh.Close()
	if err != nil {
		return err
	}

	d.fh = nil
	return nil
}

// Put creates a new entry in the database
// if the entry already exists then an error will be returned
func (d *DB) Put(key, value string) error {
	if _, found := d.index[key]; found {
		return fmt.Errorf("key %s already exists in index", key)
	}

	d.mux.Lock()
	defer d.mux.Unlock()

	stat, err := d.fh.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	}

	if _, err := d.writeToLog(d.fh, Put, key, value); err != nil {
		return err
	}

	d.index[key] = stat.Size()

	return nil
}

// Get retrievs an entry from the database by its key
func (d *DB) Get(key string) (string, error) {
	d.mux.Lock()
	defer d.mux.Unlock()

	offset, found := d.index[key]
	if !found {
		return "", fmt.Errorf("key %s not found", key)
	}

	return d.getByOffset(offset, d.fh)
}

func (d *DB) getByOffset(offset int64, fh *os.File) (string, error) {

	lenSection := io.NewSectionReader(d.fh, offset, 9)
	lenReader := bufio.NewReader(lenSection)

	var keyLen int32
	var valLen int32

	// the first byte is the log entry type and we don't need it for the get operation so it gets
	// discarded
	lenReader.Discard(1)

	if err := binary.Read(lenReader, binary.LittleEndian, &keyLen); err != nil {
		return "", fmt.Errorf("failed to read entry: %s", err)
	}

	if err := binary.Read(lenReader, binary.LittleEndian, &valLen); err != nil {
		return "", fmt.Errorf("failed to read entry: %s", err)
	}

	data := make([]byte, valLen)
	dataSection := io.NewSectionReader(d.fh, offset+9+int64(keyLen), int64(valLen))
	if err := binary.Read(bufio.NewReader(dataSection), binary.LittleEndian, &data); err != nil {
		return "", fmt.Errorf("failed to read entry: %s", err)
	}

	return string(data), nil
}

// Delete removes an entry from the database
func (d *DB) Delete(key string) error {
	if _, found := d.index[key]; !found {
		return fmt.Errorf("key %s does not exists in index", key)
	}

	d.mux.Lock()
	defer d.mux.Unlock()

	if _, err := d.writeToLog(d.fh, Delete, key, ""); err != nil {
		return err
	}

	delete(d.index, key)

	return nil
}

// Compact rebuilds the write log to remove deleted entries
//
// it is by no means an efficient implementation
func (d *DB) Compact() error {
	gcFh, err := os.OpenFile(compactDbName, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	index, err := d.rebuildIndex()
	if err != nil {
		return err
	}

	for key, offset := range index {
		val, err := d.getByOffset(offset, d.fh)
		if err != nil {
			return err
		}

		if _, err = d.writeToLog(gcFh, Put, key, val); err != nil {
			return err
		}
	}

	dbName := d.fh.Name()

	if err := gcFh.Close(); err != nil {
		return fmt.Errorf("failed to close compacted file handle: %w", err)
	}

	if err := d.Close(); err != nil {
		return fmt.Errorf("failed to close current file handle: %w", err)
	}

	if err := os.Rename(dbName, backupDbName); err != nil {
		return fmt.Errorf("failed to rename existing db: %w", err)
	}

	if err := os.Rename(compactDbName, dbName); err != nil {
		_ = os.Rename(backupDbName, dbName)
		return fmt.Errorf("failed to replace db: %w", err)
	}

	if err := os.Remove(backupDbName); err != nil {
		return fmt.Errorf("failed to clean up old database: %w", err)
	}

	return d.Open(dbName)
}

// writeToLog is the shared functionaly for presisting entries to the disk
func (d *DB) writeToLog(fh *os.File, entryType EntryType, key, value string) (int, error) {
	keySize := int32(len([]byte(key)))
	valSize := int32(len([]byte(value)))

	buf := make([]byte, 0, keySize+valSize+9)
	var err error

	buf, err = binary.Append(buf, binary.LittleEndian, entryType)
	if err != nil {
		return 0, fmt.Errorf("failed to pack data: %w", err)
	}
	buf, err = binary.Append(buf, binary.LittleEndian, keySize)
	if err != nil {
		return 0, fmt.Errorf("failed to pack data: %w", err)
	}
	buf, err = binary.Append(buf, binary.LittleEndian, valSize)
	if err != nil {
		return 0, fmt.Errorf("failed to pack data: %w", err)
	}
	buf, err = binary.Append(buf, binary.LittleEndian, []byte(key))
	if err != nil {
		return 0, fmt.Errorf("failed to pack data: %w", err)
	}
	buf, err = binary.Append(buf, binary.LittleEndian, []byte(value))
	if err != nil {
		return 0, fmt.Errorf("failed to pack data: %w", err)
	}

	if _, err := fh.Seek(0, io.SeekEnd); err != nil {
		return 0, fmt.Errorf("failed to seek to end of file: %w", err)
	}

	n, err := fh.Write(buf)
	if err != nil {
		return 0, fmt.Errorf("failed to write to file: %w", err)
	}

	return n, nil
}

func (d *DB) rebuildIndex() (map[string]int64, error) {
	if _, err := d.fh.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to start: %w", err)
	}

	index := make(map[string]int64)
	var offset int64
	reader := bufio.NewReader(d.fh)
	for {
		var keyLen int32
		var valLen int32
		var entryType EntryType

		if err := binary.Read(reader, binary.LittleEndian, &entryType); err != nil {
			// we are only checking for eof on reading the entryType and not other reads as it is an
			// expected possibility at this point, if we get EOF at reading any other value then
			// the database is corrupt
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to rebuild index: %s", err)
		}

		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			return nil, fmt.Errorf("failed to rebuild index: %s", err)
		}

		if err := binary.Read(reader, binary.LittleEndian, &valLen); err != nil {
			return nil, fmt.Errorf("failed to rebuild index: %s", err)
		}

		key := make([]byte, keyLen)
		if err := binary.Read(reader, binary.LittleEndian, key); err != nil {
			return nil, fmt.Errorf("failed to rebuild index: %s", err)
		}

		switch entryType {
		case Put:
			index[string(key)] = offset
			reader.Discard(int(valLen))
		case Delete:
			delete(index, string(key))
		default:
			return nil, fmt.Errorf("found invalid entry type %d", entryType)
		}

		offset += 9 + int64(keyLen) + int64(valLen)
	}

	return index, nil
}
