package mattress

import (
	"errors"
	"fmt"
	"math"
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

const (
	keyMaxSize = math.MaxUint8
)

type indexEntry struct {
	page uint64
	slot uint16
}

// DB implements a bare bones slotted page backed key value store
//
// # Entry Structure
// | keySize (uint8) | valueSize (uint32) | key ([]byte) | value ([]byte) |
//
// the max value size as defined by the page system currently would fit into a
// uint16 but i have it set to uint32 as i plan on later allowing records to span
// multiple pages with a max size of 1MB
type DB struct {
	fh    *os.File
	pages *PageManager
	index map[string]indexEntry
	mux   sync.RWMutex
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

	d.index = make(map[string]indexEntry)

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

	d.pages = NewPageManager(d.fh)
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
	d.pages = nil

	return nil
}

// Put creates or updates an entry by its key
func (d *DB) Put(key, value string) error {
	d.mux.Lock()
	defer d.mux.Unlock()

	var page *Page
	var err error
	var slotId int
	rec := encodeRecord(key, value)

	if loc, found := d.index[key]; found {
		if page, err = d.pages.Fetch(loc.page); err != nil {
			return fmt.Errorf("failed to fetch page containing existing record: %w", err)
		}

		if slotId, err = page.Update(int(loc.slot), rec); err != nil {
			return fmt.Errorf("failed to store record: %w", err)
		}
	} else {
		if page, err = d.pages.FetchWithSpace(len(rec)); err != nil {
			return fmt.Errorf("failed to fetch page to save record: %w", err)
		}

		if slotId, err = page.Insert(rec); err != nil {
			return fmt.Errorf("failed to store record: %w", err)
		}
	}

	if err := d.pages.Store(page); err != nil {
		return fmt.Errorf("failed to store record: %w", err)
	}

	d.index[key] = indexEntry{page.Header.PageId, uint16(slotId)}

	return nil
}

// Get retrievs an entry from the database by its key
func (d *DB) Get(key string) (string, error) {
	d.mux.RLock()
	defer d.mux.RUnlock()

	entry, found := d.index[key]
	if !found {
		return "", fmt.Errorf("key %s does not exists in index", key)
	}

	page, err := d.pages.Fetch(entry.page)
	if err != nil {
		return "", fmt.Errorf("entry %s not found", key)
	}

	rec, err := page.Read(int(entry.slot))
	if err != nil {
		return "", fmt.Errorf("entry %s not found", key)
	}

	return rec.Value(), nil
}

// Delete removes an entry from the database
func (d *DB) Delete(key string) error {
	d.mux.Lock()
	defer d.mux.Unlock()

	entry, found := d.index[key]
	if !found {
		return fmt.Errorf("key %s does not exists in index", key)
	}

	page, err := d.pages.Fetch(entry.page)
	if err != nil {
		return fmt.Errorf("entry %s not found", key)
	}

	if err := page.Delete(int(entry.slot)); err != nil {
		return fmt.Errorf("failed to delete entry: %w", err)
	}

	delete(d.index, key)

	return nil
}

func (d *DB) Compact() error {
	panic("not implemented")
}

// writeToLog is the shared functionaly for presisting entries to the disk

func (d *DB) rebuildIndex() (map[string]indexEntry, error) {
	index := make(map[string]indexEntry)

	for page, err := range d.pages.Iterator() {
		if err != nil {
			return nil, err
		}

		for slotId := 0; slotId < int(page.Header.SlotCount); slotId++ {
			record, err := page.Read(slotId)
			if err != nil {
				return nil, err
			}

			index[record.Value()] = indexEntry{
				page.Header.PageId,
				uint16(slotId),
			}
		}
	}

	return index, nil
}
