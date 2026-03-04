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
	keyMaxSize = math.MaxUint8

	inMemoryFilePath = ":memory:"
)

type indexEntry struct {
	page PageId
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
	fsm  *Fsm
	pool *BufferPool
	disk StorageManager

	index map[string]indexEntry

	mux sync.RWMutex
	wg  sync.WaitGroup
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
	d.mux.Lock()
	defer d.mux.Unlock()

	var err error

	if d.pool != nil {
		return errors.New("database already open")
	}

	d.fsm = NewFsm()
	d.index = make(map[string]indexEntry)
	poolSize := defaultBufferPoolSize

	if path == inMemoryFilePath {
		d.disk, err = NewInMemoryStorageManager()
		poolSize = unlimitedBufferPoolSize
	} else {
		fh, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		d.disk, err = NewLocalStorageManager(fh)
	}
	if err != nil {
		return err
	}

	d.pool = NewBufferPool(d.disk, poolSize)

	d.index, err = d.rebuildIndex()
	if err != nil {
		return fmt.Errorf("index rebuild failed: %w", err)
	}

	return nil
}

// Close closes the underlying file handler for the write log
func (d *DB) Close() error {
	d.mux.Lock()
	defer d.mux.Unlock()

	d.wg.Wait()

	err := d.pool.Close()
	if err != nil {
		return err
	}

	d.index = nil
	d.pool = nil
	d.fsm = nil

	return nil
}

// Put creates or updates an entry by its key
func (d *DB) Put(key, value string) error {
	d.mux.Lock()
	defer d.mux.Unlock()
	d.wg.Add(1)
	defer d.wg.Done()

	if len([]byte(key)) > keyMaxSize {
		return errors.New("key is too large")
	}

	var page *Page
	var err error
	var slotId int
	rec := encodeRecord(key, value)

	// TODO: this really need some kind of transaction so we don't lose data when moving to a new page
	if loc, found := d.index[key]; found {
		if page, err = d.pool.Fetch(loc.page); err != nil {
			return fmt.Errorf("failed to fetch page containing existing record: %w", err)
		}

		if slotId, err = page.Update(int(loc.slot), rec); errors.Is(err, ErrPageFull) {
			if err := page.Delete(int(loc.slot)); err != nil {
				return fmt.Errorf("could not relocate record to new page: %s", err)
			}

			delete(d.index, key)
		} else if err != nil {
			return fmt.Errorf("failed to store record: %w", err)
		}
	}

	pageId, found := d.fsm.FindSpace(len(rec))
	if !found {
		page, err = d.pool.Create()
	} else {
		page, err = d.pool.Fetch(pageId)
	}

	if err != nil {
		return fmt.Errorf("failed to fetch page to save record: %w", err)
	}

	var dirty bool
	defer func() {
		d.pool.Release(page, dirty)
	}()

	if slotId, err = page.Insert(rec); err != nil {
		return fmt.Errorf("failed to store record: %w", err)
	}

	dirty = true
	d.index[key] = indexEntry{page.Header.PageId, uint16(slotId)}

	return nil
}

// Get retrievs an entry from the database by its key
func (d *DB) Get(key string) (string, error) {
	d.mux.RLock()
	defer d.mux.RUnlock()
	d.wg.Add(1)
	defer d.wg.Done()

	entry, found := d.index[key]
	if !found {
		return "", fmt.Errorf("entry %s not found", key)
	}

	page, err := d.pool.Fetch(entry.page)
	if err != nil {
		return "", fmt.Errorf("entry %s not found", key)
	}
	defer d.pool.Release(page, false)

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
	d.wg.Add(1)
	defer d.wg.Done()

	entry, found := d.index[key]
	if !found {
		return fmt.Errorf("entry %s not found", key)
	}

	page, err := d.pool.Fetch(entry.page)
	if err != nil {
		return fmt.Errorf("entry %s not found", key)
	}
	var dirty bool
	defer func() {
		d.pool.Release(page, dirty)
	}()

	if err := page.Delete(int(entry.slot)); err != nil {
		return fmt.Errorf("failed to delete entry: %w", err)
	}

	dirty = true
	delete(d.index, key)

	return nil
}

func (d *DB) Compact() error {
	panic("not implemented")
}

func (d *DB) rebuildIndex() (map[string]indexEntry, error) {
	index := make(map[string]indexEntry)

	for page, err := range d.disk.Iterator() {
		if err != nil {
			return nil, err
		}

		d.fsm.Set(page.Header.PageId, uint16(page.FreeSpace()))

		for slotId := 0; slotId < int(page.Header.SlotCount); slotId++ {
			record, err := page.Read(slotId)
			if err != nil {
				if errors.Is(err, ErrInvalidSlot) {
					continue
				}
				return nil, err
			}

			index[record.Key()] = indexEntry{
				page.Header.PageId,
				uint16(slotId),
			}
		}
	}

	return index, nil
}
