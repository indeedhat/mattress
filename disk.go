package mattress

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

type PageGenerator func(yield func(*Page, error) bool)

type StorageManager interface {
	// Close closes down the disk manager freeing up any underlying file handles
	// or connections
	Close() error
	// PageCount gets the total number of pages stored on disk
	PageCount() (int, error)
	// AllocatePage returns the next page id to be used to extend the file
	AllocatePage() PageId
	// WritePage persists a page to its storage medium
	WritePage(page *Page) error
	// ReadPage reads a page from its storage medium
	ReadPage(id PageId) (*Page, error)
	// Iterate is a rangefunc that returns all page entries in the database
	Iterator() PageGenerator
}

type LocalStorageManager struct {
	fh         *os.File
	nextPageId PageId
}

func NewLocalStorageManager(fh *os.File) (StorageManager, error) {
	d := &LocalStorageManager{fh: fh}

	pageCount, err := d.PageCount()
	if err != nil {
		return nil, err
	}

	d.nextPageId = PageId(pageCount)

	return d, nil
}

// Close implements StorageManager.
func (d *LocalStorageManager) Close() error {
	return d.fh.Close()
}

// AllocatePage implements StorageManager.
func (d *LocalStorageManager) AllocatePage() PageId {
	id := d.nextPageId
	d.nextPageId++
	return id
}

// PageCount implements StorageManager.
func (d *LocalStorageManager) PageCount() (int, error) {
	stat, err := d.fh.Stat()
	if err != nil {
		return 0, err
	}

	return int(stat.Size() / pageSize), nil
}

// WritePage implements StorageManager.
func (d *LocalStorageManager) WritePage(page *Page) error {
	// TODO: later I will want to check the write size to make sure it matches
	// the expected value and handle partial writes
	data := page.Encode()
	_, err := d.fh.WriteAt(data[:], int64(page.Header.PageId)*pageSize)
	return err
}

// ReadPage implements StorageManager.
func (d *LocalStorageManager) ReadPage(id PageId) (*Page, error) {
	data := [pageSize]byte{}
	// TODO: handle partial reads
	if _, err := d.fh.ReadAt(data[:], int64(id)*pageSize); err != nil {
		return nil, err
	}

	return decodePage(data)
}

// Iterator implements StorageManager.
func (d *LocalStorageManager) Iterator() PageGenerator {
	return func(yield func(p *Page, err error) bool) {
		for i := range d.nextPageId {
			if !yield(d.ReadPage(i)) {
				return
			}
		}
	}
}

var _ StorageManager = (*LocalStorageManager)(nil)

type InMemoryStorageManager struct {
	nexPageId PageId
}

func NewInMemoryStorageManager() (StorageManager, error) {
	return &InMemoryStorageManager{}, nil
}

// AllocatePage implements StorageManager.
func (i *InMemoryStorageManager) AllocatePage() PageId {
	id := i.nexPageId
	i.nexPageId++
	return id
}

// Close implements StorageManager.
func (i *InMemoryStorageManager) Close() error {
	return nil
}

// Iterator implements StorageManager.
func (i *InMemoryStorageManager) Iterator() PageGenerator {
	return func(yield func(p *Page, err error) bool) {}
}

// PageCount implements StorageManager.
func (i *InMemoryStorageManager) PageCount() (int, error) {
	return int(i.nexPageId), nil
}

// ReadPage implements StorageManager.
func (i *InMemoryStorageManager) ReadPage(id PageId) (*Page, error) {
	return nil, errors.New("ReadPage should never be called when using an in memory store")
}

// WritePage implements StorageManager.
func (i *InMemoryStorageManager) WritePage(page *Page) error {
	return nil
}

var _ StorageManager = (*InMemoryStorageManager)(nil)

// decodePage decodes the raw byte array into a Page object
func decodePage(data [pageSize]byte) (*Page, error) {
	reader := bytes.NewReader(data[:])
	p := &Page{
		Header:       PageHeader{},
		slots:        []Slot{},
		data:         data,
		insertOffset: pageSize,
	}

	// read header
	if err := binary.Read(reader, binary.LittleEndian, &p.Header.PageId); err != nil {
		return nil, fmt.Errorf("failed to read page id: %w", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &p.Header.Flags); err != nil {
		return nil, fmt.Errorf("failed to read flags: %w", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &p.Header.SlotCount); err != nil {
		return nil, fmt.Errorf("failed to read flags: %w", err)
	}

	// read slots
	for range int(p.Header.SlotCount) {
		slot := Slot{}

		if err := binary.Read(reader, binary.LittleEndian, &slot.offset); err != nil {
			return nil, fmt.Errorf("failed to read slot offset: %w", err)
		}
		if err := binary.Read(reader, binary.LittleEndian, &slot.cap); err != nil {
			return nil, fmt.Errorf("failed to read slot cap: %w", err)
		}
		if err := binary.Read(reader, binary.LittleEndian, &slot.len); err != nil {
			return nil, fmt.Errorf("failed to read slot len: %w", err)
		}
		alive, err := reader.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("failed to read slot alive status: %w", err)
		}

		slot.alive = alive == 1
		p.slots = append(p.slots, slot)
		if slot.offset < uint16(p.insertOffset) {
			p.insertOffset = int(slot.offset)
		}
	}

	return p, nil
}
