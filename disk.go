package mattress

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

type DiskManager struct {
	fh         *os.File
	nextPageId PageId
}

func NewDiskManager(fh *os.File) (*DiskManager, error) {
	d := &DiskManager{fh: fh}

	pageCount, err := d.PageCount()
	if err != nil {
		return nil, err
	}

	d.nextPageId = PageId(pageCount)

	return d, nil
}

// Close closes down the underlying file handle
func (d *DiskManager) Close() error {
	return d.fh.Close()
}

// AllocatePage returns the next page id to be used to extend the file
func (d *DiskManager) AllocatePage() PageId {
	id := d.nextPageId
	d.nextPageId++
	return id
}

// PageCount gets the total number of pages stored on disk
func (d *DiskManager) PageCount() (int, error) {
	stat, err := d.fh.Stat()
	if err != nil {
		return 0, err
	}

	return int(stat.Size() / pageSize), nil
}

// WritePage writes a page to disk
func (d *DiskManager) WritePage(page *Page) error {
	// TODO: later I will want to check the write size to make sure it matches
	// the expected value and handle partial writes
	data := page.Encode()
	_, err := d.fh.WriteAt(data[:], int64(page.Header.PageId)*pageSize)
	return err
}

// ReadPage reads a page from disk
func (d *DiskManager) ReadPage(id PageId) (*Page, error) {
	data := [pageSize]byte{}
	// TODO: handle partial reads
	if _, err := d.fh.ReadAt(data[:], int64(id)*pageSize); err != nil {
		return nil, err
	}

	return decodePage(data)
}

// Iterate is a rangefunc that returns all page entries in the database
func (d *DiskManager) Iterator() func(yield func(*Page, error) bool) {
	return func(yield func(p *Page, err error) bool) {
		for i := range d.nextPageId {
			if !yield(d.ReadPage(i)) {
				return
			}
		}
	}
}

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
