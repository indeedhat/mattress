package mattress

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	headerSize = 12
	pageSize   = 4096
	slotSize   = 7
)

var (
	ErrPageFull        = errors.New("page cannot fit record")
	ErrInvalidSlot     = errors.New("invalid page slot")
	ErrInvalidPageSize = errors.New("invalid page size")
)

type Record []byte

// Key returns the string key for the record
func (r Record) Key() string {
	klen := uint8(r[0])
	return string(r[5 : 5+klen])
}

// Value returns the string value for the record
func (r Record) Value() string {
	klen := uint8(r[0])
	vlen := binary.LittleEndian.Uint32(r[1:5])
	return string(r[5+klen : 5+uint32(klen)+vlen])
}

type Slot struct {
	offset uint16
	cap    uint16
	len    uint16
	alive  bool
}

func (s Slot) IsAlive() bool {
	return s.alive
}

type PageHeader struct {
	PageId    uint64
	Flags     uint16
	SlotCount uint16
}

type Page struct {
	Header       PageHeader
	slots        []Slot
	data         [pageSize]byte
	insertOffset int
	mux          sync.RWMutex
}

// NewPage creates a blank page instance with the provided id/flags
func NewPage(id uint64, flags uint16) *Page {
	return &Page{
		Header: PageHeader{
			PageId: id,
			Flags:  flags,
		},
		data:         [pageSize]byte{},
		insertOffset: pageSize,
	}
}

// Insert will attempt to insert a record into a page slot
//
// should it be successful the slot index will be returned
func (p *Page) Insert(record Record) (int, error) {
	p.mux.Lock()
	defer p.mux.Unlock()
	return p.doInsert(record)
}

// doInsert does the actual work of inserting a new record in the page,
// the reason for its existence is because it is shared functionality
// between both the Insert and Update method, this allows update to call it without
// hitting a locked mutex
func (p *Page) doInsert(record Record) (int, error) {
	recLen := len(record)

	if !p.hasFreeSpace(recLen) {
		return -1, ErrPageFull
	}

	p.insertOffset -= len(record)
	copy(p.data[p.insertOffset:p.insertOffset+len(record)], record)

	p.slots = append(p.slots, Slot{
		offset: uint16(p.insertOffset),
		len:    uint16(recLen),
		cap:    uint16(recLen),
		alive:  true,
	})
	p.Header.SlotCount = uint16(len(p.slots))

	return len(p.slots) - 1, nil
}

// Read reads a single record from the page by its slot id
func (p *Page) Read(slotId int) (Record, error) {
	p.mux.RLock()
	defer p.mux.RUnlock()

	if slotId >= len(p.slots) || !p.slots[slotId].alive {
		return nil, ErrInvalidSlot
	}

	slot := p.slots[slotId]
	return p.data[slot.offset : slot.offset+slot.len], nil
}

// Update will attempt to update a record by its slot id
//
// Should this operation succeed a new slot id will be returned that may or may
// not match the existing one
func (p *Page) Update(slotId int, record Record) (int, error) {
	p.mux.Lock()
	defer p.mux.Unlock()

	if slotId >= len(p.slots) || !p.slots[slotId].alive {
		return -1, ErrInvalidSlot
	}

	slot := &p.slots[slotId]
	recLen := uint16(len(record))

	if recLen <= slot.cap {
		copy(p.data[slot.offset:slot.offset+recLen], record)
		slot.len = recLen
		return slotId, nil
	}

	// if the slot is the last in the list then it can be extended without having to compact other
	// records
	if slotId == len(p.slots)-1 && p.hasFreeSpace(int(recLen-slot.cap)) {
		p.insertOffset -= int(recLen - slot.cap)
		copy(p.data[p.insertOffset:p.insertOffset+int(recLen)], record)
		slot.len = recLen
		slot.cap = recLen
		slot.offset = uint16(p.insertOffset)
		return slotId, nil
	}

	newSlotId, err := p.doInsert(record)
	if err == nil {
		p.slots[slotId].alive = false
	}

	return newSlotId, err
}

// Delete attempts to remove a record from the page
func (p *Page) Delete(slotId int) error {
	p.mux.Lock()
	defer p.mux.Unlock()

	if slotId >= len(p.slots) || !p.slots[slotId].alive {
		return ErrInvalidSlot
	}

	p.slots[slotId].alive = false
	return nil
}

func (p *Page) hasFreeSpace(n int) bool {
	return p.insertOffset >= n+headerSize+slotSize*len(p.slots)
}

// FreeSpace will return the space in bytes left in the page less the length
// of a single slot entry, this allows it to be used by the database manager to
// calculate if a record will fit in a page without having to know about slots
func (p *Page) FreeSpace() int {
	return p.insertOffset - headerSize - slotSize*len(p.slots)
}

// Encode will return a byte array representing the page that can be persisted
// to disk
func (p *Page) Encode() [pageSize]byte {
	p.mux.RLock()
	defer p.mux.RLock()

	// write header
	binary.LittleEndian.PutUint64(p.data[:8], p.Header.PageId)
	binary.LittleEndian.PutUint16(p.data[8:10], uint16(p.Header.Flags))
	binary.LittleEndian.PutUint16(p.data[10:12], uint16(len(p.slots)))

	// write slots
	for i, slot := range p.slots {
		offset := headerSize + i*slotSize
		binary.LittleEndian.PutUint16(p.data[offset:offset+2], slot.offset)
		binary.LittleEndian.PutUint16(p.data[offset+2:offset+4], slot.cap)
		binary.LittleEndian.PutUint16(p.data[offset+4:offset+6], slot.len)
		if slot.alive {
			p.data[offset+6] = 1
		} else {
			p.data[offset+6] = 0
		}
	}

	return p.data
}

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
	for i := 0; i < int(p.Header.SlotCount); i++ {
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
		p.Header.SlotCount = uint16(len(p.slots))
		if slot.offset < uint16(p.insertOffset) {
			p.insertOffset = int(slot.offset)
		}
	}

	return p, nil
}

func encodeRecord(key, value string) []byte {
	keySize := uint8(len([]byte(key)))
	valSize := uint32(len([]byte(value)))

	buf := make([]byte, 5, uint32(keySize)+valSize+4)

	buf[0] = keySize
	binary.LittleEndian.PutUint32(buf[1:5], valSize)

	buf = append(buf, []byte(key)...)
	buf = append(buf, []byte(value)...)

	return buf
}

type PageManager struct {
	fh *os.File
	// freeList keeps track of the free space in each page so we have an in memory way
	// for finding a page to insert the data to
	freeList map[uint64]int
	mux      sync.RWMutex
	// TODO: page caching
}

func NewPageManager(fh *os.File) (*PageManager, error) {
	stat, err := fh.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	return &PageManager{
		fh:       fh,
		freeList: make(map[uint64]int),
	}, nil
}

// Fetch will attempt to fetch a page by its id
func (m *PageManager) Fetch(id uint64) (*Page, error) {
	m.mux.RLock()
	defer m.mux.RUnlock()

	return m.doFetchFromFile(id)
}

// doFetchFromFile fetches a page entry from file by its id
func (m *PageManager) doFetchFromFile(id uint64) (*Page, error) {
	stat, err := m.fh.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	offset := id * pageSize
	if int64(offset+pageSize) < stat.Size() {
		return nil, fmt.Errorf("page '%s' lies outside of the bounds of the file", id)
	}

	if _, err := m.fh.Seek(int64(offset), io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to page offset: %w", err)
	}

	buf := [pageSize]byte{}
	if readBytes, err := m.fh.Read(buf[:]); err != nil {
		return nil, fmt.Errorf("failed to read page from file: %w", err)
	} else if readBytes != pageSize {
		return nil, errors.New("failed to read whole page from file")
	}

	page, err := decodePage(buf)
	if err != nil {
		return nil, err
	}

	if _, found := m.freeList[page.Header.PageId]; !found {
		m.freeList[page.Header.PageId] = page.FreeSpace()
	}

	return page, nil
}

// Store will attempt to write a page to disk
func (m *PageManager) Store(page *Page) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	return m.doStore(page)
}

// doStore stores a page to file
func (m *PageManager) doStore(page *Page) error {
	offset := page.Header.PageId * pageSize
	data := page.Encode()

	if _, err := m.fh.WriteAt(data[:], int64(offset)); err != nil {
		return fmt.Errorf("failed to write page to file: %w", err)
	}

	m.freeList[page.Header.PageId] = page.FreeSpace()

	return nil
}

// create creates a new page entry and pre emptively stores it to file
func (m *PageManager) create(id uint64) (*Page, error) {
	m.mux.Lock()
	defer m.mux.Unlock()

	page := NewPage(id, 0x0)
	if err := m.doStore(page); err != nil {
		return nil, err
	}

	return page, nil
}

// FetchWithSpace attempts to fetch the first available page with enough space
// to store the provided size
func (m *PageManager) FetchWithSpace(n int) (*Page, error) {
	m.mux.Lock()
	defer m.mux.Unlock()

	for id, size := range m.freeList {
		if size >= n {
			return m.doFetchFromFile(id)
		}
	}

	nextId := len(m.freeList)

	return m.create(uint64(nextId))
}

// Iterate is a rangefunc that returns all page entries in the database
func (m *PageManager) Iterator() func(yield func(*Page, error) bool) {
	return func(yield func(p *Page, err error) bool) {
		m.mux.Lock()
		defer m.mux.Unlock()

		stat, err := m.fh.Stat()
		if err != nil {
			yield(nil, err)
			return
		}

		pageCount := stat.Size() / pageSize

		for i := int64(0); i < pageCount; i++ {
			if !yield(m.doFetchFromFile(uint64(i))) {
				return
			}
		}
	}
}
