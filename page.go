package mattress

import (
	"encoding/binary"
	"errors"
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

type PageId uint64

func (i PageId) V() uint64 {
	return uint64(i)
}

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
	PageId    PageId
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
func NewPage(id PageId, flags uint16) *Page {
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
	defer p.mux.RUnlock()

	// write header
	binary.LittleEndian.PutUint64(p.data[:8], p.Header.PageId.V())
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
