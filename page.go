package mattress

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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

func (r Record) Key() string {
	klen := uint8(r[0])
	return string(r[5 : 5+klen])
}

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

type PageHeader struct {
	PageId    uint64
	Flags     uint16
	SlotCount uint16
}

type Page struct {
	header       PageHeader
	slots        []Slot
	data         [pageSize]byte
	insertOffset int
	mux          sync.RWMutex
}

func NewPage(id uint64, flags uint16) *Page {
	return &Page{
		header: PageHeader{
			PageId: id,
			Flags:  flags,
		},
		data:         [pageSize]byte{},
		insertOffset: pageSize,
	}
}

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

	if !p.HasFreeSpace(recLen) {
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

	return len(p.slots) - 1, nil
}

func (p *Page) Read(slotId int) (Record, error) {
	p.mux.RLock()
	defer p.mux.RUnlock()

	if slotId >= len(p.slots) || !p.slots[slotId].alive {
		return nil, ErrInvalidSlot
	}

	slot := p.slots[slotId]
	return p.data[slot.offset : slot.offset+slot.len], nil
}

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
	if slotId == len(p.slots)-1 && p.HasFreeSpace(int(recLen-slot.cap)) {
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

func (p *Page) Delete(slotId int) error {
	p.mux.Lock()
	defer p.mux.Unlock()

	if slotId >= len(p.slots) || !p.slots[slotId].alive {
		return ErrInvalidSlot
	}

	p.slots[slotId].alive = false
	return nil
}

func (p *Page) HasFreeSpace(n int) bool {
	return p.insertOffset >= n+headerSize+slotSize*int(p.header.SlotCount+1)
}

func (p *Page) Encode() [pageSize]byte {
	p.mux.RLock()
	defer p.mux.RLock()

	// write header
	binary.LittleEndian.PutUint64(p.data[:8], p.header.PageId)
	binary.LittleEndian.PutUint16(p.data[8:10], uint16(p.header.Flags))
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
		header:       PageHeader{},
		slots:        []Slot{},
		data:         data,
		insertOffset: pageSize,
	}

	// read header
	if err := binary.Read(reader, binary.LittleEndian, &p.header.PageId); err != nil {
		return nil, fmt.Errorf("failed to read page id: %w", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &p.header.Flags); err != nil {
		return nil, fmt.Errorf("failed to read flags: %w", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &p.header.SlotCount); err != nil {
		return nil, fmt.Errorf("failed to read flags: %w", err)
	}

	// read slots
	for i := 0; i < int(p.header.SlotCount); i++ {
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
