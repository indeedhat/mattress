package main

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
	header    PageHeader
	slots     []Slot
	data      [pageSize]byte
	freeSpace int
	mux       sync.RWMutex
}

func (p *Page) Insert(record Record) (int, error) {
	p.mux.Lock()
	defer p.mux.Unlock()

	recLen := len(record)

	if !p.HasFreeSpace(recLen) {
		return -1, ErrPageFull
	}

	p.freeSpace -= len(record)
	copy(p.data[p.freeSpace:], record)

	p.slots = append(p.slots, Slot{
		offset: uint16(p.freeSpace),
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
	return p.data[slot.offset:slot.len], nil
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
		copy(p.data[slot.offset:], record)
		slot.len = recLen
		return slotId, nil
	}

	// if the slot is the last in the list then it can be extended without having to compact other
	// records
	if slotId == len(p.slots)-1 && recLen <= slot.cap+uint16(p.freeSpace) {
		p.freeSpace -= int(recLen - slot.cap)
		copy(p.data[p.freeSpace:], record)
		slot.len = recLen
		slot.cap = recLen
		slot.offset = uint16(p.freeSpace)
		return slotId, nil
	}

	slotId, err := p.Insert(record)
	if err != nil {
		slot.alive = false
	}

	return slotId, err
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
	return p.freeSpace >= n+slotSize
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
		header: PageHeader{},
		slots:  []Slot{},
		data:   data,
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
	}

	p.freeSpace = pageSize - headerSize - slotSize*int(p.header.SlotCount)

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
