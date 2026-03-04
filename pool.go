package mattress

import (
	"errors"
	"sync"
)

const (
	defaultBufferPoolSize   = 100
	unlimitedBufferPoolSize = 0
)

type Frame struct {
	page     *Page
	pinCount int
	dirty    bool
}

type BufferPool struct {
	pages    map[PageId]*Frame
	mux      sync.Mutex
	disk     StorageManager
	poolSize uint64
}

func NewBufferPool(dm StorageManager, poolSize uint64) *BufferPool {
	return &BufferPool{
		pages: make(map[PageId]*Frame),
		disk:  dm,
	}
}

// Close flushes changes to disk and closes the disk manager
//
// at the point this is called the parent has already confirmed that work is complete
// so I don't need to worry about pin counts
func (b *BufferPool) Close() error {
	b.mux.Lock()
	defer b.mux.Unlock()

	for _, frame := range b.pages {
		if !frame.dirty {
			continue
		}

		b.disk.WritePage(frame.page)
	}

	return b.disk.Close()
}

// Release relases the parents claim on the page
// this will decrement the pin counter and set the dirty flag where necessary
func (b *BufferPool) Release(p *Page, dirty bool) error {
	b.mux.Lock()
	defer b.mux.Unlock()

	frame, found := b.pages[p.Header.PageId]
	if !found {
		return errors.New("frame not found for page")
	}

	frame.dirty = frame.dirty || dirty
	frame.pinCount--

	return nil
}

// Fetch will attempt to fetch a page by its id
func (b *BufferPool) Fetch(id PageId) (*Page, error) {
	b.mux.Lock()
	defer b.mux.Unlock()

	if f, found := b.pages[id]; found {
		f.pinCount++
		return f.page, nil
	}

	if err := b.evict(); err != nil {
		return nil, err
	}

	p, err := b.disk.ReadPage(id)
	if err != nil {
		return nil, err
	}

	b.pages[id] = &Frame{
		page:     p,
		pinCount: 1,
		dirty:    false,
	}

	return p, nil
}

// Create creates a new page and returns it
func (b *BufferPool) Create() (*Page, error) {
	if err := b.evict(); err != nil {
		return nil, err
	}

	id := b.disk.AllocatePage()
	page := NewPage(id, 0x0)
	b.pages[page.Header.PageId] = &Frame{
		page:     page,
		pinCount: 1,
		dirty:    false,
	}

	return page, nil
}

// evict removes a page from the buffer pool, if it is dirty then it will first
// be persisted to disk
func (b *BufferPool) evict() error {
	// a poolSize of 0 indicates that there is no max size, everything should
	// be kept in memory
	if b.poolSize == 0 {
		return nil
	}

	if b.poolSize > uint64(len(b.pages)) {
		return nil
	}

	for id, frame := range b.pages {
		if frame.pinCount > 0 {
			continue
		}

		if frame.dirty {
			if err := b.disk.WritePage(frame.page); err != nil {
				return err
			}
		}

		delete(b.pages, id)
		return nil
	}

	return errors.New("could not find any pages to evict")
}
