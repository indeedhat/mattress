package mattress

import "math"

type Fsm struct {
	freeList map[PageId]uint16
}

func NewFsm() *Fsm {
	return &Fsm{make(map[PageId]uint16)}
}

// Set sets a free space value for a page id
func (f *Fsm) Set(id PageId, free uint16) {
	f.freeList[id] = free
}

// Get retrieves the free space value for a page by id
func (f *Fsm) Get(id PageId) uint16 {
	if v, found := f.freeList[id]; found {
		return v
	}
	return 0
}

// FindSpace searches for a page with enough free space to satisfiy the size
// request
func (f *Fsm) FindSpace(n int) (PageId, bool) {
	for id, free := range f.freeList {
		if n <= int(free) {
			return id, true
		}
	}

	return PageId(math.MaxUint64), false
}
