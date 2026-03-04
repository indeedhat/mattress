package mattress

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFsm(t *testing.T) {
	fsm := Fsm{map[PageId]uint16{
		PageId(0): 12,
		PageId(1): 200,
		PageId(2): 1000,
	}}

	t.Run("Get returns the correct value", func(t *testing.T) {
		require.Equal(t, uint16(12), fsm.Get(PageId(0)))
		require.Equal(t, uint16(200), fsm.Get(PageId(1)))
		require.Equal(t, uint16(1000), fsm.Get(PageId(2)))

		// 3 currently doesn't exist so should return 0
		require.Equal(t, uint16(0), fsm.Get(PageId(3)))
	})

	t.Run("FindSpace returns an entry with enough space", func(t *testing.T) {
		// as fsm is backed by a map it can return any entry with enough space
		id, found := fsm.FindSpace(10)
		require.True(t, found)
		require.Contains(t, []PageId{0, 1, 2}, id)

		id, found = fsm.FindSpace(100)
		require.True(t, found)
		require.Contains(t, []PageId{1, 2}, id)

		id, found = fsm.FindSpace(500)
		require.True(t, found)
		require.Equal(t, PageId(2), id)

		id, found = fsm.FindSpace(1024)
		require.False(t, found)
		require.Equal(t, PageId(math.MaxUint64), id)
	})

	t.Run("Set can update space values", func(t *testing.T) {
		// existing
		fsm.Set(PageId(0), 86)
		require.Equal(t, uint16(86), fsm.Get(0))

		// add new
		fsm.Set(PageId(3), 101)
		require.Equal(t, uint16(101), fsm.Get(3))
	})
}
