package mattress

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBufferPool(t *testing.T) {
	store, _ := NewInMemoryStorageManager()
	pool := NewBufferPool(store, unlimitedBufferPoolSize)

	t.Run("create adds a new clean page/frame to the pool", func(t *testing.T) {
		page, err := pool.Create()
		require.Nil(t, err)
		defer pool.Release(page, false)

		require.Zero(t, page.Header.SlotCount)
	})

	t.Run("can fetch a page from the pool by id", func(t *testing.T) {
		page, err := pool.Fetch(0)
		require.Nil(t, err)
		require.IsType(t, &Page{}, page)
	})

	t.Run("returns error on non existent page id", func(t *testing.T) {
		_, err := pool.Fetch(123)
		require.NotNil(t, err)
		require.Equal(t, "", err.Error())
	})
}
