package mattress

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBasicFunctionalityWithFIleStorage(t *testing.T) {
	dbPath := "./tst.mtrs"
	_ = os.Remove(dbPath)
	db := NewDB()

	require.Nil(t, db.Open(dbPath))
	defer db.Close()

	basicFunctionalityShared(t, db)

	// index gets rebuilt after close/open
	t.Run("index gets rebuilt after close/open", func(t *testing.T) {
		require.Nil(t, db.Close())
		require.Nil(t, db.Open(dbPath))
		require.Equal(t, 2, db.Len())

		v1, err := db.Get("key1")
		require.Nil(t, err)
		require.Equal(t, "updated value", v1)

		v2, err := db.Get("key2")
		require.Nil(t, err)
		require.Equal(t, "value 2 replaced", v2)
	})
}

func TestBasicFunctionalityWithInMemoryStorage(t *testing.T) {
	dbPath := ":memory:"
	_ = os.Remove(dbPath)
	db := NewDB()

	require.Nil(t, db.Open(dbPath))
	defer db.Close()

	basicFunctionalityShared(t, db)
}

func basicFunctionalityShared(t *testing.T, db *DB) {
	t.Run("values can be inserted and retrieved", func(t *testing.T) {
		// setup values
		require.Nil(t, db.Put("key1", "value 1"))
		require.Nil(t, db.Put("key2", "value 2"))
		require.Nil(t, db.Put("key3", "value 3"))

		// get values
		v1, err := db.Get("key1")
		require.Nil(t, err)
		require.Equal(t, "value 1", v1)

		v2, err := db.Get("key2")
		require.Nil(t, err)
		require.Equal(t, "value 2", v2)

		v3, err := db.Get("key3")
		require.Nil(t, err)
		require.Equal(t, "value 3", v3)
	})

	t.Run("values can be updated", func(t *testing.T) {
		// try to set existing key
		err := db.Put("key1", "updated value")
		require.Nil(t, err)

		val, err := db.Get("key1")
		require.Nil(t, err)
		require.Equal(t, "updated value", val)
	})

	t.Run("values can be deleted", func(t *testing.T) {
		// delete value
		require.Nil(t, db.Delete("key2"))
		require.Nil(t, db.Delete("key3"))

		// get missing entries
		m1, err := db.Get("key2")
		require.Equal(t, "", m1)
		require.NotNil(t, err)
		require.Equal(t, "entry key2 not found", err.Error())

		m2, err := db.Get("key4")
		require.Equal(t, "", m2)
		require.NotNil(t, err)
		require.Equal(t, "entry key4 not found", err.Error())
	})

	t.Run("values that have been previously deleted can be re set", func(t *testing.T) {
		require.Nil(t, db.Put("key2", "value 2 replaced"))

		v2, err := db.Get("key2")
		require.Nil(t, err)
		require.Equal(t, "value 2 replaced", v2)
	})
}
