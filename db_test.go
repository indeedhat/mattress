package mattress

import (
	"os"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
)

const dbPath = "./db.mtrs"

func TestBasicFunctionality(t *testing.T) {
	_ = os.Remove(dbPath)

	db := NewDB()
	defer db.Close()

	require.Nil(t, db.Open(dbPath))

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

	// try to set existing key
	err = db.Put("key1", "updated value")
	require.Nil(t, err)

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

	// can replace deleted entry
	require.Nil(t, db.Put("key2", "value 2 replaced"))
	v2, err = db.Get("key2")
	require.Nil(t, err)
	require.Equal(t, "value 2 replaced", v2)

	// index gets rebuilt after close/open
	require.Nil(t, db.Close())
	require.Nil(t, db.Open(dbPath))
	spew.Dump(db.index)
	require.Equal(t, 2, db.Len())

	v1, err = db.Get("key1")
	require.Nil(t, err)
	require.Equal(t, "updated value", v1)

	v2, err = db.Get("key2")
	require.Nil(t, err)
	require.Equal(t, "value 2 replaced", v2)
}
