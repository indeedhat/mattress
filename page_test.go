package mattress

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeAndReadPage(t *testing.T) {
	pageData, err := os.ReadFile("./fixtures/basic.page")
	require.Nil(t, err)

	page, err := decodePage([4096]byte(pageData))
	require.Nil(t, err)

	require.Equal(t, uint64(123), page.header.PageId)
	require.Equal(t, uint16(0x13), page.header.Flags)
	require.Equal(t, uint16(2), page.header.SlotCount)
	require.Len(t, page.slots, 2)

	require.Equal(t, Slot{
		offset: 4072,
		cap:    24,
		len:    24,
		alive:  true,
	}, page.slots[0])
	require.Equal(t, Slot{
		offset: 4036,
		cap:    36,
		len:    36,
		alive:  true,
	}, page.slots[1])

	r1, err := page.Read(0)
	require.Nil(t, err)
	require.Equal(t, "key1", r1.Key())
	require.Equal(t, "this is value 1", r1.Value())

	r2, err := page.Read(1)
	require.Nil(t, err)
	require.Equal(t, "key 2", r2.Key())
	require.Equal(t, "This key has a space in it", r2.Value())
}

func TestCreateAndEncodePage(t *testing.T) {
	p := NewPage(123, 0x13)
	p.Insert(encodeRecord("key1", "this is value 1"))
	p.Insert(encodeRecord("key 2", "This key has a space in it"))

	slot, err := p.Insert(encodeRecord("too large to fit", strings.Repeat("A", 4096)))
	require.Equal(t, -1, slot)
	require.Equal(t, err, ErrPageFull)

	data := p.Encode()

	expectedData, err := os.ReadFile("./fixtures/basic.page")
	require.Nil(t, err)
	require.Equal(t, [4096]byte(expectedData), data)
}

func TestUpdateExistingRecords(t *testing.T) {
	pageData, err := os.ReadFile("./fixtures/basic.page")
	require.Nil(t, err)

	page, err := decodePage([4096]byte(pageData))
	require.Nil(t, err)

	// lets try the unhappy paths first
	slot, err := page.Update(1, encodeRecord("too large to fit", strings.Repeat("A", 4096)))
	require.Equal(t, -1, slot)
	require.Equal(t, err, ErrPageFull)

	slot, err = page.Update(5, encodeRecord("too large to fit", strings.Repeat("A", 4096)))
	require.Equal(t, -1, slot)
	require.Equal(t, err, ErrInvalidSlot)

	// updating the last record with a longer value should just grow the record (slot id's start from 0)
	slot, err = page.Update(1, encodeRecord("key 2", "This key has a space in it, and now its longer"))
	require.Nil(t, err)
	require.Equal(t, 1, slot)

	r2, err := page.Read(1)
	require.Nil(t, err)
	require.Equal(t, "key 2", r2.Key())
	require.Equal(t, "This key has a space in it, and now its longer", r2.Value())

	// updating any other record with a longer value will mark teh record as dead and create a new one
	slot, err = page.Update(0, encodeRecord("key1", "This is an entirely new slot in the page"))
	require.Nil(t, err)
	require.Equal(t, 2, slot)

	r1, err := page.Read(0)
	require.Nil(t, r1)
	require.Equal(t, ErrInvalidSlot, err)

	r3, err := page.Read(2)
	require.Nil(t, err)
	require.Equal(t, "key1", r3.Key())
	require.Equal(t, "This is an entirely new slot in the page", r3.Value())

	expectedData, err := os.ReadFile("./fixtures/updated.page")
	require.Nil(t, err)
	require.Equal(t, [4096]byte(expectedData), page.Encode())
}

func TestDeleteRecords(t *testing.T) {
	pageData, err := os.ReadFile("./fixtures/basic.page")
	require.Nil(t, err)

	page, err := decodePage([4096]byte(pageData))
	require.Nil(t, err)

	err = page.Delete(5)
	require.Equal(t, ErrInvalidSlot, err)

	err = page.Delete(0)
	require.Nil(t, err)

	r1, err := page.Read(0)
	require.Nil(t, r1)
	require.Equal(t, ErrInvalidSlot, err)

	expectedData, err := os.ReadFile("./fixtures/deleted.page")
	require.Nil(t, err)
	require.Equal(t, [4096]byte(expectedData), page.Encode())
}
