// Supplies API to append/fetch key/value/docid from kv-file. kv-file is
// opened and managed by the Store API.
// entry format is,
//
//      | 4-byte size | size-byte value |
//
// Maximum size of each entry is int32, that is 2^31.

package btree

import (
    "encoding/binary"
    "fmt"
    "os"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

// Append/Fetch value as either byte-slice or string
func (store *Store) fetchValue(fpos int64) []byte {
    return readKV(store.kvStore.rfd, fpos)
}

func (store *Store) fetchValueS(fpos int64) string {
    return string(readKV(store.kvStore.rfd, fpos))
}

func (store *Store) appendValue(val []byte) int64 {
    return appendKV(store.kvStore.wfd, val)
}

func (store *Store) appendValueS(val string) int64 {
    return appendKV(store.kvStore.wfd, []byte(val))
}

// Append/Fetch key as either byte-slice or string
func (store *Store) fetchKey(fpos int64) []byte {
    return readKV(store.kvStore.rfd, fpos)
}

func (store *Store) fetchKeyS(fpos int64) string {
    return string(readKV(store.kvStore.rfd, fpos))
}

func (store *Store) appendKey(key []byte) int64 {
    return appendKV(store.kvStore.wfd, key)
}

func (store *Store) appendKeyS(key string) int64 {
    return appendKV(store.kvStore.wfd, []byte(key))
}

// Append/Fetch Docid as either byte-slice or string
func (store *Store) fetchDocid(fpos int64) []byte {
    return readKV(store.kvStore.rfd, fpos)
}

func (store *Store) fetchDocidS(fpos int64) string {
    return string(readKV(store.kvStore.rfd, fpos))
}

func (store *Store) appendDocid(docid []byte) int64 {
    return appendKV(store.kvStore.wfd, docid)
}

func (store *Store) appendDocidS(docid string) int64 {
    return appendKV(store.kvStore.wfd, []byte(docid))
}

// Append bytes to `kvStore.wfd`
func appendKV(wfd *os.File, val []byte) int64 {
    fpos, err := wfd.Seek(0, os.SEEK_CUR)
    if err != nil {
        panic(err.Error())
    }
    binary.Write(wfd, binary.LittleEndian, int32(len(val)))
    if _, err := wfd.Write( val ); err != nil {
        panic(err.Error())
    }
    return fpos
}

// Read bytes from `kvStore.rfd` at `fpos`
func readKV(rfd *os.File, fpos int64) []byte {
    var size int32
    if _, err := rfd.Seek(fpos, os.SEEK_SET); err != nil {
        panic( err.Error() )
    }
    binary.Read(rfd, binary.LittleEndian, &size)
    b := make([]byte, size)
    if _, err := rfd.Read( b ); err != nil {
        panic( err.Error() )
    }
    return b
}
