package btree
import "bytes"
import "fmt"
import "encoding/binary"

type FreeList struct {
    store *Store
    dirty bool              // Tells whether `freelist` contain side-effects
    disksize int64          // disk size of freeblock list in bytes.
    fpos_block1 int64       // file-offset into index file where 1st-list is
    fpos_block2 int64       // file-offset into index file where 2nd-list is
    offsets []int64         // array(slice) of free blocks
    block1 []byte           // Temporary buffer to read/write from/to disk.
    block2 []byte           // Temporary buffer to read/write from/to disk.
}

// fetch() *FreeList
// add([]uint64) *FreeList
// pop() uint64
// flush() *FreeList
func newFreeList(store *Store) *FreeList {
    sl := FreeList {
        store: store,
        dirty: false,
        disksize: store.flistsize,
        fpos_block1: store.sectorsize*2,
        fpos_block2: store.sectorsize*2 + store.flistsize,
        offsets: make([]int64, 0, store.flistsize/OFFSET_SIZE),
        block1: make([]byte, store.flistsize),
        block2: make([]byte, store.flistsize),
    }
    return &sl
}

func (fl *FreeList) fetch() *FreeList {
    idxstore := fl.store.idxStore
    if fl.dirty {
        panic("Cannot read index head when in-memory copy is dirty")
    }
    // Read the first copy
    if _, err := idxstore.rfd.ReadAt(fl.block1, fl.fpos_block1); err != nil {
        panic( err.Error() )
    }
    i := 0
    for {
        fpos, n := binary.Uvarint(fl.block1[i:])
        if fpos == 0 { break }
        fl.offsets = fl.offsets[0:len(fl.offsets)+1]
        fl.offsets[ len(fl.offsets) ] = int64(fpos)
        i += n
    }
    // Read the second copy
    if _, err := idxstore.rfd.ReadAt(fl.block2, fl.fpos_block2); err != nil {
        panic( err.Error() )
    }
    // Consistency check
    if bytes.Compare(fl.block1, fl.block2) != 0 {
        panic(fmt.Sprintf("Inconsistent free-list position \n"))
    }
    return fl
}

func (fl *FreeList) add( offsets []int64 ) *FreeList {
    copy(fl.offsets[len(fl.offsets):], offsets)
    fl.dirty = true
    return fl
}

func (fl *FreeList) pop() int64 {
    fpos := fl.offsets[0]
    fl.offsets = fl.offsets[1:]
    return fpos
}

func (fl *FreeList) flush() *FreeList {
    if fl.dirty == false { return fl }
    idxstore := fl.store.idxStore

    i := 0
    for _, fpos := range fl.offsets {
        i += binary.PutUvarint( fl.block1[i:], uint64(fpos) )
    }
    copy(fl.block2, fl.block1)
    // Write the second copy
    if _, err := idxstore.wfd.WriteAt(fl.block2, fl.fpos_block2); err != nil {
        panic( err.Error() )
    }
    // Write the first copy
    if _, err := idxstore.wfd.WriteAt(fl.block1, fl.fpos_block1); err != nil {
        panic( err.Error() )
    }
    fl.dirty = false
    return fl
}

