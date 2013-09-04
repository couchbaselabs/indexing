// Manages list of free blocks in btree index-file.
package btree

import (
    "encoding/binary"
    "fmt"
    "os"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

// Structure to manage the free list
type FreeList struct {
    store *Store
    dirty bool              // Tells whether `freelist` contain side-effects
    fpos_block1 int64       // file-offset into index file where 1st-list is
    fpos_block2 int64       // file-offset into index file where 2nd-list is
    offsets []int64         // array(slice) of free blocks
}

// Create a new FreeList structure
func newFreeList(store *Store) *FreeList {
    sl := FreeList {
        store: store,
        dirty: false,
        fpos_block1: store.sectorsize*2,
        fpos_block2: store.sectorsize*2 + store.flistsize,
        offsets: make([]int64, 1, maxFreeBlocks(store)+1),  // lastblock is zero
    }
    return &sl
}

// Fetch list of free blocks from index file. 
func (fl *FreeList) fetch() *FreeList {
    var fpos int64
    if fl.dirty {
        panic("Cannot read index head when in-memory copy is dirty")
    }

    rfd := fl.store.idxStore.rfd

    // Read the first copy
    rfd.Seek(fl.fpos_block1, os.SEEK_SET)
    fl.offsets = fl.offsets[0:0]
    for i := 0; i < maxFreeBlocks(fl.store); i++ {
        if err := binary.Read(rfd, binary.LittleEndian, &fpos); err != nil {
            panic(err.Error())
        }
        if fpos == 0 {
            break
        }
        fl.offsets = append(fl.offsets, int64(fpos))
    }
    fl.offsets = append(fl.offsets, 0) // zero terminator for the list

    // Read the second copy
    rfd.Seek(fl.fpos_block2, os.SEEK_SET)
    for i := 0; i < maxFreeBlocks(fl.store); i++ {
        if err := binary.Read(rfd, binary.LittleEndian, &fpos); err != nil {
            panic(err.Error())
        }
        if fpos != 0 && fl.offsets[i] != fpos {
            panic("Mismatch in freeblock list")
        } else if fpos == 0 {
            break
        }
    }
    return fl
}

// Add a list of offsets to free blocks. By adding `offsets` into the
// freelist, length of freelist must not exceed `maxFreeBlocks()+1`.
// Typically add() is called after a call to appendBlocks().
func (fl *FreeList) add(offsets []int64) *FreeList {
    ln := len(fl.offsets)-1
    if (ln + len(offsets)) <= maxFreeBlocks(fl.store) {
        fl.offsets = fl.offsets[:ln+len(offsets)]
        // `ln-1` adjusts for zero-terminator
        copy(fl.offsets[ln:], offsets)
        fl.offsets = append(fl.offsets, 0) // zero terminator for the list
        fl.dirty = true
    } else {
        panic("Cannot add more than maxFreeBlocks()")
    }
    return fl
}

// Get a freeblock
func (fl *FreeList) pop() int64 {
    fpos := fl.offsets[0]
    if fpos == 0 {
        panic("freelist empty")
    }
    fl.offsets = fl.offsets[1:]
    return fpos
}

func (fl *FreeList) flush() *FreeList {
    if fl.dirty == false {
        return fl
    }

    block := make([]byte, FLIST_SIZE)
    wfd := fl.store.idxStore.wfd
    rfd := fl.store.idxStore.rfd

    // Write the second copy
    wfd.Seek(fl.fpos_block2, os.SEEK_SET)
    for _, fpos := range fl.offsets {
        if err := binary.Write(wfd, binary.LittleEndian, &fpos); err != nil {
            panic(err.Error())
        }
    }
    // Write the first copy
    if _, err := rfd.ReadAt(block, fl.fpos_block2); err != nil {
        panic(err.Error())
    }
    if _, err := wfd.WriteAt(block, fl.fpos_block1); err != nil {
        panic(err.Error())
    }
    fl.dirty = false
    return fl
}

// Get the maximum number of free blocks that can be monitored by the
// index-file. Returned value is actual value - 1 because the last entry is
// null terminated and used to detect the end of free-list.
func maxFreeBlocks(store *Store) int {
    count := store.flistsize / OFFSET_SIZE
    count -= 1 // Last reference will always be zero.
    return int(count)
}
