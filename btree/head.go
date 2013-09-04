// Manages head sector of btree index-file. The only thing a head sector
// contains is the file position of root block.
package btree

import (
    "encoding/binary"
    "fmt"
    "os"
)

// Structure to manage the head sector
type Head struct {
    store *Store
    dirty bool              // Tells whether `root` has side-effects
    root int64              // file-offset into index file that has root block
    fpos_head1 int64        // file-offset into index file where 1st-head is
    fpos_head2 int64        // file-offset into index file where 2nd-head is
}

// Create a new Head sector structure.
func newHead(store *Store) *Head {
    hd := Head {
        store: store,
        dirty: false,
        root: 0,
        fpos_head1: 0,
        fpos_head2: store.sectorsize,
    }
    return &hd
}

// Fetch head sector from index file, read root block's file position and
// check whether head1 and head2 copies are consistent.
func (hd *Head) fetch() *Head {
    var root1, root2 int64
    if hd.dirty {
        panic("Cannot read index head when in-memory copy is dirty")
    }
    rfd := hd.store.idxStore.rfd
    // Read from first sector
    rfd.Seek(0, os.SEEK_SET)
    if err := binary.Read(rfd, binary.LittleEndian, &root1); err != nil {
        panic("Unable to read root from first head sector")
    }
    // Read from second sector
    rfd.Seek(0, os.SEEK_SET)
    if err := binary.Read(rfd, binary.LittleEndian, &root2); err != nil {
        panic("Unable to read root from second head sector")
    }
    if root1 != root2 {
        panic(fmt.Sprintf("Inconsistent root position (%v,%v)\n", root1,root2))
    }
    // Update hd.store's head element
    hd.root = int64(root1)
    return hd
}

// Refer to new root block. When ever an entry / block is updated the entire
// chain has to be re-added.
func (hd *Head) setRoot(root int64) *Head {
    hd.root = root
    hd.dirty = true
    return hd
}

// flush head-structure to index-file.
func (hd *Head) flush() *Head {
    if hd.dirty == false {
        return hd
    }

    wfd := hd.store.idxStore.wfd
    // Write the second copy
    wfd.Seek(SECTOR_SIZE, os.SEEK_SET)
    if err := binary.Write(wfd, binary.LittleEndian, &hd.root); err != nil {
        panic("Unable to write root to the second copy of head")
    }
    // Write the first copy
    wfd.Seek(0, os.SEEK_SET)
    if err := binary.Write(wfd, binary.LittleEndian, &hd.root); err != nil {
        panic("Unable to write root to the second copy of head")
    }

    hd.dirty = false
    return hd
}
