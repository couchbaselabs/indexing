package btree
import "encoding/binary"
import "fmt"

type Head struct {
    store *Store
    dirty bool              // Tells whether `root` has side-effects
    root int64              // file-offset into index file that has root block
    disksize int64          // disk-size of head sector in bytes
    fpos_head1 int64        // file-offset into index file where 1st-head is
    fpos_head2 int64        // file-offset into index file where 2nd-head is
    head1 []byte            // in-memory copy of disk sector.
    head2 []byte            // in-memory copy of disk sector.
}

func newHead(store *Store) *Head {
    hd := Head {
        store: store,
        dirty: false,
        root: 0,
        disksize: store.sectorsize,
        fpos_head1: 0,
        fpos_head2: store.sectorsize,
        head1: make([]byte, store.sectorsize),
        head2: make([]byte, store.sectorsize),
    }
    return &hd
}

func (hd *Head) fetch() *Head {
    if hd.dirty {
        panic("Cannot read index head when in-memory copy is dirty")
    }
    idxstore := hd.store.idxStore
    // Read the first copy
    _, err := idxstore.rfd.ReadAt(hd.head1, hd.fpos_head1)
    if err != nil {
        panic( err.Error() )
    }
    root1, _ := binary.Uvarint( hd.head1 )
    // Read the second copy
    _, err = idxstore.rfd.ReadAt(hd.head2, hd.fpos_head2)
    if err != nil {
        panic( err.Error() )
    }
    root2, _ := binary.Uvarint( hd.head2 )
    // Consistency check
    if root1 != root2 {
        panic(fmt.Sprintf("Inconsistent root position (%v,%v)\n", root1, root2))
    }
    // Update hd.store's head element
    hd.root = int64(root1)
    return hd
}

func (hd *Head) setRoot(root int64) *Head {
    hd.root = root
    binary.PutUvarint(hd.head1, uint64(root))
    binary.PutUvarint(hd.head2, uint64(root))
    hd.dirty = true
    return hd
}

func (hd *Head) flush() *Head {
    if hd.dirty == false { return hd }

    idxstore := hd.store.idxStore
    // Write the second copy
    if _, err := idxstore.wfd.WriteAt(hd.head2, hd.fpos_head2); err != nil {
        panic( err.Error() )
    }
    // Write the first copy
    if _, err := idxstore.wfd.WriteAt(hd.head1, hd.fpos_head1); err != nil {
        panic( err.Error() )
    }
    hd.dirty = false
    return hd
}

