package btree

import (
    "fmt"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

// Create a new copy of node by assigning a free file-position to it. Also add
// the node into the commit queue.
func (kn *knode) copyOnWrite() Node {
    newkn := *kn
    if newkn.dirty == false {
        newkn.fpos = newkn.store.wstore.popFreelist()
    }
    newkn.dirty = true
    newkn.store.wstore.commit(&newkn)
    return &newkn
}

// Refer above.
func (in *inode) copyOnWrite() Node {
    newin := *in
    if newin.dirty == false {
        newin.fpos = newin.store.wstore.popFreelist()
    }
    newin.dirty = true
    newin.store.wstore.commit(&newin)
    return &newin
}

// Create a new instance of `knode`, an in-memory representation of btree leaf
// block.
//   * keys slice must be half sized and zero valued, capacity of keys slice
//     must be 1 larger to accomodate slice-detection.
//   * values slice must be half+1 sized and zero valued, capacity of values
//     slice must be 1 larger to accomodate slice-detection.
func (kn *knode) newNode(store *Store) *knode {
    fpos := store.wstore.popFreelist()

    max := store.maxKeys() // always even
    b := (&block{leaf: TRUE}).newBlock(max/2, max)
    newkn := &knode{block: *b, store: store, fpos: fpos, dirty: true}
    store.wstore.commit(newkn)
    return newkn
}

// Refer to the notes above.
func (in *inode) newNode(store *Store) *inode {
    fpos := store.wstore.popFreelist()

    max := store.maxKeys() // always even
    b := (&block{leaf: FALSE}).newBlock(max/2, max)
    kn := knode{block: *b, store: store, fpos: fpos, dirty:true}
    newin := &inode{knode: kn}
    store.wstore.commit(newin)
    return newin
}
