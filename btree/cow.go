package btree

import (
    "fmt"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

// Create a new copy of node by assigning a free file-position to it. Also add
// the node into the commit queue.
func (kn *knode) copyOnWrite() Node {
    newkn := (&knode{}).newNode(kn.store)
    newkn.ks = newkn.ks[:len(kn.ks)]; copy(newkn.ks, kn.ks)
    newkn.ds = newkn.ds[:len(kn.ds)]; copy(newkn.ds, kn.ds)
    newkn.vs = newkn.vs[:len(kn.vs)]; copy(newkn.vs, kn.vs)
    newkn.size = len(kn.ks)
    return newkn
}

// Refer above.
func (in *inode) copyOnWrite() Node {
    newin := (&inode{}).newNode(in.store)
    newin.ks = newin.ks[:len(in.ks)]; copy(newin.ks, in.ks)
    newin.ds = newin.ds[:len(in.ds)]; copy(newin.ds, in.ds)
    newin.vs = newin.vs[:len(in.vs)]; copy(newin.vs, in.vs)
    newin.size = len(in.ks)
    return newin
}

// Create a new instance of `knode`, an in-memory representation of btree leaf
// block.
//   * keys slice must be half sized and zero valued, capacity of keys slice
//     must be 1 larger to accomodate slice-detection.
//   * values slice must be half+1 sized and zero valued, capacity of values
//     slice must be 1 larger to accomodate slice-detection.
func (kn *knode) newNode(store *Store) *knode {
    fpos := store.wstore.freelist.pop()

    max := store.maxKeys() // always even
    b := (&block{leaf: TRUE}).newBlock(max/2, max)
    newkn := &knode{block: *b, store: store, fpos: fpos, dirty: true}
    return newkn
}

// Refer to the notes above.
func (in *inode) newNode(store *Store) *inode {
    fpos := store.wstore.freelist.pop()

    max := store.maxKeys() // always even
    b := (&block{leaf: FALSE}).newBlock(max/2, max)
    kn := knode{block: *b, store: store, fpos: fpos, dirty:true}
    newin := &inode{knode: kn}
    return newin
}
