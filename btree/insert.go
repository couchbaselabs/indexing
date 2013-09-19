package btree

import (
    "fmt"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

func (kn *knode) insert(key Key, v Value) (Node, Node, int64, int64, []Node) {
    index, _, _ := kn.searchGE(key)
    kn.ks = kn.ks[:len(kn.ks)+1]         // Make space in the key array
    kn.ds = kn.ds[:len(kn.ds)+1]         // Make space in the key array
    copy(kn.ks[index+1:], kn.ks[index:]) // Shift existing data out of the way
    copy(kn.ds[index+1:], kn.ds[index:]) // Shift existing data out of the way
    kn.ks[index], kn.ds[index] = kn.keyOf(key)

    kn.vs = kn.vs[:len(kn.vs)+1]         // Make space in the value array
    copy(kn.vs[index+1:], kn.vs[index:]) // Shift existing data out of the way
    kn.vs[index] = kn.valueOf(v)

    kn.size = len(kn.ks)
    if kn.size <= kn.store.maxKeys() {
        return kn, nil, -1, -1, []Node{}
    }
    spawnKn, mkfpos, mdfpos := kn.split()
    return kn, spawnKn, mkfpos, mdfpos, []Node{}
}

func (in *inode) insert(key Key, v Value) (Node, Node, int64, int64, []Node) {
    index, _, _ := in.searchGE(key)
    // Copy on write
    stalechild := in.store.FetchNode(in.vs[index])
    child := stalechild.copyOnWrite()

    // Recursive insert
    child, spawn, mkfpos, mdfpos, stalenodes := child.insert(key, v)
    stalenodes = append(stalenodes, stalechild)
    in.vs[index] = child.getKnode().fpos
    if spawn == nil {
        return in, nil, -1, -1, stalenodes
    }

    in.ks = in.ks[:len(in.ks)+1]           // Make space in the key array
    in.ds = in.ds[:len(in.ds)+1]           // Make space in the key array
    copy(in.ks[index+1:], in.ks[index:])   // Shift existing data out of the way
    copy(in.ds[index+1:], in.ds[index:])   // Shift existing data out of the way
    in.ks[index], in.ds[index] = mkfpos, mdfpos

    in.vs = in.vs[:len(in.vs)+1]           // Make space in the value array
    copy(in.vs[index+2:], in.vs[index+1:]) // Shift existing data out of the way
    in.vs[index+1] = spawn.getKnode().fpos

    in.size = len(in.ks)
    max := in.store.maxKeys()
    if in.size <= max {
        return in, nil, -1, -1, stalenodes
    }

    // this node is full, so we have to split
    spawnIn, mkfpos, mdfpos  := in.split()
    return in, spawnIn, mkfpos, mdfpos, stalenodes
}

// Split the leaf node into two.
//
// Before:                       |  After:
//          keys        values   |           keys        values
// newkn     0            0      |  newkn    max/2      max/2 + 1
// kn       max+1       max+2    |  kn     max/2 + 1    max+2 + 2 (0 appended)
//
// `kn` will contain the first half, while `newkn` will contain the second
// half. Returns,
//  - new leaf node,
//  - key, that splits the two nodes with LessEq() method.
func (kn *knode) split() (*knode, int64, int64) {
    // Get a free block
    max := kn.store.maxKeys() // always even

    newkn := (&knode{}).newNode(kn.store) // Fetch a newnode from freelist

    copy(newkn.ks, kn.ks[max/2+1:])
    copy(newkn.ds, kn.ds[max/2+1:])
    kn.ks = kn.ks[:max/2+1]
    kn.ds = kn.ds[:max/2+1]
    kn.size = len(kn.ks)
    newkn.size = len(newkn.ks)

    copy(newkn.vs, kn.vs[max/2+1:])
    kn.vs = append(kn.vs[:max/2+1], 0)
    return newkn, newkn.ks[0], newkn.ds[0]
}

// Split intermediate node into two.
//
// Before:                       |  After:
//          keys        values   |           keys        values
// newkn     0            0      |  newkn    max/2      max/2 + 1
// kn       max+1       max+2    |  kn       max/2      max+2 + 2 (0 appended)
//
// `kn` will contain the first half, while `newkn` will contain the second
// half. Returns,
//  - new leaf node,
//  - key, that splits the two nodes with LessEq() method.
func (in *inode) split() (*inode, int64, int64) {
    // Get a free block
    max := in.store.maxKeys()  // always even

    newin := (&inode{}).newNode(in.store) // Fetch a newnode from freelist

    copy(newin.ks, in.ks[max/2+1:])
    copy(newin.ds, in.ds[max/2+1:])
    mkfpos, mdfpos := in.ks[max/2], in.ds[max/2]
    in.ks = in.ks[:max/2]
    in.ds = in.ds[:max/2]
    in.size = len(in.ks)
    newin.size = len(newin.ks)

    copy(newin.vs, in.vs[max/2+1:])
    in.vs = in.vs[:max/2+1]
    return newin, mkfpos, mdfpos
}

func (kn *knode) keyOf(k Key) (int64, int64) {
    return kn.store.appendKey(k.Bytes()), kn.store.appendDocid(k.Docid())
}

func (kn *knode) valueOf(v Value) int64 {
    return kn.store.appendValue(v.Bytes())
}
