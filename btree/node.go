package btree

import (
    "fmt"
)

// in-memory structure for leaf-block.
type knode struct { // keynode
    block   // embedded structure 

    // Book-keeping fields
    store *Store    // reference to Index data-structure
    fpos int64      // file-offset where this block resides

    // Dirty or not
    dirty bool
}

// in-memory structure for intermediate block.
type inode struct { // intermediate node
    knode
    dirtychild uint64   // number of dirty children under this node.
}

// Node interface that is implemented by both `knode` and `inode` structure.
type Node interface {
    // Inserts the value into the appropriate place in the tree, rebalancing
    // as necessary.  The first return value specifies if the value was
    // actually added (i.e. if it wasn't already there).  If a new node is 
    // created it is returned along with a separator value.
    insert(Key, Value) (Node, Node, bkey, []Node)

    // Return number of entries on all the leaf nodes from this Node.
    count() int64

    // Return value corresponding to lowest key in the tree.
    front() (uint32, []byte, []byte, []byte)

    // Returns true iff this tree contains the `key`.
    contains(Key) bool

    // Returns true iff this tree contains the `key` with specified `docid`
    equals(Key) bool

    // Passes all of the data in this node and its children through the cannel
    // in proper order.
    traverse(func(bkey, int64))

    // lookup index for key
    lookup(Key, Emitter)

    // Removes the value from the tree, rebalancing as necessary. Returns true
    // iff an element was actually deleted. Return,
    //  - Node
    //  - whether to rebalance or not.
    //  - slice of stalenodes
    remove(Key) (Node, bool, []Node)

    //---- Support methods.
    isLeaf() bool // Return whether node is a leaf node or not.
    getKnode() *knode // Return the underlying `knode` structure.
    getBlock() *block // Return the underlying `block` structure.
    copyOnWrite() Node // copy node for modification.

    // FIXME: Node is both *knode and *inode.
    // split() (Node, interface{}) // split node, happens during insert.
    // newNode(*Store) *inode // create a new node, happens during insert.

    balance(Node) int // count to rotate
    mergeRight(Node, bkey) (Node, []Node) // merge receiver to Node.
    mergeLeft(Node, bkey) (Node, []Node) // merge Node to receiver
    rotateRight(Node, int, bkey) bkey // rotate entries from receiver to Node.
    rotateLeft(Node, int, bkey) bkey // rotate entries from Node to receiver.

    //---- Development methods.
    listOffsets() []int64 // Return the list of offsets from sub-tree.
    show(int) // Render this block on stdout and recursively call child blocks
    showKeys(int) // Render keys at each level
    // Count cummulative entries at each level
    levelCount(int, []int64, int64, int64) ([]int64, int64, int64)
}

// get `block` structure embedded in knode, TODO: This must go into Node 
// interface !.
func (kn *knode) getBlock() *block {
    return &kn.block
}
// get `block` structure embedded in inode's knode.
func (in *inode) getBlock() *block {
    return &in.knode.block
}

// get `knode` structure, TODO: This must go into Node interface !
func (kn *knode) getKnode() *knode {
    return kn
}
// get `block` structure embedded in inode.
func (in *inode) getKnode() *knode {
    return &in.knode
}

func (kn *knode) listOffsets() []int64 {
    return []int64{kn.fpos}
}

func (in *inode) listOffsets() []int64 {
    ls := make([]int64, 0)
    for _, fpos := range in.vs {
        ls = append(ls, in.store.FetchNode(fpos).listOffsets()...)
    }
    return append(ls, in.fpos)
}

// Returns,
//  - index of the smallest value that is not less than `key`
//  - whether or not it equals `key`
// If there are no elements greater than or equal to `key` then it returns
// (len(node.key), false)
func (kn *knode) searchGE(key Key) (int, bool, bool) {
    ks := kn.ks
    if kn.size == 0 {
        return 0, false, false
    }

    low, high := 0, kn.size
    for (high-low) > 1 {
        mid := (high+low) / 2
        keyb := kn.store.fetchKey(ks[mid].kpos)
        docb := kn.store.fetchKey(ks[mid].dpos)
        if key.Less(keyb, docb) {
            high = mid
        } else {
            low = mid
        }
    }

    keyb := kn.store.fetchKey(ks[low].kpos)
    docb := kn.store.fetchKey(ks[low].dpos)
    if key.LessEq(keyb, docb) {
        keyeq, doceq := key.Equal(keyb, docb)
        return low, keyeq, doceq
    }
    return high, false, false
}

// Returns,
//  - index of the smallest value that is equal to `key`.
//  - whether or not it equals `key` and doc-id
// If there are no elements that matches `key`, returns (0, false)
func (kn *knode) searchEqual(key Key) (int, bool) {
    ks := kn.ks
    if kn.size == 0 {
        return 0, false
    }

    low, high, max := 0, kn.size-1, kn.size
    for high-low > 1 {
        mid := (high+low) / 2
        keyb := kn.store.fetchKey(ks[mid].kpos)
        docb := kn.store.fetchKey(ks[mid].dpos)
        if key.Less(keyb, docb) {
            high = mid
        } else {
            low = mid
        }
    }

    for i := low; i <= high; i++ {
        keyb := kn.store.fetchKey(ks[i].kpos)
        docb := kn.store.fetchDocid(ks[i].dpos)
        if keyeq, doceq := key.Equal(keyb, docb); keyeq && doceq {
            return i, true
        }
    }
    return max, false
}

//---- count
func (kn *knode) count() int64 {
    return int64(kn.size)
}

func (in *inode) count() int64 {
    n := int64(0)
    for _, v := range in.vs {
        n += in.store.FetchNode(v).count()
    }
    return n
}

//---- front
func (kn *knode) front() (uint32, []byte, []byte, []byte) {
    if kn.size == 0 {
        return 0, nil, nil, nil
    } else {
        return kn.ks[0].ctrl,
               kn.store.fetchValue(kn.ks[0].kpos),
               kn.store.fetchValue(kn.ks[0].dpos),
               kn.store.fetchValue(kn.vs[0])
    }
}

func (in *inode) front() (uint32, []byte, []byte, []byte) {
    return in.store.FetchNode(in.vs[0]).front()
}

//---- contains
func (kn *knode) contains(key Key) bool {
    _, keyeq, _ := kn.searchGE(key)
    return keyeq
}

func (in *inode) contains(key Key) bool {
    idx, keyeq, _ := in.searchGE(key)
    if keyeq {
        return true
    }
    return in.store.FetchNode(in.vs[idx]).contains(key)
}

//---- equals
func (kn *knode) equals(key Key) bool {
    _, keyeq, doceq := kn.searchGE(key)
    return keyeq && doceq
}

func (in *inode) equals(key Key) bool {
    idx, keyeq, doceq := in.searchGE(key)
    if keyeq && doceq {
        return true
    }
    return in.store.FetchNode(in.vs[idx]).equals(key)
}

//-- traverse
func (kn *knode) traverse(fun func(bkey, int64)) {
    for i := range kn.ks {
        fun(kn.ks[i], kn.vs[i])
    }
}

func (in *inode) traverse(fun func(bkey, int64)) {
    for _, v := range in.vs {
        in.store.FetchNode(v).traverse(fun)
    }
}

//---- lookup
func (kn *knode) lookup(key Key, emit Emitter) {
    index, _, _ := kn.searchGE(key)
    for i := index; i < kn.size; i++ {
        keyb := kn.store.fetchKey(kn.ks[i].kpos)
        if keyeq, _ := key.Equal(keyb, nil); keyeq {
            emit(kn.store.fetchValue(kn.vs[i]))
        } else {
            break
        }
    }
}

func (in *inode) lookup(key Key, emit Emitter) {
    index, _, _ := in.searchGE(key)
    for i := index; i < in.size+1; i++ {
        in.store.FetchNode(in.vs[i]).lookup(key, emit)
        if i < in.size {
            keyb := in.store.fetchKey(in.ks[i].kpos)
            if keyeq, _ := key.Equal(keyb, nil); keyeq == false {
                break
            }
        }
    }
}

// Convinience method
func (kn *knode) show(level int) {
    prefix := ""
    for i := 0; i < level; i++ {
        prefix += "  "
    }
    fmt.Printf(
        "%vleaf:%v size:%v fill: %v/%v, %v/%v, at fpos %v\n",
        prefix, kn.leaf, kn.size, len(kn.ks), cap(kn.ks), len(kn.vs),
        cap(kn.vs), kn.fpos,
    )
}

func (in *inode) show(level int) {
    prefix := ""
    for i := 0; i < level; i++ {
        prefix += "  "
    }
    (&in.knode).show(level)
    in.store.FetchNode(in.vs[0]).show(level+1)
    for i, bk := range in.ks {
        fmt.Printf("%v%vth key %v & %v\n", prefix, i, bk.kpos, bk.dpos)
        in.store.FetchNode(in.vs[i+1]).show(level+1)
    }
    fmt.Printf("%vdirtychild %v\n", prefix, in.dirtychild)
}

func (kn *knode) showKeys(level int) {
    prefix := ""
    for i := 0; i < level; i++ {
        prefix += "  "
    }
    for _, bk := range kn.ks {
        keyb := kn.store.fetchKey(bk.kpos)
        docb := kn.store.fetchKey(bk.dpos)
        fmt.Println(prefix, string(keyb), " ; ", string(docb))
    }
}

func (in *inode) showKeys(level int) {
    prefix := ""
    for i := 0; i < level; i++ {
        prefix += "  "
    }
    for i, bk := range in.ks {
        in.store.FetchNode(in.vs[i]).showKeys(level+1)
        keyb := in.store.fetchKey(bk.kpos)
        docb := in.store.fetchKey(bk.dpos)
        fmt.Println(prefix, "*", string(keyb), " ; ", string(docb))
    }
    in.store.FetchNode(in.vs[in.size]).showKeys(level+1)
}

func (kn *knode) levelCount(level int, acc []int64, ic, kc int64) ([]int64, int64, int64) {
    if len(acc) == level {
        acc = append(acc, int64(kn.size))
    } else {
        acc[level] += int64(kn.size)
    }
    return acc, ic, kc+1
}

func (in *inode) levelCount(level int, acc []int64, ic, kc int64) ([]int64, int64, int64) {
    if len(acc) == level {
        acc = append(acc, int64(in.size))
    } else {
        acc[level] += int64(in.size)
    }
    for _, v := range in.vs {
        acc, ic, kc = in.store.FetchNode(v).levelCount(level+1, acc, ic, kc)
    }
    return acc, ic+1, kc
}
