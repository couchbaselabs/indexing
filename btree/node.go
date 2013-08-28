package btree

type knode struct { // keynode
    block   // embedded structure 

    // Book-keeping fields
    store *Store    // reference to Index data-structure
    fpos int64      // file-offset where this block resides

    // Fields are applicable when node is mapped under `dirtyblocks`
    dirty bool          // if dirty, `stalefpos` and `stalenodes` becomes valid.
    stalefpos int64     // fpos of old block
    stalenodes []int64  // Child nodes that went stale due to rebalancing.
}

type inode struct { // intermediate node
    knode
    dirtychild uint64   // number of dirty children under this node.
}

type Node interface {
    // Return number of entries on all the leaf nodes from this Node.
    count() uint64

    // Return value corresponding to lowest key in the tree.
    front() []byte

    // Returns true iff this tree contains the value.
    contains(Key) bool

    // Passes all of the data in this node and its children through the cannel
    // in proper order.
    traverse(func(bkey, int64))

    // Inserts the value into the appropriate place in the tree, rebalancing
    // as necessary.  The first return value specifies if the value was
    // actually added (i.e. if it wasn't already there).  If a new node is 
    // created it is returned along with a separator value.
    insert(Key, Value) (Node, Node, bkey)

    // lookup index for key
    lookup(Key, Emitter)

    // Removes the value from the tree, rebalancing as necessary. Returns true
    // iff an element was actually deleted.
    remove(Key) (Node, bool, []byte)

    // Convenient development methods

    //show(int)             // Textual display of a tree
    //fsck(int, Less) bool  // Does a quick sanity check to make sure the tree is in order.

    // Grabs all of the data in bNode and merges it into this node.  Performs no checks while doing
    // so.

    //merge(bNode)

    // Like merge, but additionally adds an additional separator between the keys in the two nodes.

    //mergeWithSeparator(bNode,T)

    // We frequently need to ask about the number of keys in the node, and even though all nodes
    // contain keyNode, we still can't access that without either a type check or exposing this
    // method.

    //lenKeys() int

    // This is a method that provides a flexible way of traversing and deleting and element from the
    // tree.  The function returns two elements (traverse, delete), which indicate the index of which
    // element should be traversed or deleted.  A value of -1 is ignored, and at least one of them
    // will always be -1.  This allows us to traverse and delete a specific value from the tree, as
    // well as deleting the upper or lower bound of a value.

    //traverseAndDelete(visitor) (bool, T)

    // The following methods are used in conjunction with traverseAndDelete

    //deleteMin() (int,int)
    //deleteMax() (int,int)
    //deleteTarget(T, Less) (int,int)
    getKnode() *knode
    copyOnWrite() Node
    merge(Node, bkey) Node
    rotateLeft(Node, uint64, bkey) bkey
    rotateRight(Node, uint64, bkey) bkey
}

// Make this leaf-node a prestine copy of the disk version, this is case when
// the node was just fetched from disk or when it was just flushed to disk.
func (kn *knode) prestine() Node {
    kn.dirty = false
    return kn
}

func (kn *knode) getBlock() *block {
    return &kn.block
}

func (kn *knode) getKnode() *knode {
    return kn
}

func (in *inode) getBlock() *block {
    return &in.knode.block
}

func (in *inode) getKnode() *knode {
    return &in.knode
}

// Returns,
//  - index of the smallest value that is not less than `key`
//  - whether or not it equals `key`
//  - whether or not it also matches the doc-id.
// If there are no elements greater than or equal to `key` then it returns
// (len(node.key), false, false)
func (kn *knode) searchGE(key Key) (uint64, bool, bool) {
    ks := kn.ks
    if kn.size == 0 {
        return 0, false, false
    }

    low, high := uint64(0), kn.size
    for high-low > 8 {
        mid := (high+low) / 2
        if key.Less( kn.store.fetchKey( ks[mid].kpos )) {
            high = mid
        } else {
            low = mid
        }
    }

    for i := low; i < high; i++ {
        keyb := kn.store.fetchKey( ks[i].kpos )
        docb := kn.store.fetchDocid( ks[i].dpos )
        if key.Less(keyb) == false {
            keyeq, doceq := key.Equal(keyb, docb)
            return i, keyeq, doceq
        }
    }

    return high, false, false
}

func (kn *knode) newNode( store *Store, fpos int64 ) *knode {
    max := store.maxKeys()  // always even
    newks := make([]bkey, max/2, max+1)
    newvs := make([]int64, max/2+1, max+2)
    b := block{ leaf:TRUE, size:uint64(len(kn.ks)), ks:newks, vs:newvs }
    return &knode{ block:b, store:store, fpos:fpos, dirty:true,
                   stalenodes:[]int64{} }
}

func (in *inode) newNode( store *Store, fpos int64 ) *inode {
    max := store.maxKeys()  // always even
    newks := make([]bkey, max/2, max+1)
    newvs := make([]int64, max/2+1, max+2)
    b := block{ leaf:FALSE, size:uint64(len(in.ks)), ks:newks, vs:newvs }
    kn := knode{ block:b, store:store, fpos:fpos, dirty:true,
                 stalenodes:[]int64{} }
    return &inode{ knode:kn }
}

//---- count
func (kn *knode) count() uint64 {
    return kn.size
}
func (in *inode) count() uint64 {
    n := uint64(0)
    for _, v := range in.vs {
        n += in.store.fetchNode(v).count()
    }
    return n
}

//---- front
func (kn *knode) front() []byte {
    if kn.size == 0 {
        return nil
    } else {
        return kn.store.fetchValue( kn.vs[0] )
    }
}
func (in *inode) front() []byte {
    return in.store.fetchNode( in.vs[0] ).front()
}

//---- contains
func (kn *knode) contains(key Key) bool {
    _, keyexists, _ := kn.searchGE(key)
    return keyexists
}

func (in *inode) contains(key Key) bool {
    idx, keyexists, _ := in.searchGE(key)
    if keyexists {
        return true
    }
    return in.store.fetchNode( in.vs[idx] ).contains(key)
}

//-- traverse
func (kn *knode) traverse(fun func(bkey, int64)) {
    for i := range kn.ks {
        fun( kn.ks[i], kn.vs[i] )
    }
}

func (in *inode) traverse(fun func(bkey, int64)) {
    for _, v := range in.vs {
        in.store.fetchNode( v ).traverse( fun )
    }
}

//---- insert
func (kn *knode) insert(key Key, v Value) (Node, Node, bkey) {
    index, _, exists := kn.searchGE(key)
    if exists {
        panic("We expect a delete before insert\n")
    }

    kn = kn.copyOnWrite().(*knode)
    kn.ks = kn.ks[0 : len(kn.ks) + 1]     // Make space in the key array
    copy(kn.ks[index+1:], kn.ks[index:])  // Shift existing data out of the way
    kn.ks[index] = kn.keyOf(key)

    kn.vs = kn.vs[0 : len(kn.vs) + 1]     // Make space in the value array
    copy(kn.vs[index+1:], kn.vs[index:])  // Shift existing data out of the way
    kn.vs[index] = kn.valueOf(key)

    kn.size = uint64(len(kn.ks))
    max := kn.store.maxKeys()
    if kn.size <= max {
        return kn, nil, bkey{}
    }
    spawnKn, median := kn.split()
    return kn, spawnKn, median.(bkey)
}

func (in *inode) insert(key Key, v Value) (Node, Node, bkey) {
    index, _, exists := in.searchGE(key)
    if exists {
        panic("We expect a delete before insert\n")
    }

    in = in.copyOnWrite().(*inode)
    child, spawn, median := in.store.fetchNode( in.vs[index] ).insert(key, v)
    in.vs[index] = child.getKnode().fpos
    if spawn == nil {
        return in, nil, bkey{}
    }

    in.ks = in.ks[0 : len(in.ks)+1]         // Make space in the key array
    copy( in.ks[index+1:], in.ks[index:] )  // Shift existing data out of the way
    in.ks[index] = median

    in.vs = in.vs[0 : len(in.vs)+1]         // Make space in the value array
    copy(in.vs[index+2:], in.vs[index+1:])  // Shift existing data out of the way
    in.vs[index+1] = spawn.getKnode().fpos

    in.size = uint64(len(in.ks))
    max := in.store.maxKeys()
    if in.size <= max {
        return in, nil, bkey{}
    }

    // Now this node is too full, so we have to split
    spawnIn, medianN := in.split()
    return in, spawnIn, medianN.(bkey)
}

//---- lookup
func (kn *knode) lookup(key Key, emit Emitter) {
    index, exists, _ := kn.searchGE(key)
    if exists == false {
        emit(nil)
    }
    emit( kn.store.fetchValue( kn.vs[index] ))
    for i:=index+1; i<uint64(len(kn.ks)); i++ {
        keyb := kn.store.fetchKey( kn.ks[i].kpos )
        if keyeq, _ := key.Equal(keyb, nil); keyeq {
            emit( kn.store.fetchValue( kn.vs[i] ))
        }
    }
}

func (in *inode) lookup(key Key, emit Emitter) {
    index, _, _ := in.searchGE(key)
    for i:=index; i<uint64(len(in.ks)); i++ {
        in.store.fetchKey( in.ks[i].kpos )
        in.store.fetchNode( in.vs[i] ).lookup(key, emit)
    }
}

//---- remove
func (kn *knode) remove(key Key) (Node, bool, []byte) {
    index, exists, _ := kn.searchGE(key)
    if exists == false {
        return nil, false, nil
    }

    kn = kn.copyOnWrite().(*knode)
    copy(kn.ks[index:], kn.ks[index+1:])
    kn.ks = kn.ks[0: len(kn.ks)-1]
    kn.size = uint64(len(kn.ks))

    valb := kn.store.fetchValue( kn.vs[index] )
    if kn.size >= kn.store.rebalanceThrs {
        return kn, false, valb
    }
    return kn, true, valb
}

func (in *inode) remove(key Key) (Node, bool, []byte) {
    index, _, _ := in.searchGE(key)
    child, rebalnc, valb :=  in.store.fetchNode( in.vs[index] ).remove(key)
    in = in.copyOnWrite().(*inode)
    in.vs[index] = child.getKnode().fpos

    if rebalnc == false {
        return in, false, valb
    }

    if rebalnc && index > 0 {
        left := in.store.fetchNode( in.vs[index-1] ).copyOnWrite()
        in.vs[index-1] = left.getKnode().fpos
        rebalanceLeft(in, index, child, left)
    }
    if rebalnc && index+1 < in.size {
        right := in.store.fetchNode( in.vs[index+1] ).copyOnWrite()
        in.vs[index+1] = right.getKnode().fpos
        rebalanceRight(in, index, child, right)
    }

    if in.size >= in.store.rebalanceThrs {
        return in, false, valb
    }
    return in, true, valb
}

func rebalanceLeft(in *inode, index uint64, child Node, left Node) {
    count := balance(child, left)
    median :=  in.ks[index-1]
    if count == 0 {
        left.merge(child, median)
        // The median has to go
        copy(in.ks[index-1:], in.ks[index:])
        in.ks = in.ks[0: len(in.ks)-1]
        // Child has to go
        copy(in.vs[index:], in.vs[index+1:])
        in.vs = in.vs[0: len(in.ks)]
        in.size = uint64(len(in.ks))
    } else {
        in.ks[index-1] = left.rotateRight(child, count, median)
    }
}

func rebalanceRight(in *inode, index uint64, child Node, right Node) {
    count := balance(child, right)
    median := in.ks[index]
    if count == 0 {
        child.merge(right, median)
        // The median has to go
        copy(in.ks[index:], in.ks[index+1:])
        in.ks = in.ks[0: len(in.ks)-1]
        // Right has to go
        copy(in.vs[index+1:], in.vs[index+2:])
        in.vs = in.vs[0: len(in.ks)]
        in.size = uint64(len(in.ks))
    } else {
        in.ks[index] = child.rotateLeft(right, count, median)
    }
}

func balance(to Node, node Node) uint64 {
    kn := node.getKnode()
    max := kn.store.maxKeys()
    size := kn.size + to.getKnode().size
    if float64(size) < (float64(max) * float64(0.6)) {  // FIXME magic number ??
        return 0
    } else {
        return (kn.size - kn.store.rebalanceThrs) / 2
    }
}
