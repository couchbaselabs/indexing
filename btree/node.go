package btree

type knode struct { // keynode
    block   // embedded structure 

    // Book-keeping fields
    store *Store    // reference to Index data-structure
    fpos int64      // file-offset where this block resides

    // Fields are applicable when node is mapped under `dirtyblocks`
    dirty bool          // if dirty, `cow` and `stalenodes` becomes valid.
    cow [2]int64        // Copy-on-write
                        //    0 - fpos of old block
                        //    1 - fpos of new block.
}

type inode struct { // intermediate node
    knode
    dirtychild uint64   // number of dirty children under this node.
    stalenodes []Node   // Child nodes that went stale due to rebalancing.
}

type Key interface {
    Bytes() []byte
    Less( []byte ) bool
    Equal( []byte ) bool
    Docid() []byte
}

type Value interface {
    Bytes() []byte
}

type Node interface {
    // Return number of entries on all the leaf nodes from this Node.
    count() uint64

    // Returns the lowest element in the tree.
    front() []byte

    // Returns true iff this tree contains the value.
    contains(Key) bool

    // Passes all of the data in this node and its children through the cannel in proper order.
    traverse(func(key, int64))

    // Inserts the value into the appropriate place in the tree, rebalancing
    // as necessary.  The first return value specifies if the value was
    // actually added (i.e. if it wasn't already there).  If a new node is 
    // created it is returned along with a separator value.

    //insert(Key, Value) (bool, Node, Key)

    // Removes the value from the tree, rebalancing as necessary.  Returns true iff an element was
    // actually deleted.

    //remove(T, Less) bool

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
}

// Make this leaf-node a prestine copy of the disk version, this is case when
// the node was just fetched from disk or when it was just flushed to disk.
func (kn *knode) prestine() Node {
    kn.dirty = false
    return kn
}

// Returns the index of the smallest value that is not less than `key`, and
// whether or not it equals `key`. If there are no elements greater than or
// equal to `key` then it returns (len(node.key), false)
func (kn *knode) searchGE(key Key) (uint64, bool) {
    ks := kn.ks
    if kn.size == 0 {
        return 0, false
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
        bin := kn.store.fetchKey( ks[i].kpos )
        if key.Less(bin) == false {
            return i, key.Equal(bin)
        }
    }

    return high, false
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
        return kn.store.fetchValue( kn.block.vs[0] )
    }
}
func (in *inode) front() []byte {
    return in.store.fetchNode( in.vs[0] ).front()
}

//--- contains
func (kn *knode) contains(key Key) bool {
    _, exists := kn.searchGE(key)
    return exists
}

func (in *inode) contains(key Key) bool {
    vidx, exists := in.searchGE(key)
    if exists {
        return true
    }
    return in.store.fetchNode( in.vs[vidx] ).contains(key)
}

//--- traverse
func (kn *knode) traverse(fun func(key, int64)) {
    for i := range kn.ks {
        fun( kn.ks[i], kn.vs[i] )
    }
}

func (in *inode) traverse(fun func(key, int64)) {
    for _, v := range in.vs {
        in.store.fetchNode( v ).traverse( fun )
    }
}


//---- insert
