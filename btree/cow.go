package btree

//---- btree wide globals
var dirtyblocks = make(map[int64]Node)

func (kn *knode) copyOnWrite() Node {
    if kn.dirty == true {
        return kn
    }
    kn.fpos, kn.stalefpos := kn.store.freelist.pop(), kn.fpos
    kn.dirty = true
    if dirtyblocks[kn.fpos] != nil {
        panic("A freeblock is found in cow cache")
    }
    dirtyblocks[kn.fpos] = kn
    return kn
}

func (in *inode) copyOnWrite() Node {
    in.knode.copyOnWrite()
    dirtyblocks[in.fpos] = in
}

func (kn *knode) split() (Node, Interface) {
    // Get a free block
    fpos := kn.store.freelist.pop()
    max := kn.store.maxKeys()  // always even

    newkn := &knode{}.newNode( kn.store, fpos )

    copy(newkn.ks[0:], kn.ks[max/2+1:])
    kn.ks = kn.ks[0:max/2+1]
    kn.size := len(kn.ks)

    copy(newkn.vs[0:], kn.vs[max/2+1:])
    kn.vs = append( kn.vs[0:max/2+1], 0 )

    dirtyblocks[fpos] = newkn
    return newkn, newkn.ks[0]
}

func (in *inode) split() (Node, Interface) {
    // Get a free block
    fpos := in.store.freelist.pop()
    max := in.store.maxKeys()  // always even

    newin := &inode{}.newNode( in.store, fpos )

    copy(newin.ks[0:], in.ks[max/2+1:])
    median := in.ks[max/2]
    in.ks := in.ks[0:max/2]
    in.size := len(in.ks)

    copy(newin.vs[0:], in.vs[max/2+1:])
    in.vs := in.vs[0:max/2+1]

    dirtyblocks[fpos] = newin
    return newin, median
}

func (kn *knode) merge(other *knode, median int) Node {
    max := kn.store.maxKeys()
    if kn.size + other.size >= max {
        panic("We cannot merge knodes now. Combined size is greater")
    }
    kn.ks := kn.ks[0:kn.size+other.size]
    copy(kn.ks[kn.size:], other.ks)

    kn.vs := kn.vs[0:kn.size+other.size+1]
    copy(kn.vs[kn.size:], other.vs)

    kn.stalenodes := []int64{other.fpos}
    return kn
}

func (kn *knode) rotateRight(child *knode, count int, median key) key {
    chlen := len(child.ks)
    knlen := len(kn.ks)
    // Move last `count` keys from kn -> child.
    child.ks = child.ks[:chlen+count]   // First expand
    copy( child.ks[count:], child.ks[0:chlen] )
    copy( child.ks[:count], kn.ks[knlen-count:] )
    // Blindly shrink kn keys
    kn.ks = kn.ks[:knlen-count]
    // Update size.
    kn.size, child.size = len(kn.ks), len(child.ks)
    // Move last count values from kn -> child
    child.vs = child.vs[:chlen+count+1] // First expand
    copy( child.vs[count:], child.vs[0:chlen+1] )
    copy( child.vs[:count], kn.vs[knlen-count+1:] )
    // Blinldy shrink kn values and then append it with null pointer
    kn.vs = append( kn.vs[:knlen-count+1], 0 )
    // Return the median
    return child.ks[0]
}

func (child *knode) rotateLeft(kn *knode, count int, median key) key {
    chlen := len(child.ks)
    knlen := len(kn.ks)
    // Move first `count` keys from kn -> child.
    child.ks = child.ks[:chlen+count]  // First expand
    copy( child.ks[chlen:], kn.ks[0:count] )
    // Blindly shrink kn keys
    kn.ks = kn.ks[count:]
    // Update size.
    kn.size, child.size = len(kn.ks), len(child.ks)
    // Move last count values from kn -> child
    child.vs = child.vs[:chlen+count]    // First expand
    copy( child.vs[chlen:], kn.vs[0:count] )
    child.vs = append(child.vs, 0)
    // Blinldy shrink kn values and then append it with null pointer
    kn.vs = kn.vs[count:]
    // Return the median
    return kn.ks[0]
}

func (in *inode) merge(other *inode, median int) Node {
    max := in.store.maxKeys()
    if (in.size + other.size + 1) >= max {
        panic("We cannot merge inodes now. Combined size is greater")
    }
    in.ks := in.ks[0:in.size+other.size+1]
    in.ks[in.size] = median
    copy(in.ks[in.size+1:], other.ks)

    in.vs := in.ks[0:in.size+other.size+2]
    copy(in.vs[in.size+1:], other.vs)
    in.stalenodes := []int64{other.fpos}
    return in
}

func (in *inode) rotateRight(child *inode, count int, median key) key {
    in.ks = append(in.ks, median)
    chlen := len(child.ks)
    inlen := len(in.ks)
    // Move last `count` keys from in -> child.
    child.ks = child.ks[:chlen+count]   // First expand
    copy( child.ks[count:], child.ks[0:chlen] )
    copy( child.ks[:count], in.ks[knlen-count:] )
    // Blindly shrink in keys
    in.ks = in.ks[:inlen-count]
    // Update size.
    in.size, child.size = len(in.ks), len(child.ks)
    // Move last count values from in -> child
    child.vs = child.vs[:chlen+count+1] // First expand
    copy( child.vs[count:], child.vs[0:chlen+1] )
    copy( child.vs[:count], in.vs[inlen-count+1:] )
    // Pop out median
    median = in.ks[in.size-1]
    in.ks = in.ks[:in.size-1]
    in.size = len(in.ks)
    // Return the median
    return median
}

func (child *inode) rotateLeft(in *inode, count int, median key) key {
    child.ks = append(child.ks, median)
    chlen := len(child.ks)
    knlen := len(kn.ks)
    // Move first `count` keys from in -> child.
    child.ks = child.ks[:chlen+count]  // First expand
    copy( child.ks[chlen:], in.ks[0:count] )
    // Blindly shrink in keys
    in.ks = in.ks[count:]
    // Update size.
    in.size, child.size = len(in.ks), len(child.ks)
    // Move last count values from in -> child
    child.vs = child.vs[:chlen+count]    // First expand
    copy( child.vs[chlen:], in.vs[0:count] )
    // Blinldy shrink in values and then append it with null pointer
    in.vs = in.vs[count:]
    // Pop out median
    median = child.ks[child.size-1]
    child.ks = child.ks[:child.size-1]
    child.size = len(child.ks)
    // Return the median
    return median
}

func (kn *knode) keyOf(k Key) key {
    return key{
        ctrl: k.Control()
        kpos: kn.store.appendKey( k.Bytes() )
        dpos: kn.store.appendDocid( k.Docid() )
    }
}

func (kn *knode) valueOf(v Value) int64 {
    return kn.store.appendValue( v.Bytes() )
}

