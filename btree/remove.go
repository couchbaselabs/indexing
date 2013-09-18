package btree

func (kn *knode) remove(key Key) (Node, bool, []Node) {
    index, equal := kn.searchEqual(key)
    if equal == false {
        return nil, false, []Node{}
    }

    copy(kn.ks[index:], kn.ks[index+1:])
    kn.ks = kn.ks[:len(kn.ks)-1]
    kn.size = len(kn.ks)

    copy(kn.vs[index:], kn.vs[index+1:])
    kn.vs = kn.vs[:len(kn.vs)-1]

    if kn.size >= kn.store.RebalanceThrs {
        return kn, false, []Node{}
    }
    return kn, true, []Node{}
}

func (in *inode) remove(key Key) (Node, bool, []Node) {
    index, _ := in.searchEqual(key)

    // Copy on write
    stalechild := in.store.FetchNode(in.vs[index])
    child := stalechild.copyOnWrite()

    // Recursive remove
    child, rebalnc, stalenodes := child.remove(key)
    stalenodes = append(stalenodes, stalechild)
    in.vs[index] = child.getKnode().fpos

    if rebalnc == false {
        return in, false, stalenodes
    }

    // Try to rebalance from left, if there is a left node available.
    if rebalnc && (index > 0) {
        left := in.store.FetchNode(in.vs[index-1])
        _, stalerebalnc := rebalanceLeft(in, index, child, left)
        stalenodes = append(stalenodes, stalerebalnc...)
    }
    // Try to rebalance from right, if there is a right node available.
    if rebalnc && (index+1 <= in.size) {
        right := in.store.FetchNode(in.vs[index+1])
        _, stalerebalnc := rebalanceRight(in, index, child, right)
        stalenodes = append(stalenodes, stalerebalnc...)
    }

    if in.size >= in.store.RebalanceThrs {
        return in, false, stalenodes
    }
    return in, true, stalenodes
}

func rebalanceLeft(in *inode, index int, child Node, left Node) (Node, []Node) {
    count := child.balance(left)
    median :=  in.ks[index-1]
    if count == 0 { // We can merge with left child
        _, stalenodes := left.mergeRight(child, median)
        // The median has to go
        copy(in.ks[index-1:], in.ks[index:])
        in.ks = in.ks[:len(in.ks)-1]
        in.size = len(in.ks)
        // left-child has to go
        copy(in.vs[index-1:], in.vs[index:])
        in.vs = in.vs[:len(in.ks)]
        return in, stalenodes
    } else {
        staleleft := left.copyOnWrite()
        in.ks[index-1] = left.rotateRight(child, count, median)
        in.vs[index-1] = left.getKnode().fpos
        return in, []Node{staleleft}
    }
}

func rebalanceRight(in *inode, index int, child Node, right Node) (Node, []Node) {
    count := child.balance(right)
    median := in.ks[index]
    if count == 0 {
        child, stalenodes := child.mergeLeft(right, median)
        if in.size == 1 { // There is where btree-level gets reduced. crazy eh!
            stalenodes = append(stalenodes, in)
            return child, stalenodes
        } else {
            // The median has to go
            copy(in.ks[index:], in.ks[index+1:])
            in.ks = in.ks[:len(in.ks)-1]
            in.size = len(in.ks)
            // right child has to go
            copy(in.vs[index+1:], in.vs[index+2:])
            in.vs = in.vs[:len(in.ks)]
            return in, stalenodes
        }
    } else {
        staleright := right.copyOnWrite()
        in.ks[index] = child.rotateLeft(right, count, median)
        in.vs[index+1] = right.getKnode().fpos
        return in, []Node{staleright}
    }
}

func (from *knode) balance(to Node) int {
    max := from.store.maxKeys()
    size := from.size + to.getKnode().size
    if float64(size) < (float64(max) * float64(0.6)) {  // FIXME magic number ??
        return 0
    } else {
        return (from.size - from.store.RebalanceThrs) / 2
    }
}

func (from *inode) balance(to Node) int {
    return (&from.knode).balance(to)
}

// Merge `kn` into `other` Node, and return,
//  - merged `other` node,
//  - `kn` as stalenode
func (kn *knode) mergeRight(othern Node, median bkey) (Node, []Node) {
    other := othern.(*knode)
    max := kn.store.maxKeys()
    if kn.size + other.size >= max {
        panic("We cannot merge knodes now. Combined size is greater")
    }
    other.ks = other.ks[:kn.size+other.size]
    copy(other.ks[kn.size:], other.ks[:other.size])
    copy(other.ks[:kn.size], kn.ks)

    other.vs = other.vs[:kn.size+other.size+1]
    copy(other.vs[kn.size:], other.vs[:other.size+1])
    copy(other.vs[:kn.size], kn.vs) // Skip last value, which is zero

    return other, []Node{kn}
}

// Merge `in` into `other` Node, and return,
//  - merged `other` node,
//  - `in` as stalenode
func (in *inode) mergeRight(othern Node, median bkey) (Node, []Node) {
    other := othern.(*inode)
    max := in.store.maxKeys()
    if (in.size + other.size + 1) >= max {
        panic("We cannot merge inodes now. Combined size is greater")
    }
    other.ks = other.ks[:in.size+other.size+1]
    copy(other.ks[in.size+1:], other.ks[:other.size])
    copy(other.ks[:in.size], in.ks)
    other.ks[in.size] = median

    other.vs = other.vs[:in.size+other.size+2]
    copy(other.vs[in.size+1:], other.vs)
    copy(other.vs[:in.size+1], in.vs)
    return other, []Node{in}
}

// Merge `other` into `kn` Node, and return,
//  - merged `kn` node,
//  - `other` as stalenode
func (kn *knode) mergeLeft(othern Node, median bkey) (Node, []Node) {
    other := othern.(*knode)
    max := kn.store.maxKeys()
    if kn.size + other.size >= max {
        panic("We cannot merge knodes now. Combined size is greater")
    }
    kn.ks = kn.ks[:kn.size+other.size]
    copy(kn.ks[kn.size:], other.ks)

    kn.vs = kn.vs[:kn.size+other.size+1]
    copy(kn.vs[kn.size:], other.vs[:other.size+1])

    return kn, []Node{other}
}

// Merge `other` into `in` Node, and return,
//  - merged `in` node,
//  - `other` as stalenode
func (in *inode) mergeLeft(othern Node, median bkey) (Node, []Node) {
    other := othern.(*inode)
    max := in.store.maxKeys()
    if (in.size + other.size + 1) >= max {
        panic("We cannot merge inodes now. Combined size is greater")
    }
    in.ks = in.ks[:in.size+other.size+1]
    copy(in.ks[in.size+1:], other.ks[:other.size])
    in.ks[in.size] = median

    in.vs = in.vs[:in.size+other.size+2]
    copy(in.vs[in.size+1:], other.vs[:other.size+1])

    return in, []Node{other}
}

// rotate `count` entries from `left` node to `childn` node. Return the median
func (left *knode) rotateRight(childn Node, count int, median bkey) bkey {
    child := childn.(*knode)
    chlen, leftlen := len(child.ks), len(left.ks)

    // Move last `count` keys from left -> child.
    child.ks = child.ks[:chlen+count]   // First expand
    copy(child.ks[count:], child.ks[:chlen])
    copy(child.ks[:count], left.ks[leftlen-count:])
    // Blindly shrink left keys
    left.ks = left.ks[:leftlen-count]
    // Update size.
    left.size, child.size = len(left.ks), len(child.ks)

    // Move last count values from left -> child
    child.vs = child.vs[:chlen+count+1] // First expand
    copy(child.vs[count:], child.vs[:chlen+1])
    copy(child.vs[:count], left.vs[leftlen-count:leftlen])
    // Blinldy shrink left values and then append it with null pointer
    left.vs = append(left.vs[:leftlen-count], 0)
    // Return the median
    return child.ks[0]
}

// rotate `count` entries from `left` node to `childn` node. Return the median
func (left *inode) rotateRight(childn Node, count int, median bkey) bkey {
    child := childn.(*inode)
    left.ks = append(left.ks, median)
    chlen, leftlen := len(child.ks), len(left.ks)

    // Move last `count` keys from left -> child.
    child.ks = child.ks[:chlen+count]   // First expand
    copy(child.ks[count:], child.ks[:chlen])
    copy(child.ks[:count], left.ks[leftlen-count:])
    // Blindly shrink left keys
    left.ks = left.ks[:leftlen-count]
    // Update size.
    left.size, child.size = len(left.ks), len(child.ks)

    // Move last count values from left -> child
    child.vs = child.vs[:chlen+count+1] // First expand
    copy(child.vs[count:], child.vs[:chlen+1])
    copy(child.vs[:count], left.vs[leftlen-count:])
    // Pop out median
    median = left.ks[left.size-1]
    left.ks = left.ks[:left.size-1]
    left.size = len(left.ks)
    // Return the median
    return median
}

// rotate `count` entries from `right` node to `child` node. Return the median
func (child *knode) rotateLeft(rightn Node, count int, median bkey) bkey {
    right := rightn.(*knode)
    chlen := len(child.ks)

    // Move first `count` keys from right -> child.
    child.ks = child.ks[:chlen+count]  // First expand
    copy(child.ks[chlen:], right.ks[:count])
    // Don't blindly shrink right keys
    copy(right.ks, right.ks[count:])
    right.ks = right.ks[:len(right.ks)-count]
    // Update size.
    right.size, child.size = len(right.ks), len(child.ks)

    // Move last count values from right -> child
    child.vs = child.vs[:chlen+count+1]    // First expand
    copy(child.vs[chlen:], right.vs[:count])
    child.vs = append(child.vs, 0)
    // Don't blinldy shrink right values
    copy(right.vs, right.vs[count:])
    right.vs = right.vs[:len(right.vs)-count+1]
    // Return the median
    return right.ks[0]
}

func (child *inode) rotateLeft(rightn Node, count int, median bkey) bkey {
    right := rightn.(*knode)
    child.ks = append(child.ks, median)
    chlen := len(child.ks)

    // Move first `count` keys from right -> child.
    child.ks = child.ks[:chlen+count]  // First expand
    copy(child.ks[chlen:], right.ks[:count])
    // Don't blindly shrink right keys
    copy(right.ks, right.ks[count:])
    right.ks = right.ks[:len(right.ks)-count]
    // Update size.
    right.size, child.size = len(right.ks), len(child.ks)

    // Move last count values from right -> child
    child.vs = child.vs[:chlen+count+1] // First expand
    copy(child.vs[chlen+1:], right.vs[:count])
    // Don't blinldy shrink right values
    copy(right.vs, right.vs[count:])
    right.vs = right.vs[:len(right.vs)-count+1]

    // Pop out median
    median = child.ks[child.size-1]
    child.ks = child.ks[:child.size-1]
    child.size = len(child.ks)
    // Return the median
    return median
}

