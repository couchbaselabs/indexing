package btree

import (
    "fmt"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

// Return the mutated node along with a boolean that says whether a rebalance
// is required or not.
func (kn *knode) remove(key Key, mv *MV) (Node, bool, int64, int64) {
    index, equal := kn.searchEqual(key)
    mk, md := int64(-1), int64(-1)
    if equal == false {
        return kn, false, mk, md
    }

    copy(kn.ks[index:], kn.ks[index+1:])
    copy(kn.ds[index:], kn.ds[index+1:])
    kn.ks = kn.ks[:len(kn.ks)-1]
    kn.ds = kn.ds[:len(kn.ds)-1]
    kn.size = len(kn.ks)

    copy(kn.vs[index:], kn.vs[index+1:])
    kn.vs = kn.vs[:len(kn.ks)+1]

    //Debug
    if len(kn.vs) != len(kn.ks)+1 {
        panic("Bomb")
    }

    // If first entry in the leaf node is always a separator key in
    // intermediate node.
    if index == 0  &&  kn.size > 0 {
        mk, md = kn.ks[0], kn.ds[0]
    }

    if kn.size >= kn.store.RebalanceThrs {
        return kn, false, mk, md
    }
    return kn, true, mk, md
}

// Return the mutated node along with a boolean that says whether a rebalance
// is required or not.
func (in *inode) remove(key Key, mv *MV) (Node, bool, int64, int64) {
    index, equal := in.searchEqual(key)

    // Copy on write
    stalechild := in.store.FetchMVCCNode(in.vs[index])
    child := stalechild.copyOnWrite()
    mv.stales = append(mv.stales, stalechild)
    mv.commits = append(mv.commits, child)

    // Recursive remove
    child, rebalnc, mk, md := child.remove(key, mv)
    if equal {
        if mk < 0  ||  md < 0 {
            panic("separator cannot be less than zero")
        }
        if index < 1 {
            panic("cannot be less than 1")
        }
        in.ks[index-1], in.ds[index-1] = mk, md
    }
    in.vs[index] = child.getKnode().fpos

    if rebalnc == false {
        return in, false, mk, md
    }

    var node Node = in

    // FIXME : In the below rebalance logic, we are fetching the left and right
    // node and discarding it if they are of other type from child. Can this
    // be avoided and optimized ?

    // Try to rebalance from left, if there is a left node available.
    if rebalnc && (index > 0) {
        left := in.store.FetchMVCCNode(in.vs[index-1])
        if canRebalance(child, left) {
            node, index = in.rebalanceLeft(index, child, left, mv)
        }
    }
    // Try to rebalance from right, if there is a right node available.
    if rebalnc  &&  (index >= 0)  &&  (index+1 <= in.size) {
        right := in.store.FetchMVCCNode(in.vs[index+1])
        if canRebalance(child, right) {
            node, index = in.rebalanceRight(index, child, right, mv)
        }
    }

    if node.getKnode().size >= in.store.RebalanceThrs {
        return node, false, mk, md
    }
    return node, true, mk, md
}

func (in *inode) rebalanceLeft(index int, child Node, left Node, mv *MV) (Node, int) {
    count := left.balance(child);

    mk, md :=  in.ks[index-1], in.ds[index-1]
    if count == 0 { // We can merge with left child
        _, stalenodes := left.mergeRight(child, mk, md)
        mv.stales = append(mv.stales, stalenodes...)
        if in.size == 1 { // There is where btree-level gets reduced. crazy eh!
            mv.stales = append(mv.stales, in)
            return child, -1
        } else {
            // The median aka seperator has to go
            copy(in.ks[index-1:], in.ks[index:])
            copy(in.ds[index-1:], in.ds[index:])
            in.ks = in.ks[:len(in.ks)-1]
            in.ds = in.ds[:len(in.ds)-1]
            in.size = len(in.ks)
            // left-child has to go
            copy(in.vs[index-1:], in.vs[index:])
            in.vs = in.vs[:len(in.ks)+1]
            return in, index-1
        }
    } else {
        mv.stales = append(mv.stales, left)
        left := left.copyOnWrite()
        mv.commits = append(mv.commits, left)
        in.ks[index-1], in.ds[index-1] = left.rotateRight(child, count, mk, md)
        in.vs[index-1] = left.getKnode().fpos
        return in, index
    }
}

func (in *inode) rebalanceRight(index int, child Node, right Node, mv *MV) (Node, int) {
    count := right.balance(child)

    mk, md := in.ks[index], in.ds[index]
    if count == 0 {
        _, stalenodes := child.mergeLeft(right, mk, md)
        mv.stales = append(mv.stales, stalenodes...)
        if in.size == 1 { // There is where btree-level gets reduced. crazy eh!
            mv.stales = append(mv.stales, in)
            return child, -1
        } else {
            // The median aka separator has to go
            copy(in.ks[index:], in.ks[index+1:])
            copy(in.ds[index:], in.ds[index+1:])
            in.ks = in.ks[:len(in.ks)-1]
            in.ds = in.ds[:len(in.ds)-1]
            in.size = len(in.ks)
            // right child has to go
            copy(in.vs[index+1:], in.vs[index+2:])
            in.vs = in.vs[:len(in.ks)+1]
            return in, index
        }
    } else {
        mv.stales = append(mv.stales, right)
        right := right.copyOnWrite()
        mv.commits = append(mv.commits, right)
        in.ks[index], in.ds[index] = child.rotateLeft(right, count, mk, md)
        in.vs[index+1] = right.getKnode().fpos
        return in, index
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

// Merge `kn` into `other` Node, and return,
//  - merged `other` node,
//  - `kn` as stalenode
func (kn *knode) mergeRight(othern Node, mk, md int64) (Node, []Node) {
    other := othern.(*knode)
    max := kn.store.maxKeys()
    if kn.size + other.size >= max {
        panic("We cannot merge knodes now. Combined size is greater")
    }
    other.ks = other.ks[:kn.size+other.size]
    other.ds = other.ds[:kn.size+other.size]
    copy(other.ks[kn.size:], other.ks[:other.size])
    copy(other.ds[kn.size:], other.ds[:other.size])
    copy(other.ks[:kn.size], kn.ks)
    copy(other.ds[:kn.size], kn.ds)

    other.vs = other.vs[:kn.size+other.size+1]
    copy(other.vs[kn.size:], other.vs[:other.size+1])
    copy(other.vs[:kn.size], kn.vs[:kn.size]) // Skip last value, which is zero
    other.size = len(other.ks)

    //Debug
    if len(other.vs) != len(other.ks)+1 {
        panic("Bomb")
    }

    kn.store.wstore.countMergeRight += 1
    return other, []Node{kn}
}

// rotate `count` entries from `left` node to child `n` node. Return the median
func (left *knode) rotateRight(n Node, count int, mk, md int64) (int64, int64) {
    child := n.(*knode)
    chlen, leftlen := len(child.ks), len(left.ks)

    // Move last `count` keys from left -> child.
    child.ks = child.ks[:chlen+count]   // First expand
    child.ds = child.ds[:chlen+count]   // First expand
    copy(child.ks[count:], child.ks[:chlen])
    copy(child.ds[count:], child.ds[:chlen])
    copy(child.ks[:count], left.ks[leftlen-count:])
    copy(child.ds[:count], left.ds[leftlen-count:])
    // Blindly shrink left keys
    left.ks = left.ks[:leftlen-count]
    left.ds = left.ds[:leftlen-count]
    // Update size.
    left.size, child.size = len(left.ks), len(child.ks)

    // Move last count values from left -> child
    child.vs = child.vs[:chlen+count+1] // First expand
    copy(child.vs[count:], child.vs[:chlen+1])
    copy(child.vs[:count], left.vs[leftlen-count:leftlen])
    // Blinldy shrink left values and then append it with null pointer
    left.vs = append(left.vs[:leftlen-count], 0)

    //Debug
    if (len(left.vs) != len(left.ks)+1) || (len(child.vs) != len(child.ks)+1) {
        panic("Bomb")
    }

    // Return the median
    child.store.wstore.countRotateRight += 1
    return child.ks[0], child.ds[0]
}

// Merge `other` into `kn` Node, and return,
//  - merged `kn` node,
//  - `other` as stalenode
func (kn *knode) mergeLeft(othern Node, mk, md int64) (Node, []Node) {
    other := othern.(*knode)
    max := kn.store.maxKeys()
    if kn.size + other.size >= max {
        panic("We cannot merge knodes now. Combined size is greater")
    }
    kn.ks = kn.ks[:kn.size+other.size]
    kn.ds = kn.ds[:kn.size+other.size]
    copy(kn.ks[kn.size:], other.ks[:other.size])
    copy(kn.ds[kn.size:], other.ds[:other.size])

    kn.vs = kn.vs[:kn.size+other.size+1]
    copy(kn.vs[kn.size:], other.vs[:other.size+1])
    kn.size = len(kn.ks)

    //Debug
    if len(kn.vs) != len(kn.ks)+1 {
        panic("Bomb")
    }

    kn.store.wstore.countMergeLeft += 1
    return kn, []Node{other}
}

// rotate `count` entries from right `n` node to `child` node. Return median
func (child *knode) rotateLeft(n Node, count int, mk, md int64) (int64, int64) {
    right := n.(*knode)
    chlen := len(child.ks)

    // Move first `count` keys from right -> child.
    child.ks = child.ks[:chlen+count]  // First expand
    child.ds = child.ds[:chlen+count]  // First expand
    copy(child.ks[chlen:], right.ks[:count])
    copy(child.ds[chlen:], right.ds[:count])
    // Don't blindly shrink right keys
    copy(right.ks, right.ks[count:])
    copy(right.ds, right.ds[count:])
    right.ks = right.ks[:len(right.ks)-count]
    right.ds = right.ds[:len(right.ds)-count]
    // Update size.
    right.size, child.size = len(right.ks), len(child.ks)

    // Move last count values from right -> child
    child.vs = child.vs[:chlen+count+1]    // First expand
    copy(child.vs[chlen:], right.vs[:count])
    child.vs[chlen+count] = 0
    // Don't blinldy shrink right values
    copy(right.vs, right.vs[count:])
    right.vs = right.vs[:len(right.vs)-count]

    //Debug
    if (len(child.vs) != len(child.ks)+1) {
        panic("Bomb")
    }

    // Return the median
    child.store.wstore.countRotateLeft += 1
    return right.ks[0], right.ds[0]
}

// Merge `in` into `other` Node, and return,
//  - merged `other` node,
//  - `in` as stalenode
func (in *inode) mergeRight(othern Node, mk, md int64) (Node, []Node) {
    other := othern.(*inode)
    max := in.store.maxKeys()
    if (in.size + other.size + 1) >= max {
        panic("We cannot merge inodes now. Combined size is greater")
    }
    other.ks = other.ks[:in.size+other.size+1]
    other.ds = other.ds[:in.size+other.size+1]
    copy(other.ks[in.size+1:], other.ks[:other.size])
    copy(other.ds[in.size+1:], other.ds[:other.size])
    copy(other.ks[:in.size], in.ks)
    copy(other.ds[:in.size], in.ds)
    other.ks[in.size], other.ds[in.size] = mk, md

    other.vs = other.vs[:in.size+other.size+2]
    copy(other.vs[in.size+1:], other.vs)
    copy(other.vs[:in.size+1], in.vs)
    other.size = len(other.ks)

    in.store.wstore.countMergeRight += 1
    return other, []Node{in}
}

// rotate `count` entries from `left` node to child `n` node. Return the median
func (left *inode) rotateRight(n Node, count int, mk, md int64) (int64, int64) {
    child := n.(*inode)
    left.ks = append(left.ks, mk)
    left.ds = append(left.ds, md)
    chlen, leftlen := len(child.ks), len(left.ks)

    // Move last `count` keys from left -> child.
    child.ks = child.ks[:chlen+count]   // First expand
    child.ds = child.ds[:chlen+count]   // First expand
    copy(child.ks[count:], child.ks[:chlen])
    copy(child.ds[count:], child.ds[:chlen])
    copy(child.ks[:count], left.ks[leftlen-count:])
    copy(child.ds[:count], left.ds[leftlen-count:])
    // Blindly shrink left keys
    left.ks = left.ks[:leftlen-count]
    left.ds = left.ds[:leftlen-count]
    // Update size.
    left.size, child.size = len(left.ks), len(child.ks)

    // Move last count values from left -> child
    child.vs = child.vs[:chlen+count+1] // First expand
    copy(child.vs[count:], child.vs[:chlen+1])
    copy(child.vs[:count], left.vs[len(left.vs)-count:])
    left.vs = left.vs[:len(left.vs)-count]
    // Pop out median
    mk, md = left.ks[left.size-1], left.ds[left.size-1]
    left.ks = left.ks[:left.size-1]
    left.ds = left.ds[:left.size-1]
    left.size = len(left.ks)
    // Return the median
    child.store.wstore.countRotateRight += 1
    return mk, md
}

// Merge `other` into `in` Node, and return,
//  - merged `in` node,
//  - `other` as stalenode
func (in *inode) mergeLeft(othern Node, mk, md int64) (Node, []Node) {
    other := othern.(*inode)
    max := in.store.maxKeys()
    if (in.size + other.size + 1) >= max {
        panic("We cannot merge inodes now. Combined size is greater")
    }
    in.ks = in.ks[:in.size+other.size+1]
    in.ds = in.ds[:in.size+other.size+1]
    copy(in.ks[in.size+1:], other.ks[:other.size])
    copy(in.ds[in.size+1:], other.ds[:other.size])
    in.ks[in.size], in.ds[in.size] = mk, md

    in.vs = in.vs[:in.size+other.size+2]
    copy(in.vs[in.size+1:], other.vs[:other.size+1])
    in.size = len(in.ks)

    in.store.wstore.countMergeLeft += 1
    return in, []Node{other}
}

// rotate `count` entries from right `n` node to `child` node. Return median
func (child *inode) rotateLeft(n Node, count int, mk, md int64) (int64, int64) {
    right := n.(*inode)
    child.ks = append(child.ks, mk)
    child.ds = append(child.ds, md)
    chlen := len(child.ks)
    rlen := len(right.ks)

    // Move first `count` keys from right -> child.
    child.ks = child.ks[:chlen+count]  // First expand
    child.ds = child.ds[:chlen+count]  // First expand
    copy(child.ks[chlen:], right.ks[:count])
    copy(child.ds[chlen:], right.ds[:count])
    // Don't blindly shrink right keys
    copy(right.ks, right.ks[count:])
    copy(right.ds, right.ds[count:])
    right.ks = right.ks[:rlen-count]
    right.ds = right.ds[:rlen-count]
    // Update size.
    right.size, child.size = len(right.ks), len(child.ks)

    // Move last count values from right -> child
    child.vs = child.vs[:chlen+count] // First expand
    copy(child.vs[chlen:], right.vs[:count])
    // Don't blinldy shrink right values
    copy(right.vs, right.vs[count:])
    right.vs = right.vs[:rlen-count+1]

    // Pop out median
    mk, md = child.ks[child.size-1], child.ds[child.size-1]
    child.ks = child.ks[:child.size-1]
    child.ds = child.ds[:child.size-1]
    child.size = len(child.ks)
    // Return the median
    child.store.wstore.countRotateLeft += 1
    return mk, md
}

func canRebalance(n Node, m Node) bool {
    var rc bool
    if _, ok := n.(*knode); ok {
        if _, ok = m.(*knode); ok {
            rc = true
        }
    } else {
        if _, ok = m.(*inode); ok {
            rc = true
        }
    }
    return rc
}
