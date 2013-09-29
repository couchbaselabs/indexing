package btree

import (
    "fmt"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

func (kn *knode) remove(key Key, mv *MV) (Node, bool) {
    index, kfpos, dfpos := kn.searchGE(key, true)
    if (kfpos < 0) && (dfpos < 0) {
        return kn, false
    }

    copy(kn.ks[index:], kn.ks[index+1:])
    copy(kn.ds[index:], kn.ds[index+1:])
    kn.ks = kn.ks[:len(kn.ks)-1]
    kn.ds = kn.ds[:len(kn.ds)-1]
    kn.size = len(kn.ks)

    copy(kn.vs[index:], kn.vs[index+1:])
    kn.vs = kn.vs[:len(kn.vs)-1]

    if kn.size >= kn.store.RebalanceThrs {
        return kn, false
    }
    return kn, true
}

func (in *inode) remove(key Key, mv *MV) (Node, bool) {
    index, _, _ := in.searchGE(key, true)

    // Copy on write
    stalechild := in.store.FetchMVCCNode(in.vs[index])
    child := stalechild.copyOnWrite()
    mv.stales = append(mv.stales, stalechild)
    mv.commits = append(mv.commits, child)

    // Recursive remove
    child, rebalnc := child.remove(key, mv)
    in.vs[index] = child.getKnode().fpos

    if rebalnc == false {
        return in, false
    }

    // Try to rebalance from left, if there is a left node available.
    if rebalnc && (index > 0) {
        left := in.store.FetchMVCCNode(in.vs[index-1])
        rebalanceLeft(in, index, child, left, mv)
    }
    // Try to rebalance from right, if there is a right node available.
    if rebalnc && (index+1 <= in.size) {
        right := in.store.FetchMVCCNode(in.vs[index+1])
        rebalanceRight(in, index, child, right, mv)
    }

    if in.size >= in.store.RebalanceThrs {
        return in, false
    }
    return in, true
}

func rebalanceLeft(in *inode, index int, child Node, left Node, mv *MV) Node {
    count := child.balance(left)
    mk, md :=  in.ks[index-1], in.ds[index-1]
    if count == 0 { // We can merge with left child
        _, stalenodes := left.mergeRight(child, mk, md)
        // The median has to go
        copy(in.ks[index-1:], in.ks[index:])
        copy(in.ds[index-1:], in.ds[index:])
        in.ks = in.ks[:len(in.ks)-1]
        in.ds = in.ds[:len(in.ds)-1]
        in.size = len(in.ks)
        // left-child has to go
        copy(in.vs[index-1:], in.vs[index:])
        in.vs = in.vs[:len(in.ks)]
        mv.stales = append(mv.stales, stalenodes...)
        return in
    } else {
        mv.stales = append(mv.stales, left)
        left := left.copyOnWrite()
        mv.commits = append(mv.commits, left)
        in.ks[index-1], in.ds[index-1] = left.rotateRight(child, count, mk, md)
        in.vs[index-1] = left.getKnode().fpos
        return in
    }
}

func rebalanceRight(in *inode, index int, child Node, right Node, mv *MV) Node {
    count := child.balance(right)
    mk, md := in.ks[index], in.ds[index]
    if count == 0 {
        child, stalenodes := child.mergeLeft(right, mk, md)
        mv.stales = append(mv.stales, stalenodes...)
        if in.size == 1 { // There is where btree-level gets reduced. crazy eh!
            mv.stales = append(mv.stales, in)
            return child
        } else {
            // The median has to go
            copy(in.ks[index:], in.ks[index+1:])
            copy(in.ds[index:], in.ds[index+1:])
            in.ks = in.ks[:len(in.ks)-1]
            in.ds = in.ds[:len(in.ds)-1]
            in.size = len(in.ks)
            // right child has to go
            copy(in.vs[index+1:], in.vs[index+2:])
            in.vs = in.vs[:len(in.ks)]
            return in
        }
    } else {
        mv.stales = append(mv.stales, right)
        right := right.copyOnWrite()
        mv.commits = append(mv.commits, right)
        in.ks[index], in.ds[index] = child.rotateLeft(right, count, mk, md)
        in.vs[index+1] = right.getKnode().fpos
        return in
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
    copy(other.vs[:kn.size], kn.vs) // Skip last value, which is zero

    return other, []Node{kn}
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
    return other, []Node{in}
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
    copy(kn.ks[kn.size:], other.ks)
    copy(kn.ds[kn.size:], other.ds)

    kn.vs = kn.vs[:kn.size+other.size+1]
    copy(kn.vs[kn.size:], other.vs[:other.size+1])

    return kn, []Node{other}
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

    return in, []Node{other}
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
    // Return the median
    return child.ks[0], child.ds[0]
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
    copy(child.vs[:count], left.vs[leftlen-count:])
    // Pop out median
    mk, md = left.ks[left.size-1], left.ds[left.size-1]
    left.ks = left.ks[:left.size-1]
    left.ds = left.ds[:left.size-1]
    left.size = len(left.ks)
    // Return the median
    return mk, md
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
    child.vs = append(child.vs, 0)
    // Don't blinldy shrink right values
    copy(right.vs, right.vs[count:])
    right.vs = right.vs[:len(right.vs)-count+1]
    // Return the median
    return right.ks[0], right.ds[0]
}

// rotate `count` entries from right `n` node to `child` node. Return median
func (child *inode) rotateLeft(n Node, count int, mk, md int64) (int64, int64) {
    right := n.(*knode)
    child.ks = append(child.ks, mk)
    child.ds = append(child.ds, md)
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
    child.vs = child.vs[:chlen+count+1] // First expand
    copy(child.vs[chlen+1:], right.vs[:count])
    // Don't blinldy shrink right values
    copy(right.vs, right.vs[count:])
    right.vs = right.vs[:len(right.vs)-count+1]

    // Pop out median
    mk, md = child.ks[child.size-1], child.ds[child.size-1]
    child.ks = child.ks[:child.size-1]
    child.ds = child.ds[:child.size-1]
    child.size = len(child.ks)
    // Return the median
    return mk, md
}

