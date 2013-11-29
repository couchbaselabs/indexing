// Copyright 2010 Petar Maymounkov. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// A Left-Leaning Red-Black (LLRB) implementation of 2-3 balanced binary search trees,
// based on the following work:
//
//   http://www.cs.princeton.edu/~rs/talks/LLRB/08Penn.pdf
//   http://www.cs.princeton.edu/~rs/talks/LLRB/LLRB.pdf
//   http://www.cs.princeton.edu/~rs/talks/LLRB/Java/RedBlackBST.java
//
//  2-3 trees (and the run-time equivalent 2-3-4 trees) are the de facto
//  standard BST algoritms found in implementations of Python, Java, and other
//  libraries. The LLRB implementation of 2-3 trees is a recent improvement
//  on the traditional implementation, observed and documented by Robert
//  Sedgewick.

package llrb

import (
    "github.com/couchbaselabs/indexing/api"
)

// Tree is a Left-Leaning Red-Black (LLRB) implementation of 2-3 trees
type LLRB struct {
    count int
    root  *Node
}

type Node struct {
    api.Key
    api.Value
    Left, Right *Node // Pointers to left and right child nodes
    Black       bool  // If set, the color of the link (incoming from the parent) is black
    // In the LLRB, new nodes are always red, hence the zero-value for node
}

type Iterator func(api.Key, api.Value) bool

func less(x, y api.Key) bool {
    if x == api.PInf {
        return false
    }
    if x == api.NInf {
        return true
    }
    return x.Less(y)
}

// New() allocates a new tree
func New() *LLRB {
    return &LLRB{}
}

// SetRoot sets the root node of the tree.
// It is intended to be used by functions that deserialize the tree.
func (t *LLRB) SetRoot(r *Node) {
    t.root = r
}

// Root returns the root node of the tree.
// It is intended to be used by functions that serialize the tree.
func (t *LLRB) Root() *Node {
    return t.root
}

// Len returns the number of nodes in the tree.
func (t *LLRB) Len() int {
    return t.count
}

// Has returns true if the tree contains an element whose order is the same as that of key.
func (t *LLRB) Has(key api.Key) (rc bool) {
    t.Get(key, func(key api.Key, value api.Value) bool {
        if key != nil {
            rc = true
        }
        return false
    })
    return rc
}

// Get retrieves an element from the tree whose order is the same as that of key.
func (t *LLRB) Get(key api.Key, iterator Iterator) {
    h := t.root
    for h != nil {
        switch {
        case less(key, h.Key):
            h = h.Left
        case less(h.Key, key):
            h = h.Right
        default:
            iterator(h.Key, h.Value)
            t.ascendRange(h, key, key, iterator)
            return
        }
    }
}

// Min returns the minimum element in the tree.
func (t *LLRB) Min() api.KV {
    h := t.root
    if h == nil {
        return [2]interface{}{nil, nil}
    }
    for h.Left != nil {
        h = h.Left
    }
    return [2]interface{}{h.Key, h.Value}
}

// Max returns the maximum element in the tree.
func (t *LLRB) Max() api.KV {
    h := t.root
    if h == nil {
        return [2]interface{}{nil, nil}
    }
    for h.Right != nil {
        h = h.Right
    }
    return [2]interface{}{h.Key, h.Value}
}

func (t *LLRB) InsertMany(kvs ...api.KV) {
    for _, kv := range kvs {
        key := kv[0].(api.Key)
        value := kv[1].(api.Value)
        t.Insert(key, value)
    }
}

func (t *LLRB) AddMany(kvs ...api.KV) {
    for _, kv := range kvs {
        key := kv[0].(api.Key)
        value := kv[1].(api.Value)
        t.Add(key, value)
    }
}

// Insert inserts key into the tree. If an existing
// element has the same order, it is removed from the tree and returned.
func (t *LLRB) Insert(key api.Key, value api.Value) api.KV {
    if key == nil {
        panic("inserting nil key")
    }

    var replaced api.KV
    t.root, replaced = t.insert(t.root, key, value)
    t.root.Black = true
    if replaced[0] == nil {
        t.count++
    }
    return replaced
}

func (t *LLRB) insert(h *Node, key api.Key, value api.Value) (*Node, api.KV) {
    if h == nil {
        return newNode(key, value), [2]interface{}{nil, nil}
    }

    var replaced api.KV
    h = walkDownRot23(h)
    if less(key, h.Key) { // BUG
        h.Left, replaced = t.insert(h.Left, key, value)
    } else if less(h.Key, key) {
        h.Right, replaced = t.insert(h.Right, key, value)
    } else {
        replaced, h.Key = [2]interface{}{h.Key, h.Value}, key
    }
    h = walkUpRot23(h)

    return h, replaced
}

// Add inserts key into the tree. If an existing
// element has the same order, both elements remain in the tree.
func (t *LLRB) Add(key api.Key, value api.Value) {
    if key == nil {
        panic("inserting nil key")
    }
    t.root = t.add(t.root, key, value)
    t.root.Black = true
    t.count++
}

func (t *LLRB) add(h *Node, key api.Key, value api.Value) *Node {
    if h == nil {
        return newNode(key, value)
    }

    h = walkDownRot23(h)
    if less(key, h.Key) {
        h.Left = t.add(h.Left, key, value)
    } else {
        h.Right = t.add(h.Right, key, value)
    }
    return walkUpRot23(h)
}

// Rotation driver routines for 2-3 algorithm
func walkDownRot23(h *Node) *Node {
    return h
}

func walkUpRot23(h *Node) *Node {
    if isRed(h.Right) && !isRed(h.Left) {
        h = rotateLeft(h)
    }
    if isRed(h.Left) && isRed(h.Left.Left) {
        h = rotateRight(h)
    }
    if isRed(h.Left) && isRed(h.Right) {
        flip(h)
    }
    return h
}

// Rotation driver routines for 2-3-4 algorithm

func walkDownRot234(h *Node) *Node {
    if isRed(h.Left) && isRed(h.Right) {
        flip(h)
    }
    return h
}

func walkUpRot234(h *Node) *Node {
    if isRed(h.Right) && !isRed(h.Left) {
        h = rotateLeft(h)
    }
    if isRed(h.Left) && isRed(h.Left.Left) {
        h = rotateRight(h)
    }
    return h
}

// DeleteMin deletes the minimum element in the tree and returns the
// deleted key or nil otherwise.
func (t *LLRB) DeleteMin() api.KV {
    var deleted api.KV
    t.root, deleted = deleteMin(t.root)
    if t.root != nil {
        t.root.Black = true
    }
    if deleted[0] != nil {
        t.count--
    }
    return deleted
}

// deleteMin code for LLRB 2-3 trees
func deleteMin(h *Node) (*Node, api.KV) {
    if h == nil {
        return nil, [2]interface{}{nil, nil}
    }
    if h.Left == nil {
        return nil, [2]interface{}{h.Key, h.Value}
    }
    if !isRed(h.Left) && !isRed(h.Left.Left) {
        h = moveRedLeft(h)
    }

    var deleted api.KV
    h.Left, deleted = deleteMin(h.Left)

    return fixUp(h), deleted
}

// DeleteMax deletes the maximum element in the tree and returns
// the deleted key or nil otherwise
func (t *LLRB) DeleteMax() api.KV {
    var deleted api.KV
    t.root, deleted = deleteMax(t.root)
    if t.root != nil {
        t.root.Black = true
    }
    if deleted[0] != nil {
        t.count--
    }
    return deleted
}

func deleteMax(h *Node) (*Node, api.KV) {
    if h == nil {
        return nil, [2]interface{}{nil, nil}
    }
    if isRed(h.Left) {
        h = rotateRight(h)
    }
    if h.Right == nil {
        return nil, [2]interface{}{h.Key, h.Value}
    }
    if !isRed(h.Right) && !isRed(h.Right.Left) {
        h = moveRedRight(h)
    }
    var deleted api.KV
    h.Right, deleted = deleteMax(h.Right)

    return fixUp(h), deleted
}

// Delete deletes an key from the tree whose key equals key.
// The deleted key is return, otherwise nil is returned.
func (t *LLRB) Delete(key api.Key) api.KV {
    var deleted api.KV
    t.root, deleted = t.delete(t.root, key)
    if t.root != nil {
        t.root.Black = true
    }
    if deleted[0] != nil {
        t.count--
    }
    return deleted
}

func (t *LLRB) delete(h *Node, key api.Key) (*Node, api.KV) {
    var deleted api.KV
    if h == nil {
        return nil, [2]interface{}{nil, nil}
    }
    if less(key, h.Key) {
        if h.Left == nil { // key not present. Nothing to delete
            return h, [2]interface{}{nil, nil}
        }
        if !isRed(h.Left) && !isRed(h.Left.Left) {
            h = moveRedLeft(h)
        }
        h.Left, deleted = t.delete(h.Left, key)
    } else {
        if isRed(h.Left) {
            h = rotateRight(h)
        }
        // If @key equals @h.Key and no right children at @h
        if !less(h.Key, key) && h.Right == nil {
            return nil, [2]interface{}{h.Key, h.Value}
        }
        // PETAR: Added 'h.Right != nil' below
        if h.Right != nil && !isRed(h.Right) && !isRed(h.Right.Left) {
            h = moveRedRight(h)
        }
        // If @key equals @h.Key, and (from above) 'h.Right != nil'
        if !less(h.Key, key) {
            var subDeleted api.KV
            h.Right, subDeleted = deleteMin(h.Right)
            if subDeleted[0] == nil {
                panic("logic")
            }
            deleted = [2]interface{}{h.Key, h.Value}
            h.Key = subDeleted[0].(api.Key)
            h.Value = subDeleted[1].(api.Value)
        } else { // Else, @key is bigger than @h.Key
            h.Right, deleted = t.delete(h.Right, key)
        }
    }

    return fixUp(h), deleted
}

// Internal node manipulation routines

func newNode(key api.Key, value api.Value) *Node {
    return &Node{Key: key, Value: value}
}

func isRed(h *Node) bool {
    if h == nil {
        return false
    }
    return !h.Black
}

func rotateLeft(h *Node) *Node {
    x := h.Right
    if x.Black {
        panic("rotating a black link")
    }
    h.Right = x.Left
    x.Left = h
    x.Black = h.Black
    h.Black = false
    return x
}

func rotateRight(h *Node) *Node {
    x := h.Left
    if x.Black {
        panic("rotating a black link")
    }
    h.Left = x.Right
    x.Right = h
    x.Black = h.Black
    h.Black = false
    return x
}

// REQUIRE: Left and Right children must be present
func flip(h *Node) {
    h.Black = !h.Black
    h.Left.Black = !h.Left.Black
    h.Right.Black = !h.Right.Black
}

// REQUIRE: Left and Right children must be present
func moveRedLeft(h *Node) *Node {
    flip(h)
    if isRed(h.Right.Left) {
        h.Right = rotateRight(h.Right)
        h = rotateLeft(h)
        flip(h)
    }
    return h
}

// REQUIRE: Left and Right children must be present
func moveRedRight(h *Node) *Node {
    flip(h)
    if isRed(h.Left.Left) {
        h = rotateRight(h)
        flip(h)
    }
    return h
}

func fixUp(h *Node) *Node {
    if isRed(h.Right) {
        h = rotateLeft(h)
    }

    if isRed(h.Left) && isRed(h.Left.Left) {
        h = rotateRight(h)
    }

    if isRed(h.Left) && isRed(h.Right) {
        flip(h)
    }

    return h
}
