package btree

import (
    "fmt"
    //"os"
    "testing"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

func Test_store(t *testing.T) {
    store := testStore()
    defer func() {
        store.Destroy()
    }()
    // Fetch root.
    root := store.Root().(*knode)
    if root.isLeaf() == false {
        t.Fail()
    }
    if root.size != 0 {
        t.Fail()
    }
    // Add keys
    root.ks = root.ks[0:0]
    root.vs = root.vs[0:0]
    for i := 0; i < store.maxKeys(); i++ {
        root.ks = append(root.ks, bkey{uint32(i), int64(i), int64(i)})
        root.vs = append(root.vs, int64(i))
    }
    root.vs = append(root.vs, 0)
    root.leaf = FALSE
    root.size = store.maxKeys()
    iroot := store.flushNode(root).Root().(*inode)
    if iroot.size != store.maxKeys() {
        panic("Size mismatch")
    }
    if iroot.isLeaf() {
        panic("Block's leaf attribute is not properly set")
    }
    lst := iroot.size-1
    if iroot.ks[0].ctrl != 0 || iroot.ks[lst].ctrl != uint32(lst) {
        panic("iroot keys are not sane")
    }
    if len(iroot.vs) != iroot.size+1 {
        panic("len(vs) must be len(ks)+1")
    }
    if iroot.vs[0] != 0 || iroot.vs[lst] != int64(lst) || iroot.vs[iroot.size] != 0 {
        panic("root values are not sane")
    }
}
