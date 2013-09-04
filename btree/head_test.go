package btree

import (
    "fmt"
    "testing"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");
func Test_Head(t *testing.T) {
    store := testStore()
    defer func() {
        store.Destroy()
    }()

    head := store.head
    if head.store != store {
        t.Fail()
    }
    if head.dirty != false {
        t.Fail()
    }
    if head.root != store.fpos_firstblock {
        t.Fail()
    }
}
