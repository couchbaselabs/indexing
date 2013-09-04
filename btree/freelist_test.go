package btree

import (
    "fmt"
    "testing"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

func Test_maxFreeBlock(t *testing .T) {
    store := testStore()
    defer func() {
        store.Destroy()
    }()

    if ((maxFreeBlocks(store)+1)*8) != FLIST_SIZE {
        t.Fail()
    }
}

func Test_fetch(t *testing.T) {
    store := testStore()
    defer func() {
        store.Destroy()
    }()

    freelist := store.freelist
    if freelist.dirty == true {
        t.Fail()
    }
    if (freelist.offsets[0] != store.fpos_firstblock+BLOCK_SIZE) ||
       (freelist.offsets[1] != store.fpos_firstblock+(2*BLOCK_SIZE)) ||
       (freelist.offsets[1022] != 0) {
        t.Fail()
    }
}
