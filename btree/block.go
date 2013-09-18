// translates btree blocks from persistant storage to in-memory data
// structure, called btree-node. A btree node can be a knode (also called leaf
// node) or it can be a inode. `block` structure is fundamental to both type
// of nodes.
package btree

import (
    "bytes"
    "encoding/binary"
    "fmt"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

const (
    // FIXME : Is there a better way to learn sizeof a struct.
    BLK_KEY_SIZE = 20  // bytes
    BLK_VALUE_SIZE = 8 // bytes
    BLK_OVERHEAD = 16  // bytes, leaf+size field
    TRUE = 1
    FALSE= 0
)

// block structure. every field in this structure has a corresponding data
// persisted as btree-block.
type block struct {
    leaf byte   // Is this leaf block, 0-false, 1-true
    size int    // number of `keys` in this node. `values` will be `keys`+1
    ks []bkey   // slice of len() `size`.
    vs []int64  // slice of len() `size` + 1.
                // file-offset in kv-file where value resides.
}

// bkey structure. This strucutre is internal to btree algorithm, to learn more
// about key structure passed from user code refer to `Key` interface.
type bkey struct {
    ctrl uint32 // control word.
    kpos int64  // file-offset in kv-file where key resides.
    dpos int64  // file-offset in kv-file where document-id (primary key).
}

// Check whether `block` is a leaf block, which means `Node` is a `knode`
// structure.
func (b *block) isLeaf() bool {
    return b.leaf == TRUE
}

// Convert btree block from persistant storage to `block` structure, which
// then will be get embedded inside `knode` or `inode` structure.
func (b *block) load(store *Store, buf *bytes.Buffer) *block {
    // Read information whether this block is a leaf node or not.
    binary.Read(buf, binary.LittleEndian, &b.leaf)

    // Fetch number of entries in this block
    var size32 int32
    binary.Read(buf, binary.LittleEndian, &size32)
    b.size = int(size32)

    // Load keys in this block
    var ctrl uint32
    var kpos, dpos int64
    max := store.maxKeys()
    // Make additional room, `max+1`, to detect node overflow.
    b.ks = make([]bkey, 0, max+1)
    for i := 0; i < b.size; i++ {
        binary.Read(buf, binary.LittleEndian, &ctrl)
        binary.Read(buf, binary.LittleEndian, &kpos)
        binary.Read(buf, binary.LittleEndian, &dpos)
        b.ks = append(b.ks, bkey{ctrl:ctrl, kpos:kpos, dpos:dpos})
    }

    // Load values in this block
    var vpos int64
    // in b+tree nodes, values are one more than the keys. Additional room,
    // `max+2`, is made to detect node overflow.
    b.vs = make([]int64, 0, max+2)
    for i := 0; i < (b.size+1); i++ {
        binary.Read(buf, binary.LittleEndian, &vpos)
        b.vs = append(b.vs, vpos)
    }
    return b
}

// Convert btree node structure, which can be either knode or inode structure,
// to byte-buffer that can be persisted as btree-block
func (b *block) dump(buf *bytes.Buffer) *block {
    // persist whether this node is leaf node or not.
    binary.Write(buf, binary.LittleEndian, &b.leaf)

    // persist the number of keys in this node.
    size32 := int32(b.size)
    binary.Write(buf, binary.LittleEndian, &size32)

    // Dump keys
    for i := 0; i < b.size; i++ {
        k := b.ks[i]
        binary.Write(buf, binary.LittleEndian, &k.ctrl)
        binary.Write(buf, binary.LittleEndian, &k.kpos)
        binary.Write(buf, binary.LittleEndian, &k.dpos)
    }

    // Dump values
    for i := 0; i < (b.size+1); i++ {
        binary.Write(buf, binary.LittleEndian, &b.vs[i])
    }
    return b
}
