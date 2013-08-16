package btree
import "bytes"
import "encoding/binary"

const (
    BLK_KEY_SIZE = 20   // bytes
    BLK_VALUE_SIZE = 8  // bytes
    BLK_OVERHEAD = 10   // bytes
)

type block struct {
    // Following fields are loaded from disk.
    leaf byte       // Is this leaf block, 0-false, 1-true
    size uint64     // number of `kv` entries in this node.
    ks []key        // slice of len() `size`.
    vs []int64      // slice of len() `size` + 1.
                    // file-offset in kv-file where value resides.
}

type key struct {
    ctrl uint32     // control word.
    kpos int64      // file-offset in kv-file where key resides.
    dpos int64      // file-offset in kv-file where document-id (primary key).
}

func (b *block) isLeaf() bool {
    return b.leaf == 1
}

func (b *block) load( store *Store, buf *bytes.Buffer ) *block {
    var size uint64
    var leaf byte
    // Fetch number of entries in this block
    if err := binary.Read( buf, binary.LittleEndian, &leaf ); err != nil {
        panic( err.Error() )
    }
    b.leaf = leaf
    // Fetch number of entries in this block
    if err := binary.Read( buf, binary.LittleEndian, &size ); err != nil {
        panic( err.Error() )
    }
    b.size = size
    // Fetch ikeys in this block
    var ctrl uint32
    var kpos, dpos int64
    max := (store.blocksize-BLK_OVERHEAD) / (BLK_KEY_SIZE+BLK_VALUE_SIZE)
    b.ks = make([]key, 0, max+1)
    for i:=uint64(0); i<b.size; i++ {
        if err := binary.Read( buf, binary.LittleEndian, &ctrl ); err != nil {
            panic(err.Error())
        }
        if err := binary.Read( buf, binary.LittleEndian, &kpos ); err != nil {
            panic(err.Error())
        }
        if err := binary.Read( buf, binary.LittleEndian, &dpos ); err != nil {
            panic(err.Error())
        }
        b.ks = append(b.ks, key{ctrl:ctrl, kpos:kpos, dpos:dpos})
    }
    // Fetch values in this block
    var vpos int64
    b.vs = make([]int64, max+2)
    for i:=uint64(0); i<b.size+1 ; i++ {
        if err := binary.Read( buf, binary.LittleEndian, &vpos ); err != nil {
            panic(err.Error())
        }
        b.vs = append(b.vs, vpos)
    }
    return b
}

func (b *block) dump( store *Store, buf *bytes.Buffer ) *block {
    if err := binary.Write( buf, binary.LittleEndian, &b.leaf ); err != nil {
        panic( err.Error() )
    }
    if err := binary.Write( buf, binary.LittleEndian, &b.size ); err != nil {
        panic( err.Error() )
    }
    for i:=uint64(0); i<b.size; i++ {
        k := b.ks[i]
        if err := binary.Write(buf, binary.LittleEndian, &k.ctrl); err != nil {
            panic(err.Error())
        }
        if err := binary.Read(buf, binary.LittleEndian, &k.kpos); err != nil {
            panic(err.Error())
        }
        if err := binary.Read(buf, binary.LittleEndian, &k.dpos); err != nil {
            panic(err.Error())
        }
    }
    for i:=uint64(0); i<b.size+1; i++ {
        v := b.vs[i]
        if err := binary.Read(buf, binary.LittleEndian, &v); err != nil {
            panic(err.Error())
        }
    }
    return b
}

