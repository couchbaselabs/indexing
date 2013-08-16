package btree

type Config struct {
    idxfile string
    kvfile string
    sectorsize int64        // in bytes
    flistsize int64         // in bytes
    blocksize int64         // in bytes
    minFreelist int         // Minimum threshold of free blocks
}

type BTree struct {
    Config
    store *Store
    root Node
}

// Interfaces
type Indexer interface {
    Count() int64
    Front() []byte
    Contains(Key) bool
    FullSet() <-chan []byte
    KeySet() <-chan []byte
    DocidSet() <-chan []byte
    ValueSet() <-chan []byte
    Insert(Key, Value) bool
    Remove(Key) bool
}

func NewBTree(store *Store) *BTree {
    btree := BTree{ Config:store.Config, store:store, root:store.Root() }
    return &btree
}

func (bt *BTree) Count() uint64 {
  return bt.root.count()
}

func (bt *BTree) Front() []byte {
    return bt.root.front()
}

func (bt *BTree) Contains(key Key) bool {
    return bt.root.contains(key)
}

func (bt *BTree) FullSet() <-chan []byte {
    c := make(chan []byte)
    go func() {
        bt.root.traverse( func(k key, v int64) {
            c <- bt.store.fetchKey(k.kpos)
            c <- bt.store.fetchDocid(k.dpos)
            c <- bt.store.fetchValue(v)
        })
        close(c)
    } ()
    return c
}

func (bt *BTree) KeySet() <-chan []byte {
    c := make(chan []byte)
    go func() {
        bt.root.traverse( func(k key, v int64) {
            c <- bt.store.fetchKey(k.kpos)
        })
        close(c)
    }()
    return c
}

func (bt *BTree) DocidSet() <-chan []byte {
    c := make(chan []byte)
    go func() {
        bt.root.traverse( func(k key, v int64) {
            c <- bt.store.fetchDocid(k.dpos)
        })
        close(c)
    }()
    return c
}

func (bt *BTree) ValueSet() <-chan []byte {
    c := make(chan []byte)
    go func() {
        bt.root.traverse( func(k key, v int64) {
            c <- bt.store.fetchValue(v)
        })
        close(c)
    }()
    return c
}

//func (bt *BTree) Insert(k Key, v Value) bool {
//    added, spawn, val := bt.root.insert(k, v)
//    if added == false {
//        return false
//    }
//    if spawn == nil {
//        return true
//    }
//    max := (bt.blocksize-BLK_OVERHEAD) / (BLK_KEY_SIZE+BLK_VALUE_SIZE)
//    root := new(inode)
//    root.child = make([]bNode, max+2)[0:0]
//    root.key = make([]T, max+1)[0:0]
//
//    root.child = root.child[0:2]
//    root.child[0] = t.node
//    root.child[1] = spawn
//    root.key = root.key[0:1]
//    root.key[0] = val
//    t.node = root
//    return true
//}
//
//func (index *Store) Remove(key Key) bool {
//    root := index.root
//    if root.size == 0 {
//        return false
//    }
//    if !index.root.remove(key)  {
//        return false
//    }
//    root.size--
//    if inode, ok := root.(*inode); ok {
//      if inode.lenKeys() == 0 {
//        index.root = inode.child[0]
//      }
//    }
//    return true
//}
//
//func (index *Store) Show() {
//    index.root.show(0)
//}
//
//func (index *Store) fsck() bool {
//    return index.root.fsck(0)
//}
