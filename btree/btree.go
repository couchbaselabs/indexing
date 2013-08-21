package btree

type Interface interface{}
type Emitter func([]byte)
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
    Lookup(Key) (chan []byte, error)
    Range(Key, Key) (chan []byte, error)
    Remove(Key) bool
}

type Key interface {
    Bytes() []byte
    Docid() []byte
    Control() uint32
    Less( []byte ) bool
    Equal( []byte, []byte ) (bool, bool)
}

type Value interface {
    Bytes() []byte
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

func (bt *BTree) Insert(k Key, v Value) bool {
    root, spawn, median := bt.root.insert(k, v)
    if spawn == nil {
        bt.root = bt.root
        return true
    }
    fpos := bt.store.freelist.pop()
    in := inode{}.newNode(bt.store, fpos)
    in.ks := []key{ median }
    in.vs := []int64{ root.getKnode().fpos, spawn.getKnode().fpos }
    in.size := len(in.ks)
    bt.root := in
    dirtyblocks[fpos] = in
    return true
}

func (bt *BTree) Lookup(key Key) chan []byte {
    c := make(chan []byte)
    emit := func(val []byte) {
        if val == nil {
            close(c)
        } else {
            c <- val
        }
    }
    go bt.root.lookup(key, emit)
    return c
}

func (index *Store) Remove(key Key) bool {
    root := index.root
    if root.size == 0 {
        return false
    }
    if !index.root.remove(key)  {
        return false
    }
    root.size--
    if inode, ok := root.(*inode); ok {
      if inode.lenKeys() == 0 {
        index.root = inode.child[0]
      }
    }
    return true
}
//
//func (index *Store) Show() {
//    index.root.show(0)
//}
//
//func (index *Store) fsck() bool {
//    return index.root.fsck(0)
//}
