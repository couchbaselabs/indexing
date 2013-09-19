// Btree indexing algorithm for json key,value pairs.
package btree

import (
    "fmt"
)

type Emitter func([]byte)

// The following configuration go into the indexfile as well.
type IndexConfig struct {
    Sectorsize int64 // head sector-size in bytes.
    Flistsize int64 // free-list size in bytes.
    Blocksize int64 // btree block size in bytes.
}
// BTree configuration parameters
type Config struct {
    //-- file store
    Idxfile string
    Kvfile string
    IndexConfig

    //-- configuring the algorithm

    // maximum number of levels btree can grow.
    Maxlevel int

    // while count of keys in block goes less
    RebalanceThrs int

    AppendRatio float32

    // async
    Sync bool

    // nocache
    Nocache bool
}

// Btree instance
type BTree struct {
    Config
    store *Store
}

// interface made available to btree user.
type Indexer interface {
    // Insert key, value pair into the index. key is expected to implement
    // `Key` interface and value is expected to implement `Value` interface.
    // If the key is successfuly inserted returns true.
    Insert(Key, Value) bool

    // Count number of key,value pairs in this index.
    Count() int64

    // Return key-bytes, docid-bytes, and value bytes of the first
    // element in the list
    Front() ([]byte, []byte, []byte)

    // Check whether `key` is present in the index.
    Contains(Key) bool

    // Check whether `key` a particular `docid` is present in the index.
    Equals(Key) bool

    // Return a channel that will transmit a sequence of key bytes, docid
    // bytes and value bytes in sequence. After recieving three byte slices,
    // next entry will starts.
    FullSet() <-chan []byte

    // Return a channel that will transmit key bytes
    KeySet() <-chan []byte

    // Return a channel that will transmit docid bytes
    DocidSet() <-chan []byte

    // Return a channel that will transmit value bytes
    ValueSet() <-chan []byte

    // Return a channel that will transmit all values associated with `key`
    Lookup(Key) (chan []byte, error)

    //Range(Key, Key) (chan []byte, error)

    Remove(Key) bool

    //-- Meant for debugging.

    // Displays in-memory btree structure on stdout.
    Show()
    ShowKeys()

    // Display statistics so far.
    Stats()
}

// interfaces to be supported by key,value types.
type Key interface {
    // transform actual key content into byte slice, that can be persisted in
    // file.
    Bytes() []byte

    // transform document id that emitted this key-value pair into byte slice,
    // that can be persisted in file.
    Docid() []byte

    // control word.
    Control() uint32

    // Check this key with key slice argument. Return true if this key is less
    // than argument bytes.
    Less([]byte, []byte) bool

    // Check this key with key slice argument. Return true if this key is
    // less than or equal to argument bytes. Note that if keys are equal,
    // then they are sorted by docid.
    LessEq([]byte, []byte) bool

    // Check whether both key and document id compares equal.
    Equal([]byte, []byte) (bool, bool)
}

type Value interface {
    // transform value content into byte slice, that can be persisted in file.
    Bytes() []byte
}

// Create a new instance of btree. `store` will be used to persist btree
// blocks, key-value data and associated meta-information.
func NewBTree(store *Store) *BTree {
    btree := BTree{Config: store.Config, store: store}
    return &btree
}

func (bt *BTree) Close() {
    bt.store.Close()
}

// Insert key and value pair
func (bt *BTree) Insert(k Key, v Value) bool {
    root, staleroot, timestamp := bt.store.Root(true) // root with transaction
    root, spawn, mk, md, stalenodes := root.insert(k, v)
    stalenodes = append(stalenodes, staleroot)
    if spawn != nil {
        in := (&inode{}).newNode(bt.store)

        in.ks[0], in.ds[0] = mk, md
        in.ks, in.ds = in.ks[:1], in.ds[:1]
        in.size = len(in.ks)

        in.vs[0] = root.getKnode().fpos
        in.vs[1] = spawn.getKnode().fpos
        in.vs = in.vs[:2]

        root = in
    }
    bt.store.SetRoot(root) // First this
    bt.store.Release(true, stalenodes, timestamp) // Then this
    return true
}

func (bt *BTree) Count() int64 {
    root, _, timestamp := bt.store.Root(false)
    count := root.count()
    bt.store.Release(false, nil, timestamp)
    return count
}

func (bt *BTree) Front() ([]byte, []byte, []byte) {
    root, _, timestamp := bt.store.Root(false)
    b, c, d := root.front()
    bt.store.Release(false, nil, timestamp)
    return b, c, d
}

func (bt *BTree) Contains(key Key) bool {
    root, _, timestamp := bt.store.Root(false)
    st := root.contains(key)
    bt.store.Release(false, nil, timestamp)
    return st
}

func (bt *BTree) Equals(key Key) bool {
    root, _, timestamp := bt.store.Root(false)
    st := root.equals(key)
    bt.store.Release(false, nil, timestamp)
    return st
}

func (bt *BTree) FullSet() <-chan []byte {
    c := make(chan []byte)
    go func() {
        root, _, timestamp := bt.store.Root(false)
        root.traverse(func(kpos int64, dpos int64, vpos int64) {
            c <- bt.store.fetchKey(kpos)
            c <- bt.store.fetchDocid(dpos)
            c <- bt.store.fetchValue(vpos)
        })
        bt.store.Release(false, nil, timestamp)
        close(c)
    } ()
    return c
}

func (bt *BTree) KeySet() <-chan []byte {
    c := make(chan []byte)
    go func() {
        root, _, timestamp := bt.store.Root(false)
        root.traverse(func(kpos int64, dpos int64, vpos int64) {
            c <- bt.store.fetchKey(kpos)
        })
        bt.store.Release(false, nil, timestamp)
        close(c)
    }()
    return c
}

func (bt *BTree) DocidSet() <-chan []byte {
    c := make(chan []byte)
    go func() {
        root, _, timestamp := bt.store.Root(false)
        root.traverse(func(kpos int64, dpos int64, vpos int64) {
            c <- bt.store.fetchDocid(dpos)
        })
        bt.store.Release(false, nil, timestamp)
        close(c)
    }()
    return c
}

func (bt *BTree) ValueSet() <-chan []byte {
    c := make(chan []byte)
    go func() {
        root, _, timestamp := bt.store.Root(false)
        root.traverse(func(kpos int64, dpos int64, vpos int64) {
            c <- bt.store.fetchValue(vpos)
        })
        bt.store.Release(false, nil, timestamp)
        close(c)
    }()
    return c
}

func (bt *BTree) Lookup(key Key) chan []byte {
    c := make(chan []byte)
    go func () {
        root, _, timestamp := bt.store.Root(false)
        root.lookup(key, func(val []byte) {
            c <- val
        })
        close(c)
        bt.store.Release(false, nil, timestamp)
    }()
    return c
}

func (bt *BTree) Remove(key Key) bool {
    var stalenodes []Node
    root, staleroot, timestamp := bt.store.Root(true) // root with transaction
    if root.getKnode().size > 0 {
        root, _, stalenodes = root.remove(key)
        stalenodes = append(stalenodes, staleroot)
    } else {
        panic("Empty index")
    }
    bt.store.SetRoot(root) // First this
    bt.store.Release(true, stalenodes, timestamp) // Then this
    return true // FIXME: What is this ??
}

func (bt *BTree) Show() {
    fmt.Printf(
        "flist:%v block:%v maxKeys:%v\n\n",
        bt.Flistsize, bt.Blocksize, bt.store.maxKeys(),
    )
    root, _, timestamp := bt.store.Root(false)
    root.show(0)
    bt.store.Release(false, nil, timestamp)
}

func (bt *BTree) ShowKeys() {
    root, _, timestamp := bt.store.Root(false)
    root.showKeys(0)
    bt.store.Release(false, nil, timestamp)
}

func (bt *BTree) Stats() {
    store := bt.store
    wstore := store.wstore
    fmt.Printf(
        "cacheHits: %v     cacheEvicts: %v   popCounts: %v\n",
        wstore.cacheHits, wstore.cacheEvicts, wstore.popCounts,
    )
    fmt.Printf(
        "appendCounts: %v  flushHeads: %v    flushFreelists: %v\n",
        wstore.appendCounts, wstore.flushHeads, wstore.flushFreelists,
    )
    fmt.Printf(
        "maxlenCommitQ: %v maxlenAccessQ: %v maxlenReclaimQ:%v\n",
        wstore.maxlenCommitQ, wstore.maxlenAccessQ, wstore.maxlenReclaimQ,
    )
    fmt.Printf(
        "maxlenNodecache: %v dumpCounts: %v reclaimedFpos: %v loadCounts: %v\n",
        wstore.maxlenNodecache, wstore.dumpCounts, wstore.reclaimedFpos,
        store.loadCounts,
    )
    // Level counts
    root, _, timestamp := bt.store.Root(false)
    acc := make([]int64, 0, 16)
    acc, icount, kcount := root.levelCount(0, acc, 0, 0)
    fmt.Println("Levels :", acc, icount, kcount)
    bt.store.Release(false, nil, timestamp)
}
