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

    DrainRate int

    MaxLeafCache int

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

    // Return a channel that will transmit all values associated with `key`,
    // make sure the `docid` is set to minimum value to lookup all values
    // greater thatn `key` && `docid`
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
    CompareLess(*Store, int64, int64, bool) (int, int64, int64)

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
    root, mv, timestamp := bt.store.OpStart(true) // root with transaction
    spawn, mk, md := root.insert(k, v, mv)
    if spawn != nil {
        in := (&inode{}).newNode(bt.store)

        in.ks[0], in.ds[0] = mk, md
        in.ks, in.ds = in.ks[:1], in.ds[:1]
        in.size = len(in.ks)

        in.vs[0] = root.getKnode().fpos
        in.vs[1] = spawn.getKnode().fpos
        in.vs = in.vs[:2]

        mv.commits = append(mv.commits, in)
        root = in
    }
    mv.root = root.getKnode().fpos
    bt.store.OpEnd(true, mv, timestamp) // Then this
    return true
}

func (bt *BTree) Count() int64 {
    root, mv, timestamp := bt.store.OpStart(false)
    count := root.count()
    bt.store.OpEnd(false, mv, timestamp)
    return count
}

func (bt *BTree) Front() ([]byte, []byte, []byte) {
    root, mv, timestamp := bt.store.OpStart(false)
    b, c, d := root.front()
    bt.store.OpEnd(false, mv, timestamp)
    return b, c, d
}

func (bt *BTree) Contains(key Key) bool {
    root, mv, timestamp := bt.store.OpStart(false)
    st := root.contains(key)
    bt.store.OpEnd(false, mv, timestamp)
    return st
}

func (bt *BTree) Equals(key Key) bool {
    root, mv, timestamp := bt.store.OpStart(false)
    st := root.equals(key)
    bt.store.OpEnd(false, mv, timestamp)
    return st
}

func (bt *BTree) FullSet() <-chan []byte {
    c := make(chan []byte)
    go func() {
        root, mv, timestamp := bt.store.OpStart(false)
        root.traverse(func(kpos int64, dpos int64, vpos int64) {
            c <- bt.store.fetchKey(kpos)
            c <- bt.store.fetchDocid(dpos)
            c <- bt.store.fetchValue(vpos)
        })
        bt.store.OpEnd(false, mv, timestamp)
        close(c)
    } ()
    return c
}

func (bt *BTree) KeySet() <-chan []byte {
    c := make(chan []byte)
    go func() {
        root, mv, timestamp := bt.store.OpStart(false)
        root.traverse(func(kpos int64, dpos int64, vpos int64) {
            c <- bt.store.fetchKey(kpos)
        })
        bt.store.OpEnd(false, mv, timestamp)
        close(c)
    }()
    return c
}

func (bt *BTree) DocidSet() <-chan []byte {
    c := make(chan []byte)
    go func() {
        root, mv, timestamp := bt.store.OpStart(false)
        root.traverse(func(kpos int64, dpos int64, vpos int64) {
            c <- bt.store.fetchDocid(dpos)
        })
        bt.store.OpEnd(false, mv, timestamp)
        close(c)
    }()
    return c
}

func (bt *BTree) ValueSet() <-chan []byte {
    c := make(chan []byte)
    go func() {
        root, mv, timestamp := bt.store.OpStart(false)
        root.traverse(func(kpos int64, dpos int64, vpos int64) {
            c <- bt.store.fetchValue(vpos)
        })
        bt.store.OpEnd(false, mv, timestamp)
        close(c)
    }()
    return c
}

func (bt *BTree) Lookup(key Key) chan []byte {
    c := make(chan []byte)
    go func () {
        root, mv, timestamp := bt.store.OpStart(false)
        root.lookup(key, func(val []byte) {
            c <- val
        })
        close(c)
        bt.store.OpEnd(false, mv, timestamp)
    }()
    return c
}

func (bt *BTree) Remove(key Key) bool {
    root, mv, timestamp := bt.store.OpStart(true) // root with transaction
    if root.getKnode().size > 0 {
        root, _, _, _ = root.remove(key, mv)
    } else {
        panic("Empty index")
    }
    mv.root = root.getKnode().fpos
    bt.store.OpEnd(true, mv, timestamp) // Then this
    return true // FIXME: What is this ??
}

// Development method.
func (bt *BTree) Drain() {
    bt.store.wstore.commit(nil, 0, true)
}

func (bt *BTree) Show() {
    fmt.Printf(
        "flist:%v block:%v maxKeys:%v\n\n",
        bt.Flistsize, bt.Blocksize, bt.store.maxKeys(),
    )
    root, mv, timestamp := bt.store.OpStart(false)
    root.show(0)
    bt.store.OpEnd(false, mv, timestamp)
}

func (bt *BTree) Check() {
    root, mv, timestamp := bt.store.OpStart(false)
    root.check()
    root.checkSeparator(make([]int64, 0))
    bt.store.OpEnd(false, mv, timestamp)
}

func (bt *BTree) ShowKeys() {
    root, mv, timestamp := bt.store.OpStart(false)
    root.showKeys(0)
    bt.store.OpEnd(false, mv, timestamp)
}

func (bt *BTree) Stats() {
    store := bt.store
    wstore := store.wstore
    fmt.Printf(
        "cacheHits:    %10v    popCounts:  %10v    maxlenAccessQ: %10v\n",
        wstore.cacheHits, wstore.popCounts, wstore.maxlenAccessQ,
    )
    fmt.Printf(
        "commitHits:   %10v    maxlenNC:   %10v    maxlenLC:      %10v \n",
        wstore.commitHits, wstore.maxlenNC, wstore.maxlenLC,
    )
    fmt.Printf(
        "appendCounts: %10v    flushHeads: %10v    flushFreelists:%10v\n",
        wstore.appendCounts, wstore.flushHeads, wstore.flushFreelists,
    )
    fmt.Printf(
        "dumpCounts: %v loadCounts: %v readKV: %v appendKV: %v\n",
        wstore.dumpCounts, store.loadCounts, wstore.countReadKV,
        wstore.countAppendKV,
    )
    fmt.Printf(
        "garbageBlocks: %v freelist: %v\n",
        wstore.garbageBlocks, len(wstore.freelist.offsets),
    )
    // Level counts
    acc, icount, kcount := bt.LevelCount()
    fmt.Println("Levels :", acc, icount, kcount)
}

func (bt *BTree) LevelCount() ([]int64, int64, int64) {
    root, mv, timestamp := bt.store.OpStart(false)
    acc := make([]int64, 0, 16)
    acc, icount, kcount := root.levelCount(0, acc, 0, 0)
    ln := int64(len(bt.store.wstore.freelist.offsets))
    fmt.Println("Blocks: ", icount + kcount + ln)
    bt.store.OpEnd(false, mv, timestamp)
    return acc, icount, kcount
}
