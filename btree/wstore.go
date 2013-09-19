// Contains necessary functions to do index writing.
package btree

import (
    "fmt"
    "os"
    "path/filepath"
    "sync"
    "syscall"
)

var _ = fmt.Sprintln("keep 'fmt' import during debugging", syscall.F_NOCACHE);

// WStore instances are created for each index. If applications tend to create
// multiple stores for the same index file, they will refer to the same
// wstore.
var writeStores = make(map[string]*WStore)
var wmu sync.Mutex // Protected access to `writeStores`

// structure that handles write.
type WStore struct {
    Config
    // More than one *Store can refer to a single instance of *WStore. Don't
    // close *WStore until refcount becomes Zero.
    refcount int
    idxWfd *os.File       // index-file opened in write-only mode.
    kvWfd *os.File        // file descriptor opened in append-only mode.
    head *Head            // head of the index store.
    freelist *FreeList    // list of free blocks.
    fpos_firstblock int64 // file offset for btree block.
    MVCC                  // MVCC concurrency control go-routine
    WStoreStats
}

// Statistical counts
type WStoreStats struct {
    cacheHits int64
    cacheEvicts int64
    popCounts int64
    maxlenAccessQ int64
    maxlenCommitQ int64
    maxlenReclaimQ int64
    maxlenNodecache int64
    appendCounts int64
    flushHeads int64
    flushFreelists int64
    dumpCounts int64
    reclaimedFpos int64
}

// Main API to get or instantiate a write-store. If write-store for this index
// file is already created, it will bre returned after incrementing the 
// reference count.
func OpenWStore(conf Config) *WStore {
    var wstore *WStore
    defer func() {
        wstore.req <- []interface{}{WS_SAYHI} // Say hi
        <-wstore.res
    }()
    wstore = getWStore(conf) // Try getting a write-store
    if wstore == nil { // nil means we have to create a new index file
        idxfile, _ := filepath.Abs(conf.Idxfile)
        // If index file is not even created, then create a new index file.
        createWStore(conf)
        // Open a new instance of index file in write-mode.
        wstore = newWStore(conf)
        wstore.head = newHead(wstore)
        wstore.freelist = newFreeList(wstore)
        wstore.head.fetch()
        wstore.freelist.fetch(wstore.head.crc)
        writeStores[idxfile] = wstore
        go doMVCC(wstore)
    }
    return wstore
}

// Close write-Store
func (wstore *WStore) CloseWStore() bool {
    if derefWSTore(wstore) && (wstore.refcount == 0) {
        // Persist
        wstore.reclaimBlocks(true)
        wstore.flushCommit(true, 0) // force flush !
        crc := wstore.freelist.flush() // then this
        wstore.head.flush(crc) // finally this
        // Cleanup
        wstore.kvWfd.Close();  wstore.kvWfd = nil
        wstore.idxWfd.Close(); wstore.idxWfd = nil
        wstore.judgementDay()
        close(wstore.req); wstore.req = nil
        close(wstore.res); wstore.res = nil
        close(wstore.translock); wstore.translock = nil
        return true
    }
    return false
}

// Destroy is opposite of Create, it cleans up the datafiles.
func (wstore *WStore) DestroyWStore() {
    if _, err := os.Stat(wstore.Idxfile); err == nil {
        os.Remove(wstore.Idxfile)
    }
    if _, err := os.Stat(wstore.Kvfile); err == nil {
        os.Remove(wstore.Kvfile)
    }
}

// Use `wmu` exclusion lock to fetch an existing write-store. By existing we
// refer an already instantiated write-store for this index file, or a new
// instance of the write-store if index file is present. If index file is
// not-found return nil.
func getWStore(conf Config) *WStore {
    var wstore *WStore
    idxfile, _ := filepath.Abs(conf.Idxfile)
    wmu.Lock() // Protected access
    defer wmu.Unlock()

    wstore = writeStores[idxfile]
    if wstore != nil {
        // If already index file is opened, return the same reference.
        wstore.refcount += 1 // increment the reference count.
    } else if _, err := os.Stat(idxfile); err == nil {
        // Open the new Store.
        wstore = newWStore(conf)
        wstore.head = newHead(wstore)
        wstore.freelist = newFreeList(wstore)
        wstore.head.fetch()
        wstore.freelist.fetch(wstore.head.crc)
        writeStores[idxfile] = wstore
        go doMVCC(wstore)
    }
    return wstore
}

// New instance of wstore.
func newWStore(conf Config) *WStore {
    idxmode, kvmode := os.O_WRONLY, os.O_APPEND | os.O_WRONLY
    // open in durability mode.
    if conf.Sync {
        idxmode |= os.O_SYNC
        kvmode |= os.O_SYNC
    }
    if conf.Nocache {
        idxmode |= syscall.F_NOCACHE
        kvmode |= syscall.F_NOCACHE
    }
    wstore := &WStore{
        Config: conf,
        refcount: 1,
        idxWfd: openWfd(conf.Idxfile, idxmode, 0660),
        kvWfd: openWfd(conf.Kvfile, kvmode, 0660),
        fpos_firstblock: conf.Sectorsize*2 + conf.Flistsize*2,
        MVCC: MVCC{
            nodecache: make(map[int64]Node),
            accessQ: make([]int64, 0),
            reclaimQ: make([]ReclaimData, 0),
            commitQ: make(map[int64]Node),
            // Serialization.
            req: make(chan []interface{}),
            res: make(chan []interface{}),
            translock: make(chan bool, 1),
        },
    }
    return wstore
}

// Lock and dereference the wstore before closing it.
func derefWSTore(wstore *WStore) bool {
    wmu.Lock()
    defer wmu.Unlock() 
    idxfile, _ := filepath.Abs(wstore.Idxfile)
    if writeStores[idxfile] != nil {
        wstore.refcount -= 1 // decrement reference count and check
        if wstore.refcount == 0 {
            delete(writeStores, idxfile)
        }
        return true
    }
    return false
}

// Create a new data-store for btree indexing.
func createWStore(conf Config) {
    // Create index file and associated key-value file.
    os.Create(conf.Idxfile)
    os.Create(conf.Kvfile)
    // Index store
    wfd := openWfd(conf.Idxfile, os.O_RDWR, 0660)
    // Append head sectors
    hdblock := make([]byte, conf.Sectorsize)
    wfd.Write(hdblock)
    wfd.Write(hdblock)
    // Append freelist block
    flblock := make([]byte, conf.Flistsize)
    wfd.Write(flblock)
    wfd.Write(flblock)
    wfd.Close()

    // Create a head, and freelist
    wstore := newWStore(conf)
    wstore.head = newHead(wstore)
    wstore.freelist = newFreeList(wstore)
    wstore.head.fetch() // Nothing important
    wstore.freelist.fetch(wstore.head.crc) // load `offsets`
    wstore.head.sectorsize = wstore.Sectorsize
    wstore.head.flistsize = wstore.Flistsize
    wstore.head.blocksize = wstore.Blocksize
    wstore.head.maxkeys = calculateMaxKeys(wstore.Blocksize)

    // Setup the head and freelist on disk.
    wstore.appendBlocks(wstore.fpos_firstblock, wstore.freelist.limit())

    // Root : Fetch a new node from freelist for root and setup.
    fpos := wstore.freelist.pop()
    b := (&block{leaf: TRUE}).newBlock(0, 0)
    root := &knode{block: *b, fpos: fpos, dirty: true}
    wstore.flushNode(root)
    wstore.head.setRoot(root.fpos)
    crc := wstore.freelist.flush()
    wstore.head.flush(crc)
    // Close wstore
    wstore.kvWfd.Close();  wstore.kvWfd = nil
    wstore.idxWfd.Close(); wstore.idxWfd = nil
    close(wstore.req); wstore.req = nil
    close(wstore.res); wstore.res = nil
    close(wstore.translock); wstore.translock = nil
}

// appendBlocks will add new free blocks at the end of the index-file. New
// offsets will be added to the in-memory copy of the freelist and the same
// slice of offsets will be returned back to the caller.
// 
// If `fpos` is passed as 0, then free blocks will be create starting from
// SEEK_END, otherwise it will be created from specified `fpos`.
//
// If `limit` > 0, will limit the number of blocks appended.
func (wstore *WStore) appendBlocks(fpos int64, limit int) []int64 {
    var err error
    max := wstore.maxFreeBlocks()
    count := max - len(wstore.freelist.offsets) - wstore.Maxlevel
    offsets := make([]int64, 0, max)
    if count > 0 {
        data := make([]byte, wstore.Blocksize) // Empty block
        wfd := wstore.idxWfd
        // Fix where to append
        if fpos == 0 {
            if fpos, err = wfd.Seek(0, os.SEEK_END); err != nil {
                panic(err.Error())
            }
        } else {
            if fpos, err = wfd.Seek(fpos, os.SEEK_SET); err != nil {
                panic(err.Error())
            }
        }
        // Actuall append
        if (limit > 0) && (count > limit) {
            count = limit - len(wstore.freelist.offsets) - wstore.Maxlevel
        }
        for i := 0; i < count; i++ {
            if n, err := wfd.Write(data); err == nil {
                offsets = append(offsets, fpos)
                fpos += int64(n)
            } else {
                panic(err.Error())
            }
        }
        wstore.freelist.add(offsets)
        wstore.appendCounts += 1 // stats
    }
    return offsets
}

func (wstore *WStore) flushNode(node Node) {
    kn := node.getKnode()
    if data := kn.gobEncode(); len(data) <= int(wstore.Blocksize) {
        wstore.idxWfd.WriteAt(data, kn.fpos)
        wstore.dumpCounts += 1 // stats
    } else {
        panic("flushNode, btree block greater than store.blocksize")
    }
}

// Get the maximum number of free blocks that can be monitored by the
// index-file. Returned value includes zero entry terminating the list.
func (wstore *WStore) maxFreeBlocks() int {
    return int(wstore.Flistsize / OFFSET_SIZE)
}

func (wstore *WStore) judgementDay() {
    if len(wstore.commitQ) > 0 {
        panic("there is still nodes to be commited")
    }
    if len(wstore.reclaimQ) > 0 {
        panic("there are still blocks to be reclaimed")
    }
    if len(wstore.accessQ) > 0 {
        panic("still a store access is in-progress")
    }
    wstore.head = nil; wstore.freelist = nil; wstore.nodecache = nil;
    wstore.accessQ = nil; wstore.reclaimQ =nil; wstore.commitQ =nil;
}
