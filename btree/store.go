// Data store for btree, organised in two files, index-file and kv-file.
//
// index-file,
//   contains head block, list of free blocks within index file,
//   and btree-blocks.
//
//   head,
//     512 byte sector written at the head of the file. contains reference to
//     the root bock, head-sector-size, free-list-size and block-size.
//   freelist,
//     contains a list of 8-byte offset into the index file that contains
//     free blocks.
//
// kv-file,
//   contains key, value, docid bytes. They are always added in append 
//   only mode, and a separate read-fd fetches them in random-access. Refer to
//   appendkv.go for more information.

package btree

import (
    "fmt"
    "sync/atomic"
    "os"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

// constants that are relevant for index-file and kv-file
const (
    OFFSET_SIZE = 8                 // 64 bit offset
    SECTOR_SIZE = 512               // Disk drive sector size in bytes.
    FLIST_SIZE = 1024 * OFFSET_SIZE // default free list size in bytes.
    BLOCK_SIZE = 1024 * 64          // default block size in bytes.
)

type Store struct {
    Config
    wstore *WStore  // Reference to write-store.
    kvRfd *os.File  // Random read-only access for kv-file
    idxRfd *os.File // Random read-only access for index-file
    // Stats
    loadCounts int64
}

//---- functions and receivers

// Construct a new `Store` object.
func NewStore(conf Config) *Store {
    wstore := OpenWStore(conf)
    store := &Store{
        Config: conf,
        wstore: wstore,
        idxRfd: openRfd(conf.Idxfile),
        kvRfd: openRfd(conf.Kvfile),
    }
    // TODO : Check whether index file is sane, both configuration and
    // freelist.
    return store
}

// Close will release all resources maintained by store.
func (store *Store) Close() {
    store.kvRfd.Close(); store.kvRfd = nil
    store.idxRfd.Close(); store.idxRfd = nil
    store.wstore.CloseWStore(); store.wstore = nil
}

// Destroy is opposite of Create, it cleans up the datafiles. Data files will
// be deleted only when all references to WStore is removed.
func (store *Store) Destroy() {
    store.kvRfd.Close(); store.kvRfd = nil
    store.idxRfd.Close(); store.idxRfd = nil
    // Close and destroy
    if store.wstore.CloseWStore() {
        store.wstore.DestroyWStore();
    }
    store.wstore = nil
}


// Fetch the root btree block from index-file. `transaction` must be true for
// write access. It is assumed that there will be only one outstanding
// transaction at any given time, so the caller has to make sure to acquire a
// transaction lock from MVCC controller.
func (store *Store) OpStart(transaction bool) (Node, *MV, int64) {
    var mv *MV
    var root Node
    if transaction {
        store.wstore.translock <- true
        staleroot := store.FetchMVCCNode(mvRoot(store))
        root = staleroot.copyOnWrite()
        mv = &MV{stales: []Node{staleroot}, commits: []Node{root}}
    } else {
        root = store.FetchNode(store.currentRoot())
        mv = &MV{stales: []Node{}, commits: []Node{}}
    }
    ts := store.wstore.access()
    return root, mv, ts
}

// Opposite of OpStart() API.
func (store *Store) OpEnd(transaction bool, mv *MV, ts int64) {
    minAccess := store.wstore.release(ts)
    if transaction {
        store.wstore.commit(mv, minAccess, false)
        <-store.wstore.translock
    }
}

func (store *Store) currentRoot() int64 {
    return atomic.LoadInt64(&store.wstore.head.root)
}

func (store *Store) FetchNode(fpos int64) Node {
    var node Node

    // Sanity check
    fpos_firstblock, blocksize := store.wstore.fpos_firstblock, store.Blocksize
    if fpos < fpos_firstblock || (fpos - fpos_firstblock) % blocksize != 0 {
        panic("Invalid fpos to fetch")
    }

    // Try to fetch from cache
    if node = store.wstore.cacheLookup(fpos); node != nil {
        return node
    }

    // If not, fetch the prestine block from the disk and make a knode or inode.
    data := make([]byte, blocksize)
    if _, err := store.idxRfd.ReadAt(data, fpos); err != nil {
        panic(err.Error())
    }
    b := (&block{}).newBlock(0, store.maxKeys()); b.gobDecode(data)
    kn := knode{block:*b, store:store, fpos:fpos}
    if b.isLeaf() {
        node = &kn
    } else {
        node = &inode{knode:kn}
    }
    store.wstore.cache(node)
    store.loadCounts += 1
    return node
}

func (store *Store) FetchMVCCNode(fpos int64) Node {
    var node Node

    // Sanity check
    fpos_firstblock, blocksize := store.wstore.fpos_firstblock, store.Blocksize
    if fpos < fpos_firstblock || (fpos - fpos_firstblock) % blocksize != 0 {
        panic("Invalid fpos to fetch")
    }

    // Try to fetch from commitQ
    if node = store.wstore.ccacheLookup(fpos); node != nil {
        return node
    }
    return store.FetchNode(fpos)
}


// Maximum number of keys that are stored in a btree block, it is always an
// even number and adjusted for the additional value entry.
func (store *Store) maxKeys() int {
    return int(store.wstore.head.maxkeys)
}

func calculateMaxKeys(blocksize int64) int64 {
    return (blocksize - 16) / 24
}

func calculateMaxKeys_gob(blocksize int64) int64 {
    max64 := int64(9223372036854775807-1)
    start := int64(float64(blocksize-14) / (10.1875*3))
    inc := int64(2)
    for i := start; ; {
        b := (&block{leaf:TRUE}).newBlock(int(i), int(i))
        for j := int64(0); j < i; j++ {
            b.ks[j] = max64; b.ds[j] = max64; b.vs[j] = max64
        }
        if int64(len(b.gobEncode())) > blocksize {
            if inc > 4 {
                i -= inc/2; inc = 2
                continue
            }
            max :=  i-2
            if max % 2 == 0 {   // fix max as even value.
                return max
            }
            return max-1
        }
        i += inc; inc *= 2
    }
}

//---- local functions
func openWfd(file string, flag int, perm os.FileMode) *os.File {
    if wfd, err := os.OpenFile(file, flag, perm); err != nil {
        panic( err.Error() )
    } else {
        return wfd
    }
}

func openRfd(file string) *os.File {
    if rfd, err := os.Open(file); err != nil {
        panic( err.Error() )
    } else {
        return rfd
    }
}

func is_configSane(store *Store) bool {
    wstore := store.wstore
    if store.Sectorsize != wstore.Sectorsize {
        return false
    }
    if store.Flistsize != wstore.Flistsize {
        return false
    }
    if store.Blocksize != wstore.Blocksize {
        return false
    }
    return true
}

func BlockCalculate(store *Store) {
    fi, _ := store.idxRfd.Stat()
    size := fi.Size()
    fmt.Println("size:", size)
    count := (size - 1024 - (store.Flistsize*2)) / store.Blocksize
    fmt.Println("diskblocks:", count)
}
