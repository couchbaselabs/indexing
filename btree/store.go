// Data store for btree, organised in two files, index-file and kv-file.
//
// index-file,
//   contains head block, list of free blocks within the index file,
//   and btree-blocks.
//
//   head,
//     512 byte sector written at the head of the file. contains reference to
//     the root bock.
//   freelist,
//     contains a list of 8-byte offset into the index file that contains
//     free blocks and blocks to be reclaimed.
//
// kv-file,
//   contains key, value, docid bytes. They are always added in append 
//   only mode, and a separate read-fd fetches them in random-access. Refer to
//   appendkv.go for more information.

package btree

import (
    "bytes"
    "fmt"
    "os"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

// Constants that are relevant for index-file and kv-file
const (
    OFFSET_SIZE = 8                 // 64 bit offset
    SECTOR_SIZE = 512               // Disk drive sector size in bytes.
    FLIST_SIZE = 1024 * OFFSET_SIZE // default free list size in bytes.
    BLOCK_SIZE = 1024 * 64          // default block size in bytes.
)

// An instance of this structure is constructed by reading the index-file.
type idxStore struct {
    head *Head         // head of the index store.
    freelist *FreeList // list of free blocks
    wfd *os.File       // index-file opened in write-only mode.
    rfd *os.File       // index-file opened in read-only mode.
}

// An instance of this type is constructed to manage kv-file.
type kvStore struct {
    wfd *os.File    // File descriptor opened in append-only mode.
    rfd *os.File    // Random access for read-only.
}

// Main structure that deals with btree data-store.
type Store struct {
    Config
    idxStore
    kvStore
    fpos_firstblock int64
}

//---- functions and receivers

// Create a new data-store for btree indexing.
// * creates index-file.
// * create kv-file.
// * open kv-file in appendonly mode for writing and readonly mode for
//   fetching.
// * open index-file in writeonly mode for updating btree and readonly mode
//   for fetching.
// * create an initial set of free blocks and update them in freelist.
// * update root block in head.
func Create(conf Config) *Store {
    store := &Store{
        Config: conf,
        fpos_firstblock: conf.sectorsize*2 + conf.flistsize*2,
    }
    if _, err := os.Stat(store.idxfile); err == nil {
        panic(err.Error())
    }
    if _, err := os.Stat(store.kvfile); err == nil {
        panic(err.Error())
    }
    os.Create(store.idxfile)
    os.Create(store.kvfile)
    // KV Store
    store.kvStore.rfd = openRfd(conf.kvfile)
    store.kvStore.wfd = openWfd(conf.kvfile, os.O_APPEND | os.O_WRONLY, 0660)
    // Index store
    store.idxStore.rfd = openRfd(conf.idxfile)
    store.idxStore.wfd = openWfd(conf.idxfile, os.O_WRONLY, 0660)

    // Setup the index head and freelist
    store.head = newHead(store)
    store.freelist = newFreeList(store)

    // Setup the head and freelist on disk.
    offsets := store.appendBlocks(store.fpos_firstblock)
    store.freelist.add(offsets[1:]).flush()

    // Setup root node.
    store.flushNode((&knode{}).newNode(store, offsets[0]))
    store.head.setRoot( offsets[0] ).flush()

    // Setup the index head and freelist
    store.head.fetch()
    store.freelist.fetch()
    return store
}

// Destroy is opposite of Create, it cleans up the datafiles.
func (store *Store) Destroy() *Store {
    if _, err := os.Stat(store.idxfile); err == nil {
        os.Remove(store.idxfile)
    }
    if _, err := os.Stat(store.kvfile); err == nil {
        os.Remove(store.kvfile)
    }
    return store
}

// Construct a new `Store` object.
func NewStore( conf Config ) *Store {
    store := &Store{
        Config: conf,
        fpos_firstblock: conf.sectorsize*2 + conf.flistsize*2,
    }
    // KV Store
    store.kvStore.rfd = openRfd(conf.kvfile)
    store.kvStore.wfd = openWfd(conf.kvfile, os.O_APPEND | os.O_WRONLY, 0660)
    // Index store
    store.idxStore.rfd = openRfd(conf.idxfile)
    store.idxStore.wfd = openWfd(conf.idxfile, os.O_WRONLY, 0660)
    // Fetch index header and freelist
    store.head = newHead(store).fetch()
    store.freelist = newFreeList(store).fetch()
    return store
}

// Close will release all resources maintained by store.
func (store *Store) Close() {
    store.idxStore.rfd.Close()
    store.idxStore.wfd.Close()
    store.kvStore.rfd.Close()
    store.kvStore.wfd.Close()
    store.head = nil
    store.freelist = nil
    store.idxStore.rfd = nil
    store.idxStore.wfd = nil
    store.kvStore.rfd = nil
    store.kvStore.wfd = nil
}

// Fetch the root btree block from index-file.
func (store *Store) Root() Node {
    return store.fetchNode( store.head.root )
}

// Fetch btree block persisted at location `fpos` in the index-file.
func (store *Store) fetchNode(fpos int64) Node {
    // If this node is already dirty and available in copy-on-write cache.
    if node := dirtyblocks[fpos]; node != nil {
        return node
    }

    // Fetch the prestine block from the disk and make a knode or inode.
    data := make([]byte, store.blocksize)
    if _, err := store.idxStore.rfd.ReadAt(data, fpos); err != nil {
        panic(err.Error())
    }
    buf := bytes.NewBuffer(data)
    b := (&block{}).load(store, buf)
    kn := knode{block:*b, store:store, fpos:fpos}
    if b.isLeaf() {
        return &kn
    } else {
        return &inode{knode:kn}
    }
    return nil // execution does not come here : FIXME.
}

// Persist btree block specified by `node` in the index-file.
func (store *Store) flushNode( node Node ) *Store {
    var kn *knode
    buf := new(bytes.Buffer)
    if in, ok := node.(*inode); ok {
        kn = &in.knode
    } else {
        kn = node.(*knode)
    }
    kn.dump(store, buf)
    if data := buf.Bytes(); len(data) <= store.blocksize {
        store.idxStore.wfd.WriteAt(data, kn.fpos)
        kn.prestine()
    } else {
        panic("flusnNode, btree block greater than store.blocksize")
    }
    return store
}

// appendBlocks will add new free blocks at the end of the index-file. Number
// of free blocks added will depend on the head room available in the
// freelist. Returns a slice of offsets, each element is a file-position of
// newly appened file-block.
// 
// If `fpos` is passed as 0, then free blocks will be create starting from
// SEEK_END, otherwise it will be created from specified `fpos`
func (store *Store) appendBlocks(fpos int64) []int64 {
    // `count` gives the head room in the freelist. since freelist.offsets
    // includes zero-terminator aswell, we add one more to the count.
    count := maxFreeBlocks(store) - len(store.freelist.offsets) + 1
    offsets := make([]int64, 0)
    if count > 0 {
        data := make([]byte, store.blocksize) // Empty block
        wfd := store.idxStore.wfd
        if fpos == 0 {
            var err error;
            if fpos, err = wfd.Seek(0, os.SEEK_END); err != nil {
                panic(err.Error())
            }
        }
        for i := 0; i < count; i++ {
            if n, err := wfd.Write(data); err == nil {
                offsets = append(offsets, fpos)
                fpos += int64(n)
            } else {
                panic(err.Error())
            }
        }
    }
    return offsets
}

// Maximum number of keys that are stored in a btree block.
func (store *Store) maxKeys() int {
    max := (store.blocksize-BLK_OVERHEAD) /
           (BLK_KEY_SIZE+BLK_VALUE_SIZE)
    max -= 1 // for n keys there will be n+1 values
    if max % 2 == 0 {
        return int(max)
    } else {
        return int(max-1)
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
