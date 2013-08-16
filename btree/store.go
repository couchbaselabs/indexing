package btree
import "os"
import "bytes"
import "encoding/binary"

//---- BTree wide Constants
const (
    OFFSET_SIZE = 8                 // 64 bit offset
    SECTOR_SIZE = 512               // Disk drive sector size in bytes.
    FLIST_SIZE = 1024 * OFFSET_SIZE // default free list size in bytes.
    BLOCK_SIZE = 1024 * 64          // default block size in bytes.
)

//---- Btree wide types
type idxStore struct {
    head *Head
    freelist *FreeList
    wfd *os.File
    rfd *os.File
}
type kvStore struct {
    wfd *os.File
    rfd *os.File
    writech chan interface{}
}
type Store struct {
    Config
    idxStore
    kvStore
}

//---- functions and receivers
func NewStore( conf Config ) *Store {
    store := &Store{Config:conf}
    // KV Store
    store.kvStore.rfd = openRfd(conf.kvfile)
    store.kvStore.wfd = openWfd(conf.kvfile, os.O_APPEND, 0660)
    // Index store
    store.idxStore.rfd = openRfd(conf.idxfile)
    store.idxStore.wfd = openWfd(conf.idxfile, os.O_WRONLY, 0660)
    // Fetch index header and freelist
    store.head = newHead(store).fetch()
    store.freelist = newFreeList(store).fetch()
    // Launch go-routine to serialize append to KV store.
    store.kvStore.wfd.Seek(0, os.SEEK_END)
    store.writech = make(chan interface{})
    go appendOnly(store.kvStore.wfd, store.writech)
    return store
}

func Create(conf Config) *Store {
    store := &Store{Config:conf}
    if _, err := os.Stat( store.idxfile ); err != nil {
        panic(err.Error())
    }
    if _, err := os.Stat( store.kvfile ); err != nil {
        panic(err.Error())
    }
    // KV Store
    store.kvStore.rfd = openRfd(conf.kvfile)
    store.kvStore.wfd = openWfd(conf.kvfile, os.O_APPEND, 0660)
    // Index store
    store.idxStore.rfd = openRfd(conf.idxfile)
    store.idxStore.wfd = openWfd(conf.idxfile, os.O_WRONLY, 0660)
    // Setup the index head and freelist
    store.head = newHead(store)
    store.freelist = newFreeList(store)
    // Setup the head and freelist on disk.
    offsets := store.appendBlocks( store.flistsize / OFFSET_SIZE )
    root := offsets[0]
    store.head.setRoot( root )
    store.freelist.add( offsets[1:] ).flush()
    // Launch go-routine to serialize append to KV store.
    store.kvStore.wfd.Seek(0, os.SEEK_END)
    store.writech = make(chan interface{})
    go appendOnly(store.kvStore.wfd, store.writech)
    return store
}

func (store *Store) Close() {
    store.idxStore.rfd.Close()
    store.idxStore.wfd.Close()
    store.kvStore.rfd.Close()
    store.kvStore.wfd.Close()
    close( store.writech )
}

func (store *Store) Root() Node {
    return store.fetchNode( store.head.root )
}

func (store *Store) fetchNode(fpos int64) Node {
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

func (store *Store) flushNode( node Node ) *Store {
    var kn *knode
    buf := bytes.NewBuffer( []byte{} )
    if in, ok := node.(*inode); ok {
        kn = &in.knode
    } else {
        kn = node.(*knode)
    }
    kn.dump(store, buf)
    store.idxStore.wfd.WriteAt(buf.Bytes(), kn.fpos)
    kn.prestine()
    return store
}

func (store *Store) fetchValue(fpos int64) []byte {
    return readKV(store, fpos)
}

func (store *Store) appendValue(val []byte) int64 {
    return appendKV(store, val)
}

func (store *Store) fetchKey(fpos int64) []byte {
    return readKV(store, fpos)
}

func (store *Store) appendKey(key []byte) int64 {
    return appendKV(store, key)
}

func (store *Store) fetchDocid(fpos int64) []byte {
    return readKV(store, fpos)
}

func (store *Store) appendDocid(docid []byte) int64 {
    return appendKV(store, docid)
}

func (store *Store) appendBlocks( count int64 ) []int64 {
    offsets := make([]int64, 0, count)
    data := make([]byte, store.blocksize)
    if fpos, err := store.idxStore.wfd.Seek( 0, os.SEEK_END ); err == nil {
        for i:=int64(0); i<count; i++ {
            if n, err := store.idxStore.wfd.Write(data); err == nil {
                offsets = append(offsets, fpos)
                fpos += int64(n)
            } else {
                panic(err.Error())
            }
        }
    } else {
        panic(err.Error())
    }
    return offsets
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

func appendKV(store *Store, val []byte) int64 {
    store.writech <- val
    rc := <-store.writech
    if err, ok := rc.(error); ok {
        panic(err.Error())
    }
    fpos, ok := rc.(int64)
    if ok == false {
        panic("Expecting file-position !!")
    }
    return fpos
}

func readKV(store *Store, fpos int64) []byte {
    var size int32
    if _, err := store.kvStore.rfd.Seek(fpos, os.SEEK_SET); err != nil {
        panic( err.Error() )
    }
    binary.Read(store.kvStore.rfd, binary.LittleEndian, &size)
    b := make([]byte, size)
    if _, err := store.kvStore.rfd.Read( b ); err != nil {
        panic( err.Error() )
    }
    return b
}

