package btree

import (
    "time"
    "fmt"
    "unsafe"
    "sync/atomic"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

const(
    WS_SAYHI byte = iota
    WS_CLOSE         // {WS_CLOSE}
    // messages to mvcc goroutine
    WS_ACCESS        // {WS_ACCESS} -> timestamp int64
    WS_RELEASE       // {WS_RELEASE, timestamp} -> minAccess int64
    WS_CACHE         // {WS_CACHE, fpos}
    WS_CACHEQ        // {WS_CACHEQ} -> nodes []Node
    // messages to kv
    WS_PREFETCH      // {WS_PREFETCH, fposs []int64}
    WS_KVREAD        // {WS_KVREAD, fpos int64} -> []byte
    // messages to io
    WS_MVROOT        // {WS_MVROOT} -> root Node
    WS_POPFREELIST   // {WS_POPFREELIST} -> fpos int64
    WS_COMMIT        // {WS_COMMIT, node Node}
)

const (
    IO_FLUSH byte = iota
    IO_APPEND
    IO_CLOSE
)

type ReclaimData struct {
    fpos int64 // Node file position that needs to be reclaimed to free-list
    timestamp int64 // transaction timestamp under which fpos became stale.
}
type RecycleData ReclaimData

type MVCC struct {
    // In-memory data structure to cache intermediate nodes.
    nodecache unsafe.Pointer
    leafcache unsafe.Pointer

    // MVCC !
    // Note that fpos listed in reclaimQ could be cached.
    accessQ []int64        // sorted slice of timestamps

    // Communication channel for MVCC goroutine.
    req chan []interface{}
    res chan []interface{}
    // transaction channel
    translock chan bool
}

func (wstore *WStore) access() int64 {
    wstore.req <- []interface{}{WS_ACCESS}
    return (<-wstore.res)[0].(int64)
}

func (wstore *WStore) release(timestamp int64) int64 {
    wstore.req <- []interface{}{WS_RELEASE, timestamp}
    minAccess := (<-wstore.res)[0].(int64)
    return minAccess
}

func (wstore *WStore) cache(node Node) {
    wstore.req <- []interface{}{WS_CACHE, node}
}

func (wstore *WStore) cacheQ() []Node {
    wstore.req <- []interface{}{WS_CACHEQ}
    return (<-wstore.res)[0].([]Node)
}

func (wstore *WStore) close() {
    wstore.req <- []interface{}{WS_CLOSE}
    <-wstore.res
}

func doMVCC(wstore *WStore) {
    req, res := wstore.req, wstore.res
    newCacheQ := func() []Node {
        return make([]Node, 0, wstore.MaxLeafCache)
    }
    cacheQ := newCacheQ()

    for {
        cmd := <-req
        if cmd == nil {
            break
        }
        switch cmd[0].(byte) {
        case WS_SAYHI: // say hi!
            res <- []interface{}{WS_SAYHI}
        case WS_ACCESS:
            ts := time.Now().UnixNano()
            wstore.accessQ = append(wstore.accessQ, ts)
            res <- []interface{}{ts}
        case WS_RELEASE:
            minAccess := wstore.minAccess(cmd[1].(int64))
            res <- []interface{}{minAccess}
        case WS_CACHE:
            node := cmd[1].(Node)
            cacheQ = append(cacheQ, node)
        case WS_CACHEQ:
            res <- []interface{}{cacheQ}
            cacheQ = newCacheQ()
        case WS_CLOSE:
            res <- nil
        }
    }
}

func (wstore *WStore) closeChannels() {
    wstore.req <- []interface{}{WS_CLOSE}
    <-wstore.res
    close(wstore.req); wstore.req = nil
    close(wstore.res); wstore.res = nil
    wstore.kvreq <- []interface{}{WS_CLOSE}
    <-wstore.kvres
    close(wstore.kvreq); wstore.kvreq = nil
    close(wstore.kvres); wstore.kvres = nil
}

func (wstore *WStore) cacheLookup(fpos int64) Node{
    var node Node
    nc := (*map[int64]Node)(atomic.LoadPointer(&wstore.nodecache))
    if node = (*nc)[fpos]; node == nil {
        lc := (*map[int64]Node)(atomic.LoadPointer(&wstore.leafcache))
        node = (*lc)[fpos]
    }
    if node != nil {
        wstore.cacheHits += 1
    }
    return node
}

func (wstore *WStore) swapCache(nodecache, leafcache *map[int64]Node) {
    nc := atomic.LoadPointer(&wstore.nodecache)
    atomic.CompareAndSwapPointer(
        &wstore.nodecache, nc, unsafe.Pointer(nodecache))
    lc := atomic.LoadPointer(&wstore.leafcache)
    atomic.CompareAndSwapPointer(
        &wstore.leafcache, lc, unsafe.Pointer(leafcache))
}

// Demark the timestamp to zero in accessQ and return the minimum value of
// timestamp from accessQ. Also remove demarked timestamps from accessQ uptil
// the lowest timestamp.
func (wstore *WStore) minAccess(demarkts int64) int64 {
    // Shrink accessQ by sliding out demarked access.
    skip := 0
    for i, ts := range wstore.accessQ {
        if ts == 0 {
            skip += 1
        } else if ts == demarkts {
            wstore.accessQ[i] = 0
            skip += 1
        } else {
            break
        }
    }
    wstore.accessQ = wstore.accessQ[skip:]
    if len(wstore.accessQ) == 0 {
        return 0
    }
    return wstore.accessQ[0]
}

func max(a, b int64) int64 {
    if a > b {
        return a
    }
    return b
}
