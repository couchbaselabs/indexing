// MVCC controller process.
package btree

import (
    "fmt"
    "time"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging")

const (
    WS_SAYHI byte = iota
    WS_CLOSE      // {WS_CLOSE}

    // messages to mvcc goroutine
    WS_ACCESS      // {WS_ACCESS} -> timestamp int64
    WS_RELEASE     // {WS_RELEASE, timestamp} -> minAccess int64
    WS_SETSNAPSHOT // {WS_SETSNAPSHOT, offsets []int64, root int64, timestamp int64}

    // messages to defer routine
    WS_PINGCACHE    // {WS_PINGCACHE, what byte, fpos int64, node Node}
    WS_PINGKD       // {WS_PINGKD, fpos int64, key []byte}
    WS_MV           // {WS_MV, mv *MV}
    WS_SYNCSNAPSHOT // {WS_MV, minAccess int64}
)

const (
    IO_FLUSH byte = iota
    IO_APPEND
    IO_CLOSE
)

type ReclaimData struct {
    fpos      int64 // Node file position that needs to be reclaimed to free-list
    timestamp int64 // transaction timestamp under which fpos became stale.
}
type RecycleData ReclaimData

type MVCC struct {
    accessQ   []int64            // sorted slice of timestamps
    req       chan []interface{} // Communication channel for MVCC goroutine.
    res       chan []interface{}
    translock chan bool // transaction channel
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

func (wstore *WStore) setSnapShot(offsets []int64, root, timestamp int64) {
    wstore.req <- []interface{}{WS_SETSNAPSHOT, offsets, root, timestamp}
    <-wstore.res
}

func (wstore *WStore) close() {
    wstore.req <- []interface{}{WS_CLOSE}
    <-wstore.res
}

func doMVCC(wstore *WStore) {
    req, res := wstore.req, wstore.res
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
        case WS_SETSNAPSHOT:
            offsets, root, ts := cmd[1].([]int64), cmd[2].(int64), cmd[3].(int64)
            wstore.freelist.add(offsets)
            wstore.head.setRoot(root, ts)
            wstore.ping2Pong()
            res <- nil
        case WS_CLOSE:
            res <- nil
        }
    }
}

func (wstore *WStore) closeChannels() {
    wstore.req <- []interface{}{WS_CLOSE}
    <-wstore.res
    close(wstore.req)
    wstore.req = nil
    close(wstore.res)
    wstore.res = nil

    syncChan := make(chan interface{})
    wstore.deferReq <- []interface{}{WS_CLOSE, syncChan}
    <-syncChan
    close(wstore.deferReq)
    wstore.deferReq = nil
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
        } else if ts > demarkts {
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
