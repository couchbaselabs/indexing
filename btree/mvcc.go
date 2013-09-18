package btree

import (
    "time"
    "fmt"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

const(
    WS_SAYHI byte = iota
    WS_GETROOT       // {WS_GETROOT}                 -> node Node
    WS_SETROOT       // {WS_SETROOT, root Node}
    WS_CACHELOOKUP   // {WS_CACHELOOKUP, fpos int64} -> node Node
    WS_CACHE         // {WS_CACHE, node Node}
    WS_POPFREELIST   // {WS_POPFREELIST}             -> fpos int64
    WS_ACCESS        // {WS_ACCESS}
    WS_RELEASE       // {WS_RELEASE, stalenodes []Node}
    WS_COMMIT        // {WS_COMMIT, node Node}
)

type ReclaimData struct {
    fpos int64 // Node file position that needs to be reclaimed to free-list
    timestamp int64 // transaction timestamp under which fpos became stale.
}

type MVCC struct {
    // In-memory data structure to cache intermediate nodes.
    nodecache map[int64]Node

    // MVCC !
    // Note that fpos listed in reclaimQ could be cached, but fpos in commitQ
    // will not be.
    accessQ []int64        // sorted slice of timestamps
    reclaimQ []ReclaimData // sorted slice of [fpos, timestamp]
    commitQ map[int64]Node // slice of fpos

    // Communication channel to serialize writes and other side effects.
    req chan []interface{}
    res chan []interface{}
    // transaction channel
    translock chan bool
}

func (wstore *WStore) getRoot(store *Store) Node {
    wstore.req <- []interface{}{WS_GETROOT, store}
    return (<-wstore.res)[0].(Node)
}

func (wstore *WStore) setRoot(root Node) {
    wstore.req <- []interface{}{WS_SETROOT, root}
    <-wstore.res
}

func (wstore *WStore) cacheLookup(fpos int64) Node {
    wstore.req <- []interface{}{WS_CACHELOOKUP, fpos}
    node := (<-wstore.res)[0]
    if node == nil {
        return nil
    }
    return node.(Node)
}

func (wstore *WStore) cache(node Node) {
    wstore.req <- []interface{}{WS_CACHE, node}
    <-wstore.res
}

func (wstore *WStore) popFreelist() int64 {
    wstore.req <- []interface{}{WS_POPFREELIST}
    return (<-wstore.res)[0].(int64)
}

func (wstore *WStore) access() int64 {
    wstore.req <- []interface{}{WS_ACCESS}
    return (<-wstore.res)[0].(int64)
}

func (wstore *WStore) release(stalenodes []Node, timestamp int64) {
    wstore.req <- []interface{}{WS_RELEASE, stalenodes, timestamp}
    <-wstore.res
}

func (wstore *WStore) commit(node Node) {
    wstore.req <- []interface{}{WS_COMMIT, node}
    <-wstore.res
}

func doMVCC(wstore *WStore) {
    req, res := wstore.req, wstore.res
    for {
        cmd := <-req
        if cmd != nil {
            switch cmd[0].(byte) {
            case WS_SAYHI: // say hi!
                res <- []interface{}{WS_SAYHI}
            case WS_GETROOT:
                store := cmd[1].(*Store)
                if wstore.head.rootN == nil {
                    root := store.FetchNode(wstore.head.root)
                    res <- []interface{}{root}
                } else {
                    res <- []interface{}{wstore.head.rootN}
                }
            case WS_SETROOT:
                root := cmd[1].(Node)
                wstore.head.setRoot(root.getKnode().fpos)
                wstore.head.rootN = root
                res <- nil
            case WS_CACHELOOKUP:
                node := wstore._cacheLookup(cmd[1].(int64))
                res <- []interface{}{node}
            case WS_CACHE:
                wstore._cache(cmd[1].(Node))
                res <- nil
            case WS_POPFREELIST:
                fpos := wstore.freelist.pop()
                res <- []interface{}{fpos}
            case WS_ACCESS:
                ts := time.Now().UnixNano()
                wstore.accessQ = append(wstore.accessQ, ts)
                res <- []interface{}{ts}
            case WS_RELEASE:
                wstore._release(cmd[1].([]Node), cmd[2].(int64))
                wstore.maxlenAccessQ =
                        max(wstore.maxlenAccessQ, int64(len(wstore.accessQ)))
                res <- nil
            case WS_COMMIT:
                node := cmd[1].(Node)
                wstore._cache(node) // Commit will also cache
                wstore.commitQ[node.getKnode().fpos] = node
                wstore.maxlenCommitQ =
                        max(wstore.maxlenCommitQ, int64(len(wstore.commitQ)))
                res <- nil
            }
        } else {
            break
        }
    }
}

func (wstore *WStore) _cacheLookup(fpos int64) Node {
    var node Node
    if node = wstore.commitQ[fpos]; node == nil {
        node = wstore.nodecache[fpos]
    }
    if node != nil {
        wstore.cacheHits += 1
    }
    return node
}

func (wstore *WStore) _cache(node Node) {
    wstore.maxlenNodecache =
            max(wstore.maxlenNodecache, int64(len(wstore.nodecache)))
    if in, ok := node.(*inode); ok {
        wstore.nodecache[in.fpos] = in
    }
}

func (wstore *WStore) _cacheEvict(fpos int64) {
    delete(wstore.nodecache, fpos)
    wstore.cacheEvicts += 1
}

func (wstore *WStore) _release(stalenodes []Node, timestamp int64) {
    // If release is for a write-access, stalenodes need to be added to
    // reclaimQ, but note that stalenodes can still be referred by on-going
    // read-access.
    if len(stalenodes) > 0 {
        ts := time.Now().UnixNano()
        for _, node := range stalenodes {
            kn := node.getKnode()
            // Skip stalenodes that are already in the commitQ. Their
            // file-positions are reused anyway.
            if kn.dirty {
                if wstore.commitQ[kn.fpos] != nil {
                    continue
                } else {
                    panic("Dirty blocks are expected in commitQ")
                }
            }
            rd := ReclaimData{kn.fpos, ts}
            wstore.reclaimQ = append(wstore.reclaimQ, rd)
        }
        wstore.maxlenReclaimQ =
                max(wstore.maxlenReclaimQ, int64(len(wstore.reclaimQ)))
    }

    // De-mark this access.
    for i := range wstore.accessQ {
        if wstore.accessQ[i] == timestamp {
            wstore.accessQ[i] = 0
            break
        }
    }
    wstore.reclaimBlocks(false)
}

func (wstore *WStore) reclaimBlocks(force bool) {
    accessTS := wstore.minAccess()

    // Adjust commitQ and nodecache based on reclaimed stalenodes so that we
    // wont cache or commit un-referred nodes.
    for _, rd := range wstore.reclaimQ {
        if (accessTS == 0) || (rd.timestamp < accessTS) {
            delete(wstore.commitQ, rd.fpos)
            wstore._cacheEvict(rd.fpos)
        }
    }

    // Whether free blocks are critically low, then reclaim some from reclaimQ
    if force || wstore.freelist.isCritical() {
        reclaimFpos := make([]int64, 0, wstore.maxFreeBlocks())
        skip := 0
        for _, rd := range wstore.reclaimQ {
            if (accessTS == 0) || (rd.timestamp < accessTS) {
                wstore._cacheEvict(rd.fpos)
                reclaimFpos = append(reclaimFpos, rd.fpos)
                skip += 1
                continue
            }
            break
        }
        wstore.reclaimQ = wstore.reclaimQ[skip:]
        wstore.reclaimedFpos += int64(len(reclaimFpos))
        if len(reclaimFpos) > 0 {
            wstore.freelist.add(reclaimFpos)
            // new blocks are added to the list, flush them.
            wstore.flushCommit(true, 0) // force flush !
            crc := wstore.freelist.flush() // then this
            wstore.head.flush(crc) // finally this
        }
    }

    // Still low, then append more free blocks.
    if wstore.freelist.isCritical() {
        wstore.appendBlocks(0, wstore.freelist.limit())
    }
}

func (wstore *WStore) flushCommit(force bool, count int) {
    if count == 0 {
        count = len(wstore.commitQ)
    }
    flushed := make([]int64, 0, count)
    for fpos, node := range wstore.commitQ {
        if force || node.isLeaf() {
            wstore.flushNode(node)
            flushed = append(flushed, fpos)
            count -= 1
            if count == 0 {
                break
            }
        }
    }
    for _, fpos := range flushed {
        delete(wstore.commitQ, fpos)
    }
}

func (wstore *WStore) minAccess() int64 {
    // Shrink accessQ by sliding out demarked access.
    skip := 0
    for i := range wstore.accessQ {
        if wstore.accessQ[i] == 0 {
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
