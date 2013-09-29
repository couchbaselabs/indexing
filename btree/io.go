package btree

import (
    "fmt"
    "time"
    "sync/atomic"
)

var _ = fmt.Sprintln("keep 'fmt' import during debugging", time.Now());

type IO struct {
    mvQ []*MV
    commitQ map[int64]Node
}

func mvRoot(store *Store) int64 {
    wstore := store.wstore
    if len(wstore.mvQ) > 0 {
        return wstore.mvQ[len(wstore.mvQ)-1].root
    } else {
        return store.currentRoot()
    }
}

func (wstore *WStore) ccacheLookup(fpos int64) Node {
    node := wstore.commitQ[fpos]
    if node != nil {
        wstore.commitHits += 1
    }
    return node
}

func (wstore *WStore) commit(mv *MV, minAccess int64, force bool) {
    if mv != nil {
        mv.timestamp = time.Now().UnixNano()
        for _, node := range mv.commits {
            wstore.commitQ[node.getKnode().fpos] = node
        }
        wstore.mvQ = append(wstore.mvQ, mv)
    }
    if force || len(wstore.mvQ) > wstore.DrainRate {
        root := atomic.LoadInt64(&wstore.head.root)
        root, staleOffs, commitQ := wstore.drain(root, minAccess)
        cacheQ := wstore.cacheQ()
        wstore.adjustCache(staleOffs, commitQ, cacheQ)
        wstore.freelist.add(staleOffs)
        atomic.StoreInt64(&wstore.head.root, root)
    }
    if force == false  &&  len(wstore.freelist.offsets) < (wstore.Maxlevel*2) {
        offsets := wstore.appendBlocks(0, wstore.appendCount())
        wstore.freelist.add(offsets)
    }
}

func (wstore *WStore) drain(root, minAccess int64) (int64, []int64, []Node) {
    staleOffs := make([]int64, 0, wstore.DrainRate*wstore.Maxlevel)
    commitQ := make([]Node, 0, len(wstore.mvQ)*wstore.Maxlevel)
    skip := 0
    for _, mv := range wstore.mvQ {
        if minAccess != 0  &&  mv.timestamp >= minAccess {
            break
        }
        root = mv.root
        for _, node := range mv.stales {
            fpos := node.getKnode().fpos
            staleOffs = append(staleOffs, fpos)
            delete(wstore.commitQ, fpos)
        }
        for _, node := range mv.commits {
            delete(wstore.commitQ, node.getKnode().fpos)
            commitQ = append(commitQ, node)
        }
        skip += 1
    }
    wstore.mvQ = wstore.mvQ[skip:]
    wstore.flushSnapshot(root, staleOffs, commitQ)
    return root, staleOffs, commitQ
}

func (wstore *WStore) flushSnapshot(root int64, staleOffs []int64, ns []Node) {
    // Sync kv file
    //wstore.kvWfd.Sync()
    for _, node := range ns { // flush nodes first
        wstore.flushNode(node)
    }

    // Cloned freelist
    freelist := wstore.freelist.clone()
    freelist.add(staleOffs)
    crc := freelist.flush() // then this
    // Cloned head
    head := wstore.head.clone()
    head.root = root
    head.flush(crc) // finally this
    //wstore.idxWfd.Sync()
}

func (wstore *WStore) adjustCache(staleOffs []int64, commitQ, cacheQ []Node) {
    nodecache := newNodeCache()
    for fpos, node := range *((*map[int64]Node)(wstore.nodecache)) {
        (*nodecache)[fpos] = node
    }
    leafcache := newNodeCache()
    for fpos, node := range *((*map[int64]Node)(wstore.leafcache)) {
        (*leafcache)[fpos] = node
    }
    for _, node := range cacheQ {
        fpos := node.getKnode().fpos
        if node.isLeaf() {
            (*leafcache)[fpos] = node
        } else {
            (*nodecache)[fpos] = node
        }
    }
    for _, node := range commitQ {
        fpos := node.getKnode().fpos
        if node.isLeaf() {
            (*leafcache)[fpos] = node
        } else {
            (*nodecache)[fpos] = node
        }
    }
    for _, fpos := range staleOffs {
        delete(*nodecache, fpos)
        delete(*leafcache, fpos)
    }
    count := len(*leafcache)
    for fpos, _ := range *leafcache {
        if count <= wstore.MaxLeafCache {
            break
        }
        delete(*leafcache, fpos)
        count -= 1
    }
    wstore.maxlenNC = max(wstore.maxlenNC, int64(len(*nodecache)))
    wstore.maxlenLC = max(wstore.maxlenLC, int64(len(*leafcache)))
    wstore.swapCache(nodecache, leafcache)
}
