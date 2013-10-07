package btree

import (
    "fmt"
    "time"
)

var _ = fmt.Sprintln("keep 'fmt' import during debugging", time.Now());

type IO struct {
    mvQ []*MV
    commitQ map[int64]Node
}

func mvRoot(store *Store) int64 {
    wstore := store.wstore
    //fmt.Println("mvRoot", wstore.mvQ)
    if len(wstore.mvQ) > 0 {
        //fmt.Println("mvRoot", wstore.mvQ[len(wstore.mvQ)-1].root)
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
        for _, node := range mv.commits {
            wstore.commitQ[node.getKnode().fpos] = node
        }
        wstore.pingMV(mv)
        wstore.mvQ = append(wstore.mvQ, mv)
    }
    if force || len(wstore.mvQ) > wstore.DrainRate {
        wstore.syncSnapshot(minAccess)
        wstore.commitQ = make(map[int64]Node)
    }
    if force == false  &&  len(wstore.freelist.offsets) < (wstore.Maxlevel*2) {
        offsets := wstore.appendBlocks(0, wstore.appendCount())
        wstore.freelist.add(offsets)
    }
}

func (wstore *WStore) flushSnapshot(commitQ map[int64]Node, offsets []int64, root int64) {

    // Sync kv file
    wstore.kvWfd.Sync()
    for _, node := range commitQ { // flush nodes first
        wstore.flushNode(node)
    }

    // Cloned freelist
    freelist := wstore.freelist.clone()
    freelist.add(offsets)
    crc := freelist.flush() // then this
    // Cloned head
    head := wstore.head.clone()
    head.root = root
    head.flush(crc) // finally this
    wstore.idxWfd.Sync()
}
