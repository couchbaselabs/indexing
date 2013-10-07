package btree

import (
    "fmt"
    "sync/atomic"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

const (
    DEFER_ADD byte = iota
    DEFER_DELETE
)

type DEFER struct {
    deferReq chan []interface{}
}

func (wstore *WStore) pingCache(what byte, fpos int64, node Node) {
    wstore.deferReq <- []interface{}{WS_PINGCACHE, what, fpos, node}
}

func (wstore *WStore) pingKey(what byte, fpos int64, key []byte) {
    wstore.deferReq <- []interface{}{WS_PINGKD, what, fpos, key}
}

func (wstore *WStore) pingDocid(what byte, fpos int64, docid []byte) {
    wstore.deferReq <- []interface{}{WS_PINGKD, what, fpos, docid}
}

func (wstore *WStore) pingMV(mv *MV) {
    wstore.deferReq <- []interface{}{WS_MV, mv}
}

func (wstore *WStore) syncSnapshot(minAccess int64) {
    syncChan := make(chan interface{})
    wstore.deferReq <- []interface{}{WS_SYNCSNAPSHOT, minAccess, syncChan}
    <-syncChan
}

func doDefer(wstore *WStore) {
    var cmd []interface{}
    commitQ := make(map[int64]Node)
    recycleQ := make([]int64, 0, wstore.DrainRate*wstore.Maxlevel)
    addKDs := make(map[int64][]byte)
    delKDs := make(map[int64][]byte)
    for {
        cmd = <-wstore.deferReq
        if cmd != nil {
            switch cmd[0].(byte) {

            case WS_PINGCACHE:
                what, fpos, node := cmd[1].(byte), cmd[2].(int64), cmd[3].(Node)
                if what == DEFER_ADD {
                    wstore._pingCache(fpos, node)
                }

            case WS_PINGKD:
                what, fpos, v := cmd[1].(byte), cmd[2].(int64), cmd[3].([]byte)
                kdping := (*map[int64][]byte)(atomic.LoadPointer(&wstore.kdping))
                if what == DEFER_ADD {
                    addKDs[fpos] = v
                    (*kdping)[fpos] = v
                } else if what == DEFER_DELETE {
                    delKDs[fpos] = v
                    delete(*kdping, fpos)
                }

            case WS_MV:
                mv := cmd[1].(*MV)
                for _, node := range mv.commits { // update commitQ and ping cache
                    fpos := node.getKnode().fpos
                    commitQ[fpos] = node
                    wstore._pingCache(fpos, node)
                }
                stales := make([]int64, 0, len(mv.stales))
                for _, fpos := range mv.stales {
                    if commitQ[fpos] != nil { // Recyle stalenodes
                        recycleQ = append(recycleQ, fpos)
                        delete(commitQ, fpos) // prune commitQ
                        wstore._pingCacheEvict(fpos)
                    } else { // stalenodes to be reclaimed
                        stales = append(stales, fpos)
                    }
                }
                mv.stales = stales // Only stalenodes that need to be reclaimed

            case WS_SYNCSNAPSHOT :
                minAccess, syncChan := cmd[1].(int64), cmd[2].(chan interface{})
                reclaimQ := make([]int64, 0, wstore.DrainRate)
                offsets := make([]int64, 0, wstore.DrainRate)
                offsets = append(offsets, recycleQ...)
                wstore.recycleCount += int64(len(recycleQ))
                if len(commitQ) > 0 {
                    mv := wstore.mvQ[len(wstore.mvQ)-1]
                    if len(wstore.mvQ) < 1 {
                        panic("If commitQ is dirty, mvQ should also be dirty")
                    }
                    // Actual Reclaim
                    if minAccess == 0  ||  minAccess > wstore.head.timestamp {
                        skip := 0
                        for _, mvp := range wstore.mvQ {
                            //fmt.Println("loop", mvp.timestamp, wstore.head.timestamp)
                            if mvp.timestamp < wstore.head.timestamp {
                                skip += 1
                                reclaimQ = append(reclaimQ, mvp.stales...)
                            } else {
                                break
                            }
                        }
                        wstore.mvQ = wstore.mvQ[skip:]
                    }
                    // Reclaim file-positions into free-list.
                    offsets = append(offsets, reclaimQ...)
                    wstore.reclaimCount += int64(len(reclaimQ))

                    // Adjust commitQ and ping cache before pingpong
                    for _, fpos := range reclaimQ {
                        delete(commitQ, fpos)
                        wstore._pingCacheEvict(fpos)
                    }

                    //fmt.Println("snap", offsets, len(offsets), mv.root)
                    //fmt.Println("reclaimQ", reclaimQ)
                    //wstore.displayPing()
                    wstore.flushSnapshot(commitQ, offsets, mv.root)
                    wstore.setSnapShot(offsets, mv.root, mv.timestamp)

                    // Update btree's ping cache
                    for fpos, node := range commitQ {
                        wstore._pingCache(fpos, node)
                    }
                    for _, fpos := range recycleQ {
                        wstore._pingCacheEvict(fpos)
                    }
                    for _, fpos := range reclaimQ {
                        wstore._pingCacheEvict(fpos)
                    }

                    // Update KD's ping cache.
                    kdping := (*map[int64][]byte)(atomic.LoadPointer(&wstore.kdping))
                    for fpos, v := range addKDs {
                        (*kdping)[fpos] = v
                    }
                    for fpos, _ := range delKDs {
                        delete(*kdping, fpos)
                    }

                    //wstore.displayPing()
                    //wstore.checkPingPong()
                }
                // Reset
                commitQ = make(map[int64]Node)
                recycleQ = make([]int64, 0, wstore.DrainRate*wstore.Maxlevel)
                addKDs = make(map[int64][]byte)
                delKDs = make(map[int64][]byte)
                syncChan <- nil

            case WS_CLOSE: // Quit
                syncChan := cmd[1].(chan interface{})
                syncChan <- nil
            }
        } else {
            break
        }
    }
}
