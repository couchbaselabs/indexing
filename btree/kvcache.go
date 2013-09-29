package btree

import (
    "fmt"
    "os"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

type KVCache struct {
    // In-memory data structure to cache intermediate nodes.
    kvCache map[int64][]byte

    // Communication channel for MVCC goroutine.
    kvreq chan []interface{}
    kvres chan []interface{}
}

func (wstore *WStore) prefetch(fposs []int64) {
    wstore.kvreq <- []interface{}{WS_PREFETCH, fposs}
}

func (wstore *WStore) getKV(fpos int64) []byte {
    wstore.kvreq <- []interface{}{WS_KVREAD, fpos}
    return (<-wstore.kvres)[0].([]byte)
}

func doKV(wstore *WStore) {
    rfd, _ := os.Open(wstore.Kvfile)
    req, res := wstore.kvreq, wstore.kvres
    for {
        cmd := <-req
        if cmd != nil {
            switch cmd[0].(byte) {
            case WS_SAYHI: // say hi!
                res <- []interface{}{WS_SAYHI}
            case WS_CLOSE: // Quit
                res <- nil
                break
            case WS_PREFETCH:
                for _, fpos := range cmd[1].([]int64) {
                    wstore.kvCache[fpos] = wstore.readKV(rfd, fpos)
                }
            case WS_KVREAD:
                var data []byte
                fpos := cmd[1].(int64)
                if data = wstore.kvCache[fpos]; data == nil {
                    data = wstore.readKV(rfd, fpos)
                    wstore.kvCache[fpos] = data
                }
                res <- []interface{}{data}
            }
        } else {
            break
        }
    }
}
