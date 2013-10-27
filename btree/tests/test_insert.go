package main

import (
    "fmt"
    "github.com/couchbaselabs/indexing/btree"
    //"os"
    "time"
)

var _ = fmt.Sprintln("keep 'fmt' import during debugging", time.Now())

func main() {
    idxfile, kvfile := "./data/test_insert_index.dat", "./data/test_insert_kv.dat"
    //os.Remove(idxfile)
    //os.Remove(kvfile)

    var conf = btree.Config{
        Idxfile: idxfile,
        Kvfile:  kvfile,
        IndexConfig: btree.IndexConfig{
            Sectorsize: 512,
            Flistsize:  1000 * btree.OFFSET_SIZE,
            Blocksize:  4 * 1024,
        },
        Maxlevel:      6,
        RebalanceThrs: 25,
        AppendRatio:   0.7,
        DrainRate:     200,
        MaxLeafCache:  1000,
        Sync:          false,
        Nocache:       false,
    }
    bt := btree.NewBTree(btree.NewStore(conf))

    seed := time.Now().UnixNano()
    fmt.Println("Seed:", seed)
    doinsert(seed, 10, 10000, bt, false)
    bt.Drain()
    bt.Stats(true)
    fmt.Println()
    bt.Close()
}

func doinsert(seed int64, factor, count int, bt *btree.BTree, check bool) {
    keys, values := btree.TestData(count, seed)
    for i := 0; i < factor; i++ {
        for j := 0; j < count; j++ {
            k, v := keys[j], values[j]
            k.Id = (i * count) + j
            bt.Insert(k, v)
            if check {
                bt.Drain()
                bt.Check()
            }
        }
        fmt.Println("Done ", time.Now().UnixNano()/1000000, (i+1)*count)
    }
}
