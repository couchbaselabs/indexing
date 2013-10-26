package main

import (
    "fmt"
    "log"
    "github.com/couchbaselabs/indexing/btree"
    "os"
    "time"
)

var _ = fmt.Sprintln("keep 'fmt' import during debugging", time.Now())

func main() {
    idxfile, kvfile := "./data/test_rm_index.dat", "./data/test_rm_kv.dat"
    os.Remove(idxfile)
    os.Remove(kvfile)

    var conf = btree.Config{
        Idxfile: idxfile,
        Kvfile:  kvfile,
        IndexConfig: btree.IndexConfig{
            Sectorsize: 512,
            Flistsize:  1000 * btree.OFFSET_SIZE,
            Blocksize:  512,
        },
        Maxlevel:      6,
        RebalanceThrs: 3,
        AppendRatio:   0.7,
        DrainRate:     200,
        MaxLeafCache:  1000,
        Sync:          false,
        Nocache:       false,
    }
    bt := btree.NewBTree(btree.NewStore(conf))

    factor, count := 100, 10000
    rmcount := 0
    seed := time.Now().UnixNano()
    fmt.Println("Seed:", seed)
    for i := 0; i < 10; i++ {
        rmcount += doinsert(seed+int64(i),factor, count, bt)
        bt.Drain()
        if ((i+1) * factor * count) - rmcount != int(bt.Count()) {
            log.Panicln("mismatch in count", 
                ((i+1) * factor * count) - rmcount, bt.Count())
        }
        bt.Stats(true)
        fmt.Println()
    }
    fmt.Println("count", bt.Count())
    bt.Close()
}

func doinsert(seed int64, factor, count int, bt *btree.BTree) int {
    rmcount := 0
    for i := 0; i < factor; i++ {
        keys, values := btree.TestData(count, seed)
        for j := 0; j < count; j++ {
            k, v := keys[j], values[j]
            k.Id = (i * count) + j
            bt.Insert(k, v)
        }
        fmt.Println("Done ", time.Now().UnixNano()/1000000, (i+1)*count)
        rmcount += doremove(keys, values, bt)
    }
    return rmcount
}

func doremove(keys []*btree.TestKey, values []*btree.TestValue, bt *btree.BTree) int {
    rmcount := 0
    count := len(keys)
    for j := 0; j < count; j += 3 {
        k := keys[j]
        bt.Remove(k)
        rmcount += 1
    }
    for j := 1; j < count; j += 3 {
        k := keys[j]
        bt.Remove(k)
        rmcount += 1
    }
    return rmcount
}
