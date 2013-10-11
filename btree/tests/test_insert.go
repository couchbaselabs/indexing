package main

import (
    "flag"
    "fmt"
    "github.com/couchbaselabs/indexing/btree"
    "os"
    "time"
)

var _ = fmt.Sprintln("keep 'fmt' import during debugging", time.Now())

func main() {
    flag.Parse()
    args := flag.Args()
    idxfile, kvfile := args[0], args[1]
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
        RebalanceThrs: 4,
        AppendRatio:   0.7,
        DrainRate:     200,
        MaxLeafCache:  1000,
        Sync:          false,
        Nocache:       false,
    }
    bt := btree.NewBTree(btree.NewStore(conf))

    for i := 0; i < 100; i++ {
        seed := time.Now().UnixNano()
        fmt.Println("Seed:", seed)
        factor := i
        count := 1000
        doinsert(seed, factor, count, bt, false)
        bt.Drain()
        bt.Check()
        fmt.Println("Done ", time.Now().UnixNano()/1000000, factor*count)
        fmt.Println()
    }
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
    }
    bt.Stats()
}
