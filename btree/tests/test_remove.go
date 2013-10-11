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

    for i := 0; i < 10; i++ {
        seed := time.Now().UnixNano()
        fmt.Println("Seed:", seed)
        factor := i
        count := i * 1000
        keys, values := btree.TestData(count, seed)
        doinsert(factor, count, keys, values, bt)
        bt.Drain()
        bt.Check()
        doremove(factor, count, keys, values, bt)
        bt.Drain()
        bt.Check()
        bt.Stats()
        fmt.Println("Done ", time.Now().UnixNano()/1000000, factor*count)
        fmt.Println()
    }
    fmt.Println("count", bt.Count())
    bt.Close()
}

func doinsert(factor, count int, keys []*btree.TestKey, values []*btree.TestValue, bt *btree.BTree) {
    for i := 0; i < factor; i++ {
        for j := 0; j < count; j++ {
            k, v := keys[j], values[j]
            k.Id = (i * count) + j
            bt.Insert(k, v)
        }
    }
}

func doremove(factor, count int, keys []*btree.TestKey, values []*btree.TestValue, bt *btree.BTree) {
    checkcount := bt.Count()
    for i := 0; i < factor; i++ {
        for j := 0; j < count; j += 3 {
            k := keys[j]
            k.Id = (i * count) + j
            bt.Remove(k)
            bt.Drain()
            bt.Check()
            checkcount -= 1
            if bt.Count() != checkcount {
                msg := fmt.Sprintf("remove mismatch count %v %v", bt.Count(), checkcount)
                panic(msg)
            }
        }
        for j := 1; j < count; j += 3 {
            k := keys[j]
            k.Id = i*count + j
            bt.Remove(k)
            bt.Drain()
            bt.Check()
            checkcount -= 1
            if bt.Count() != checkcount {
                msg := fmt.Sprintf("remove mismatch count %v %v", bt.Count(), checkcount)
                panic(msg)
            }
        }
    }
}
