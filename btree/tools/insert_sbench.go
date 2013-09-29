package main

import (
    "fmt"
    "os"
    //"runtime/pprof"
    "time"
    "flag"
    "github.com/couchbaselabs/indexing/btree"
)

var _ = fmt.Sprintln("keep 'fmt' import during debugging", time.Now(), os.O_WRONLY);

func main() {
    flag.Parse()
    args := flag.Args()
    idxfile, kvfile := args[0], args[1]
    os.Remove(idxfile); os.Remove(kvfile)

    var conf = btree.Config{
        Idxfile: idxfile,
        Kvfile: kvfile,
        IndexConfig: btree.IndexConfig{
            Sectorsize: 512,
            Flistsize: 2000 * btree.OFFSET_SIZE,
            Blocksize: 4*1024,
        },
        Maxlevel: 6,
        RebalanceThrs: 6,
        AppendRatio: 0.7,
        DrainRate: 600,
        MaxLeafCache: 20000,
        Sync: false,
        Nocache: false,
    }
    store := btree.NewStore(conf)
    bt := btree.NewBTree(store)
    count := 300
    factor := 10000
    seed := int64(1380284350686509703) // time.Now().UnixNano()
    fmt.Println("Seed:", seed)
    keys, values := btree.TestData(10000, seed)
    fmt.Println(time.Now())
    for i := 0; i < count; i++ {
        for j := 0; j < factor; j++ {
            k, v := keys[j], values[j]
            k.Id = (i*factor) + j
            bt.Insert(k, v)
        }
        fmt.Println("Done ", time.Now().UnixNano()/1000000, (i+1)*factor)
    }
    bt.Drain()
    fmt.Println(time.Now())
    // Sanity check
    if bt.Count() != int64(count*factor) {
        fmt.Println(bt.Count(), int64(count*factor))
        panic("Count mismatch")
    }
    // Remove
    //for i := 0; i < count; i++ {
    //    for j := 0; j < factor; j+=3 {
    //        k := keys[j]
    //        k.Id = (i*factor) + j
    //        bt.Remove(k)
    //    }
    //    for j := 1; j < factor; j+=3 {
    //        k := keys[j]
    //        k.Id = (i*factor) + j
    //        bt.Remove(k)
    //    }
    //    for j := 2; j < factor; j+=3 {
    //        k := keys[j]
    //        k.Id = (i*factor) + j
    //        bt.Remove(k)
    //    }
    //    fmt.Println("Done ", time.Now().UnixNano()/1000000 , (i+1)*factor)
    //}
    //bt.Drain()
    bt.Stats()
    fmt.Println("Count", bt.Count())
    bt.Close()
}
