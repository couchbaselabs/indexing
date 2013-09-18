package main

import (
    "fmt"
    "os"
    "time"
    "flag"
    "github.com/couchbaselabs/indexing/btree"
)

var _ = fmt.Sprintln("keep 'fmt' import during debugging", time.Now());

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
            Flistsize: 1000 * btree.OFFSET_SIZE,
            Blocksize: 64*1024,
        },
        Maxlevel: 6,
        RebalanceThrs: 6,
        AppendRatio: 0.7,
        Sync: false,
        Nocache: false,
    }
    bt := btree.NewBTree(btree.NewStore(conf))
    count := 500
    factor := 1000
    fmt.Println(time.Now())
    for i := 0; i < count; i++ {
        keys, values := btree.TestData(factor, -1)
        for j := 0; j < factor; j++ {
            k, v := keys[j], values[j]
            k.Id = (i*factor) + j
            bt.Insert(k, v)
        }
    }
    fmt.Println(time.Now())
    bt.Stats()
    bt.Close()
}
