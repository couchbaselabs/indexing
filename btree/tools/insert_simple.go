package main

import (
    "fmt"
    "time"
    "sort"
    "os"
    "bytes"
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
            Flistsize: 100 * btree.OFFSET_SIZE,
            Blocksize: 512,
        },
        Maxlevel: 6,
        RebalanceThrs: 6,
        AppendRatio: 0.7,
        Sync: false,
        Nocache: false,
    }
    bt := btree.NewBTree(btree.NewStore(conf))
    count := 10000
    keys, values := btree.TestData(count, 10)
    for i := 0; i < count; i++ {
        k, v := keys[i], values[i]
        k.Id = i
        bt.Insert(k, v)
    }
    bt.Close()

    // Check count.
    bt = btree.NewBTree(btree.NewStore(conf))
    if bt.Count() != int64(count) {
        panic("Count mismatch")
    }

    // Front() API: Get the first key
    frontC, frontK, frontD, frontV := bt.Front()
    fmt.Println(frontC, string(frontK), frontD, frontV)

    // KeySet() API: Check whether sorted
    ch := bt.KeySet()
    prev, kcount := <-ch, 1
    if bytes.Compare(prev, frontK) != 0 {
        panic("Front key does not match")
    }
    for {
        key := <-ch
        if key == nil {
            break
        }
        if bytes.Compare(prev, key) == 1 {
            panic("Not sorted")
        }
        prev = key
        kcount += 1
    }
    if kcount != count {
        panic("KeySet does not return full keys")
    }

    // FullSet() API: Check whether sorted
    ch = bt.FullSet()
    prevKey, prevDocid, _, kcount := <-ch, <-ch, <-ch, 1
    if bytes.Compare(prevKey, frontK) != 0 {
        panic("Front key does not match")
    }
    for {
        key := <-ch
        if key == nil {
            break
        }
        docid, val := <-ch, <-ch
        if bytes.Compare(prevKey, key) == 1 {
            panic("Not sorted")
        }
        if bytes.Equal(prevKey, key) && bytes.Compare(prevDocid, docid) == 1 {
            panic("Not sorted")
        }
        prevKey, prevDocid, _ = key, docid, val
        kcount += 1
    }
    if kcount != count {
        panic("FullSet does not return full keys")
    }

    // Contains(), Equals() API
    for i := 0; i < count; i++ {
        key := *keys[i]
        if bt.Equals(&key) == false {
            panic("Does not equal key")
        }
        if bt.Contains(&key) == false {
            panic("Does not contain key")
        }
        key.Id = -1000
        if bt.Equals(&key) == true {
            panic("Does not expect key")
        }
    }

    // Lookup() API
    for i := 0; i < count; i++ {
        refvals := make([]string, 0)
        for j := 0; j < count; j++ {
            if keys[i].K == keys[j].K {
                refvals = append(refvals, values[j].V)
            }
        }
        keys[i].Id = 0
        ch := bt.Lookup(keys[i])
        vals := make([]string, 0)
        for {
            x := <-ch
            if x == nil {
                break
            }
            vals = append(vals, string(x))
        }
        sort.Strings(refvals)
        sort.Strings(vals)
        if len(refvals) != len(vals) {
            panic("Lookup length mismatch")
        }
        for i := range vals {
            if vals[i] != refvals[i] {
                panic("Lookup value mismatch")
            }
        }
    }
}
