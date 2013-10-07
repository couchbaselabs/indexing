package main

import (
    "fmt"
    "time"
    "os"
    //"runtime/pprof"
    "flag"
    "github.com/couchbaselabs/indexing/btree"
)

var _ = fmt.Sprintln("keep 'fmt' import during debugging", time.Now());
var conf = btree.Config{
    Idxfile: "./data/indexfile.dat",
    Kvfile: "./data/kvfile.dat",
    IndexConfig: btree.IndexConfig{
        Sectorsize: 512,
        Flistsize: 1000 * btree.OFFSET_SIZE,
        Blocksize: 512,
    },
    Maxlevel: 6,
    RebalanceThrs: 5,
    AppendRatio: 0.7,
    DrainRate: 200,
    MaxLeafCache: 1000,
    Sync: false,
    Nocache: false,
}

func main() {
    flag.Parse()
    //idxfile, kvfile := args[0], args[1]
    os.Remove(conf.Idxfile); os.Remove(conf.Kvfile)
    //cpuprof, _ := os.Create("cpuprof")
    //memprof, _ := os.Create("memprof")

    bt := btree.NewBTree(btree.NewStore(conf))

    //pprof.StartCPUProfile(cpuprof)
    //pprof.WriteHeapProfile(memprof)
    //defer pprof.StopCPUProfile()

    seed := time.Now().UnixNano()
    seed = int64(1381051359196030165)
    fmt.Println("Seed:", seed)

    count, items := 2, 100
    chans := []chan []interface{} {
        make(chan []interface{}), make(chan []interface{}),
        make(chan []interface{}), make(chan []interface{}),
    }
    endchan := make(chan []interface{}, count)
    check := false
    go doinsert(chans[0], chans[1], true)
    go dolookup(chans[1], endchan, check)
    //go dolookup(chans[1], chans[2], check)
    //go dolookup(chans[1], chans[2], check)
    //go dolookup(chans[1], chans[2], check)
    //go doremove(chans[2], chans[3], check)
    //go dolookupNeg(chans[3], endchan, check)
    for i := 0; i < count; i++ {
        keys, values := btree.TestData(items, seed+int64(i))
        chans[0] <- []interface{}{keys, values}
        //<-endchan
    }
    for i := 0; i < count; i++ {
        <-endchan
    }
    fmt.Println("Final Count", bt.Count())
    bt.Close()
}

func doinsert(in chan []interface{}, out chan []interface{}, check bool) {
    bt := btree.NewBTree(btree.NewStore(conf))
    count := 0
    for {
        cmd := <-in
        keys, values := cmd[0].([]*btree.TestKey), cmd[1].([]*btree.TestValue)
        for i := range keys {
            k, v := keys[i], values[i]
            k.Id = count
            count++
            fmt.Println("insert", k, v)
            bt.Insert(k, v)
            if check {
                bt.Check()
            }
        }
        bt.Drain()
        fmt.Println("Count", bt.Count(), len(keys))
        bt.Stats()
        bt.Show()
        fmt.Println()
        out <- []interface{}{keys, values}
        btree.Debug = true
    }
}

func dolookup(in chan []interface{}, out chan []interface{}, check bool) {
    bt := btree.NewBTree(btree.NewStore(conf))
    for {
        cmd := <-in
        keys, values := cmd[0].([]*btree.TestKey), cmd[1].([]*btree.TestValue)
        for i := range keys {
            k, v := keys[i], values[i]
            fmt.Println("lookup", k, v)
            ch := bt.Lookup(k)
            val := <-ch
            if string(val) != v.V {
                fmt.Println(k, "Got", string(val), v.V);
                panic("lookup value mismatch")
            }
            for val != nil {
                val = <-ch
            }
            if check {
                bt.Check()
            }
        }
        out <- []interface{}{keys, values}
    }
}

func doremove(in chan []interface{}, out chan []interface{}, check bool) {
    bt := btree.NewBTree(btree.NewStore(conf))
    for {
        cmd := <-in
        keys, values := cmd[0].([]*btree.TestKey), cmd[1].([]*btree.TestValue)
        rmkeys := make([]*btree.TestKey, 0, len(keys))
        for i := 0; i < len(keys)/3; i++ {
            k, _ := keys[i], values[i]
            bt.Remove(k)
            if check {
                bt.Check()
            }
            rmkeys = append(rmkeys, k)
        }
        bt.Drain()
        out <- []interface{}{rmkeys, values}
    }
}

func dolookupNeg(in chan []interface{}, out chan []interface{}, check bool) {
    bt := btree.NewBTree(btree.NewStore(conf))
    for {
        cmd := <-in
        keys, values := cmd[0].([]*btree.TestKey), cmd[1].([]*btree.TestValue)
        for i := range keys {
            k, _ := keys[i], values[i]
            ch := bt.Lookup(k)
            val := <-ch
            if val != nil {
                fmt.Println("Got %v for nil", string(val))
                panic("lookupF value mismatch")
            }
            if check {
                bt.Check()
            }
        }
        out <- []interface{}{keys, values}
    }
}
