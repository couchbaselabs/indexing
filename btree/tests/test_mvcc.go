package main

import (
    //"bytes"
    "flag"
    "fmt"
    "log"
    "github.com/couchbaselabs/indexing/btree"
    "os"
    "time"
)

var _ = fmt.Sprintln("keep 'fmt' import during debugging", time.Now(), os.O_WRONLY)

var conf = btree.Config{
    Idxfile: "./data/indexfile.dat",
    Kvfile:  "./data/kvfile.dat",
    IndexConfig: btree.IndexConfig{
        Sectorsize: 512,
        Flistsize:  4000 * btree.OFFSET_SIZE,
        Blocksize:  512,
    },
    Maxlevel:      6,
    RebalanceThrs: 3,
    AppendRatio:   0.7,
    DrainRate:     400,
    MaxLeafCache:  1000,
    Sync:          false,
    Nocache:       false,
    Debug:         false,
}

func main() {
    flag.Parse()
    //os.Remove(conf.Idxfile)
    //os.Remove(conf.Kvfile)
    if conf.Debug {
        fd, _ := os.Create("debug")
        log.SetOutput(fd)
    }

    bt := btree.NewBTree(btree.NewStore(conf))

    seed := time.Now().UnixNano()
    fmt.Println("Seed:", seed)

    count, items := 3, 10000
    chans := []chan []interface{}{
        make(chan []interface{}, 100), make(chan []interface{}, 100),
        make(chan []interface{}, 100), make(chan []interface{}, 100),
    }
    endchan := make(chan []interface{}, count)
    check := false
    go doinsert(0, chans[0], chans[1], chans[2], check)
    go dolookup(chans[1], endchan, check)
    go dolookup(chans[1], endchan, check)
    go dolookup(chans[1], endchan, check)
    go dolookup(chans[1], endchan, check)
    go doremove(chans[2], chans[3], check)
    go dolookupNeg(chans[3], endchan, check)
    // Prepopulate
    //for i := 0; i < count; i++ {
    //    keys, values := btree.TestData(items, seed+int64(i))
    //    for i := range keys {
    //        k, v := keys[i], values[i]
    //        k.Id = i
    //        bt.Insert(k, v)
    //    }
    //}
    //bt.Check()
    //log.Println("Prepulated", precount)
    //bt.Stats(true)
    bt.Check()
    go func() {
        for i := 0; i < count; i++ {
            keys, values := btree.TestData(items, seed+int64(i))
            chans[0] <- []interface{}{keys, values}
        }
    }()
    for i := 0; i < count; i++ {
        //bt.Check()
        <-endchan
        <-endchan
        <-endchan
        <-endchan
        <-endchan
        log.Println("Done ... ", i)
    }
    bt.Drain()
    bt.Stats(true)
    log.Println()
    bt.Close()
}

func doinsert(count int, in chan []interface{}, out, outr chan []interface{}, check bool) {
    for {
        cmd := <-in
        bt := btree.NewBTree(btree.NewStore(conf))
        keys, values := cmd[0].([]*btree.TestKey), cmd[1].([]*btree.TestValue)
        for i := range keys {
            k, v := keys[i], values[i]
            k.Id = count
            count++
            bt.Insert(k, v)
        }
        log.Println("Insert ok ...", count)
        if check {
            bt.Check()
        }

        // 33.33% remove
        rmkeys := make([]*btree.TestKey, 0, len(keys))
        rmvals := make([]*btree.TestValue, 0, len(values))
        lkeys := make([]*btree.TestKey, 0, len(keys))
        lvals := make([]*btree.TestValue, 0, len(values))
        for i := 0; i < len(keys); i++ {
            if i%3 == 0 {
                rmkeys = append(rmkeys, keys[i])
                rmvals = append(rmvals, values[i])
            } else {
                lkeys = append(lkeys, keys[i])
                lvals = append(lvals, values[i])
            }
        }
        bt.Close()
        for i := 0; i < 4; i++ {
            out <- []interface{}{lkeys, lvals}
        }
        outr <- []interface{}{rmkeys, rmvals}
    }
}

func dolookup(in chan []interface{}, out chan []interface{}, check bool) {
    keys, values := make([]*btree.TestKey, 0), make([]*btree.TestValue, 0)
    count := 0
    for {
        cmd := <-in
        bt := btree.NewBTree(btree.NewStore(conf))
        keys = append(keys[:len(keys)/4], cmd[0].([]*btree.TestKey)...)
        values = append(values[:len(values)/4], cmd[1].([]*btree.TestValue)...)
        for i := range keys {
            k, v := keys[i], values[i]
            ch := bt.Lookup(k)
            count += 1
            found := false
            vals := make([]string, 0, 100)
            val := <-ch
            for val != nil {
                vals = append(vals, string(val))
                if string(val) == v.V {
                    found = true
                }
                val = <-ch
            }
            if found == false {
                fmt.Println("could not find for ", k, "; expected", v.V, "got", vals)
            }
        }
        if check {
            bt.Check()
        }
        bt.Close()
        out <- []interface{}{keys, values}
    }
}

func doremove(in chan []interface{}, out chan []interface{}, check bool) {
    for {
        cmd := <-in
        bt := btree.NewBTree(btree.NewStore(conf))
        rmkeys, rmvals := cmd[0].([]*btree.TestKey), cmd[1].([]*btree.TestValue)
        for i := 0; i < len(rmkeys); i++ {
            k, _ := rmkeys[i], rmvals[i]
            bt.Remove(k)
        }
        if check {
            bt.Check()
        }
        bt.Close()
        out <- []interface{}{rmkeys, rmvals}
    }
}

func dolookupNeg(in chan []interface{}, out chan []interface{}, check bool) {
    count := 0
    for {
        cmd := <-in
        bt := btree.NewBTree(btree.NewStore(conf))
        keys, values := cmd[0].([]*btree.TestKey), cmd[1].([]*btree.TestValue)
        for i := range keys {
            k := keys[i]
            ch := bt.Lookup(k)
            count += 1
            vals := make([][]byte, 0, 100)
            val := <-ch
            for val != nil {
                vals = append(vals, val)
                val = <-ch
            }
        }
        if check {
            bt.Check()
        }
        bt.Close()
        out <- []interface{}{keys, values}
    }
}
