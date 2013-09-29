package btree

import (
    "fmt"
    "testing"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging");

func Benchmark_access(b *testing.B) {
    store := testStore(true)
    defer func() {
        store.Destroy()
    }()

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        ts := store.wstore.access()
        store.wstore.release([]Node{}, ts)
    }
}

func Benchmark_cache(b *testing.B) {
    store := testStore(true)
    defer func() {
        store.Destroy()
    }()

    max := store.maxKeys()
    kn := (&knode{}).newNode(store)
    kn.ks = kn.ks[:0]
    kn.vs = kn.vs[:0]
    for i := 0; i < max; i++ {
        kn.ks = append(kn.ks, int64(i))
        kn.ds = append(kn.ds, int64(i))
        kn.vs = append(kn.vs, int64(i))
    }
    kn.vs = append(kn.vs, 0)
    kn.size = max

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        kn.fpos = int64(i)
        store.wstore.cache(kn)
    }
}

func Benchmark_cachelookup(b *testing.B) {
    store := testStore(true)
    defer func() {
        store.Destroy()
    }()

    max := store.maxKeys()
    in := (&inode{}).newNode(store)
    in.ks = in.ks[:0]
    in.vs = in.vs[:0]
    for i := 0; i < max; i++ {
        in.ks = append(in.ks, int64(i))
        in.ds = append(in.ds, int64(i))
        in.vs = append(in.vs, int64(i))
    }
    in.vs = append(in.vs, 0)
    in.size = max

    for i := 0; i < 10000000; i++ {
        in.fpos = int64(i)
        store.wstore.cache(in)
    }
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        store.wstore.cacheLookup(int64(i))
    }
}
