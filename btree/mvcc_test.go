package btree

import (
    "testing"
)

func Benchmark_access(b *testing.B) {
    store := testStore(true)
    defer func() {
        store.Destroy()
    }()

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        ts := store.wstore.access()
        store.wstore.release(ts)
    }
}
