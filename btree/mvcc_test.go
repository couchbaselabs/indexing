package btree

import (
    "fmt"
    "testing"
)

var _ = fmt.Sprintf("keep 'fmt' import during debugging")

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
