// Copyright 2010 Petar Maymounkov. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package llrb

import (
    "math"
    "math/rand"
    "testing"
    "github.com/couchbaselabs/indexing/api"
)

func TestCases(t *testing.T) {
    tree := New()
    tree.Insert(api.Int(1), api.String("hello"))
    tree.Insert(api.Int(1), api.String("hello"))
    if tree.Len() != 1 {
        t.Errorf("expecting len 1")
    }
    if !tree.Has(api.Int(1)) {
        t.Errorf("expecting to find key=1")
    }

    tree.Delete(api.Int(1))
    if tree.Len() != 0 {
        t.Errorf("expecting len 0")
    }
    if tree.Has(api.Int(1)) {
        t.Errorf("not expecting to find key=1")
    }

    tree.Delete(api.Int(1))
    if tree.Len() != 0 {
        t.Errorf("expecting len 0")
    }
    if tree.Has(api.Int(1)) {
        t.Errorf("not expecting to find key=1")
    }
}

func TestReverseInsertOrder(t *testing.T) {
    tree := New()
    n := 100
    for i := 0; i < n; i++ {
        tree.Insert(api.Int(n - i), api.String("hello"))
    }
    i := 0
    tree.AscendGreaterOrEqual(api.Int(0), func(key api.Key, _ api.Value) bool {
        i++
        if key.(api.Int) != api.Int(i) {
            t.Errorf("bad order: got %d, expect %d", key.(api.Int), i)
        }
        return true
    })
}

func TestRange(t *testing.T) {
    tree := New()
    order := []api.String{
        "ab", "aba", "abc", "a", "aa", "aaa", "b", "a-", "a!",
    }
    for _, i := range order {
        tree.Insert(i, api.String("hello"))
    }
    k := 0
    iterfn := func(key api.Key, _ api.Value) bool {
        if k > 3 {
            t.Fatalf("returned more items than expected")
        }
        i1 := order[k]
        i2 := key.(api.String)
        if i1 != i2 {
            t.Errorf("expecting %s, got %s", i1, i2)
        }
        k++
        return true
    }
    tree.AscendRange(api.String("ab"), api.String("ac"), iterfn)
}

func TestRandomInsertOrder(t *testing.T) {
    tree := New()
    n := 1000
    perm := rand.Perm(n)
    for i := 0; i < n; i++ {
        tree.Insert(api.Int(perm[i]), api.String("hello"))
    }
    j := 0
    tree.AscendGreaterOrEqual(api.Int(0), func(key api.Key, _ api.Value) bool {
        if key.(api.Int) != api.Int(j) {
            t.Fatalf("bad order")
        }
        j++
        return true
    })
}

func TestRandomReplace(t *testing.T) {
    tree := New()
    n := 100
    perm := rand.Perm(n)
    for i := 0; i < n; i++ {
        tree.Insert(api.Int(perm[i]), api.String("hello"))
    }
    perm = rand.Perm(n)
    for i := 0; i < n; i++ {
        if kv := tree.Insert(api.Int(perm[i]), api.String("hello"));
            kv[0] == nil || kv[0].(api.Int) != api.Int(perm[i]) {

            t.Errorf("error replacing")
        }
    }
}

func TestRandomInsertSequentialDelete(t *testing.T) {
    tree := New()
    n := 1000
    perm := rand.Perm(n)
    for i := 0; i < n; i++ {
        tree.Insert(api.Int(perm[i]), api.String("hello"))
    }
    for i := 0; i < n; i++ {
        tree.Delete(api.Int(i))
    }
}

func TestRandomInsertDeleteNonExistent(t *testing.T) {
    tree := New()
    n := 100
    perm := rand.Perm(n)
    for i := 0; i < n; i++ {
        tree.Insert(api.Int(perm[i]), api.String("hello"))
    }
    if tree.Delete(api.Int(200))[0] != nil {
        t.Errorf("deleted non-existent item")
    }
    if tree.Delete(api.Int(-2))[0] != nil {
        t.Errorf("deleted non-existent item")
    }
    for i := 0; i < n; i++ {
        if kv := tree.Delete(api.Int(i)); kv[0] == nil ||
            kv[0].(api.Int) != api.Int(i) {

            t.Errorf("delete failed")
        }
    }
    if tree.Delete(api.Int(200))[0] != nil {
        t.Errorf("deleted non-existent item")
    }
    if tree.Delete(api.Int(-2))[0] != nil {
        t.Errorf("deleted non-existent item")
    }
}

func TestRandomInsertPartialDeleteOrder(t *testing.T) {
    tree := New()
    n := 100
    perm := rand.Perm(n)
    for i := 0; i < n; i++ {
        tree.Insert(api.Int(perm[i]), api.String("hello"))
    }
    for i := 1; i < n-1; i++ {
        tree.Delete(api.Int(i))
    }
    j := 0
    tree.AscendGreaterOrEqual(api.Int(0), func(key api.Key, value api.Value) bool {
        switch j {
        case 0:
            if key.(api.Int) != api.Int(0) {
                t.Errorf("expecting 0")
            }
        case 1:
            if key.(api.Int) != api.Int(n-1) {
                t.Errorf("expecting %d", n-1)
            }
        }
        j++
        return true
    })
}

func TestRandomInsertStats(t *testing.T) {
    tree := New()
    n := 100000
    perm := rand.Perm(n)
    for i := 0; i < n; i++ {
        tree.Insert(api.Int(perm[i]), api.String("hello"))
    }
    avg, _ := tree.HeightStats()
    expAvg := math.Log2(float64(n)) - 1.5
    if math.Abs(avg-expAvg) >= 2.0 {
        t.Errorf("too much deviation from expected average height")
    }
}

func BenchmarkInsert(b *testing.B) {
    tree := New()
    count := 1000000
    for i := 0; i < count; i++ {
        tree.Insert(api.Int(count - i), api.String("hello"))
    }
    b.ResetTimer()
    for i := 1; i < b.N; i++ {
        tree.Insert(api.Int(count+i), api.String("hello"))
    }
}

func BenchmarkDelete(b *testing.B) {
    tree := New()
    count := 1000000
    for i := 0; i < count; i++ {
        tree.Insert(api.Int(count - i), api.String("hello"))
    }
    b.ResetTimer()
    for i := 1; i < b.N; i++ {
        tree.Insert(api.Int(count+i), api.String("hello"))
        tree.Delete(api.Int(count+i))
    }
}

func BenchmarkDeleteMin(b *testing.B) {
    b.StopTimer()
    tree := New()
    for i := 0; i < b.N; i++ {
        tree.Insert(api.Int(b.N - i), api.String("hello"))
    }
    b.StartTimer()
    for i := 0; i < b.N; i++ {
        tree.DeleteMin()
    }
}

func TestInsertNoReplace(t *testing.T) {
    tree := New()
    n := 1000
    for q := 0; q < 2; q++ {
        perm := rand.Perm(n)
        for i := 0; i < n; i++ {
            tree.Insert(api.Int(perm[i]), api.String("hello"))
        }
    }
    j := 0
    tree.AscendGreaterOrEqual(api.Int(0), func(key api.Key, _ api.Value) bool {
        if key.(api.Int) != api.Int(j) {
            t.Fatalf("bad order")
        }
        j++
        return true
    })
}
