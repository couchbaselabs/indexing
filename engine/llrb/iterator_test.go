package llrb

import (
    "reflect"
    "testing"
    "github.com/couchbaselabs/indexing/api"
)

func TestAscendGreaterOrEqual(t *testing.T) {
    tree := New()
    tree.Add(api.Int64(4), api.String("hello"))
    tree.Add(api.Int64(6), api.String("world"))
    tree.Add(api.Int64(1), api.String("how"))
    tree.Add(api.Int64(3), api.String("are you"))
    var ary []api.Int64
    tree.AscendGreaterOrEqual(api.NegInf, func(key api.Key, _ api.Value) bool {
        ary = append(ary, key.(api.Int64))
        return true
    })
    expected := []api.Int64{api.Int64(1), api.Int64(3), api.Int64(4),
    api.Int64(6)}
    if !reflect.DeepEqual(ary, expected) {
        t.Errorf("expected %v but got %v", expected, ary)
    }
    ary = nil
    tree.AscendGreaterOrEqual(api.Int64(3), func(key api.Key, _ api.Value) bool {
        ary = append(ary, key.(api.Int64))
        return true
    })
    expected = []api.Int64{api.Int64(3), api.Int64(4), api.Int64(6)}
    if !reflect.DeepEqual(ary, expected) {
        t.Errorf("expected %v but got %v", expected, ary)
    }
    ary = nil
    tree.AscendGreaterOrEqual(api.Int64(2), func(key api.Key, _ api.Value) bool {
        ary = append(ary, key.(api.Int64))
        return true
    })
    expected = []api.Int64{api.Int64(3), api.Int64(4), api.Int64(6)}
    if !reflect.DeepEqual(ary, expected) {
        t.Errorf("expected %v but got %v", expected, ary)
    }
}

func TestDescendLessOrEqual(t *testing.T) {
    tree := New()
    tree.Add(api.Int64(4), api.String("hello"))
    tree.Add(api.Int64(6), api.String("world"))
    tree.Add(api.Int64(1), api.String("how"))
    tree.Add(api.Int64(3), api.String("are you"))
    var ary []api.Int64
    tree.DescendLessOrEqual(api.Int64(10), func(key api.Key, _ api.Value) bool {
        ary = append(ary, key.(api.Int64))
        return true
    })
    expected := []api.Int64{api.Int64(6), api.Int64(4), api.Int64(3), api.Int64(1)}
    if !reflect.DeepEqual(ary, expected) {
        t.Errorf("expected %v but got %v", expected, ary)
    }
    ary = nil
    tree.DescendLessOrEqual(api.Int64(4), func(key api.Key, _ api.Value) bool {
        ary = append(ary, key.(api.Int64))
        return true
    })
    expected = []api.Int64{api.Int64(4), api.Int64(3), api.Int64(1)}
    if !reflect.DeepEqual(ary, expected) {
        t.Errorf("expected %v but got %v", expected, ary)
    }
    ary = nil
    tree.DescendLessOrEqual(api.Int64(5), func(key api.Key, _ api.Value) bool {
        ary = append(ary, key.(api.Int64))
        return true
    })
    expected = []api.Int64{api.Int64(4), api.Int64(3), api.Int64(1)}
    if !reflect.DeepEqual(ary, expected) {
        t.Errorf("expected %v but got %v", expected, ary)
    }
}
