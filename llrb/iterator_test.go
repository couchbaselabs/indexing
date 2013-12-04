package llrb

import (
    "reflect"
    "testing"
    "github.com/couchbaselabs/indexing/api"
)

func TestAscendGreaterOrEqual(t *testing.T) {
    tree := New()
    tree.Add(api.Int(4), api.String("hello"))
    tree.Add(api.Int(6), api.String("world"))
    tree.Add(api.Int(1), api.String("how"))
    tree.Add(api.Int(3), api.String("are you"))
    var ary []api.Int
    tree.AscendGreaterOrEqual(api.NegInf, func(key api.Key, _ api.Value) bool {
        ary = append(ary, key.(api.Int))
        return true
    })
    expected := []api.Int{api.Int(1), api.Int(3), api.Int(4), api.Int(6)}
    if !reflect.DeepEqual(ary, expected) {
        t.Errorf("expected %v but got %v", expected, ary)
    }
    ary = nil
    tree.AscendGreaterOrEqual(api.Int(3), func(key api.Key, _ api.Value) bool {
        ary = append(ary, key.(api.Int))
        return true
    })
    expected = []api.Int{api.Int(3), api.Int(4), api.Int(6)}
    if !reflect.DeepEqual(ary, expected) {
        t.Errorf("expected %v but got %v", expected, ary)
    }
    ary = nil
    tree.AscendGreaterOrEqual(api.Int(2), func(key api.Key, _ api.Value) bool {
        ary = append(ary, key.(api.Int))
        return true
    })
    expected = []api.Int{api.Int(3), api.Int(4), api.Int(6)}
    if !reflect.DeepEqual(ary, expected) {
        t.Errorf("expected %v but got %v", expected, ary)
    }
}

func TestDescendLessOrEqual(t *testing.T) {
    tree := New()
    tree.Add(api.Int(4), api.String("hello"))
    tree.Add(api.Int(6), api.String("world"))
    tree.Add(api.Int(1), api.String("how"))
    tree.Add(api.Int(3), api.String("are you"))
    var ary []api.Int
    tree.DescendLessOrEqual(api.Int(10), func(key api.Key, _ api.Value) bool {
        ary = append(ary, key.(api.Int))
        return true
    })
    expected := []api.Int{api.Int(6), api.Int(4), api.Int(3), api.Int(1)}
    if !reflect.DeepEqual(ary, expected) {
        t.Errorf("expected %v but got %v", expected, ary)
    }
    ary = nil
    tree.DescendLessOrEqual(api.Int(4), func(key api.Key, _ api.Value) bool {
        ary = append(ary, key.(api.Int))
        return true
    })
    expected = []api.Int{api.Int(4), api.Int(3), api.Int(1)}
    if !reflect.DeepEqual(ary, expected) {
        t.Errorf("expected %v but got %v", expected, ary)
    }
    ary = nil
    tree.DescendLessOrEqual(api.Int(5), func(key api.Key, _ api.Value) bool {
        ary = append(ary, key.(api.Int))
        return true
    })
    expected = []api.Int{api.Int(4), api.Int(3), api.Int(1)}
    if !reflect.DeepEqual(ary, expected) {
        t.Errorf("expected %v but got %v", expected, ary)
    }
}
