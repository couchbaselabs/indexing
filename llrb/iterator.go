package llrb

import (
    "github.com/couchbaselabs/indexing/api"
)

func (t *LLRB) AscendRange(greaterOrEqual, lessThan api.Key, iterator Iterator) {
    t.ascendRange(t.root, greaterOrEqual, lessThan, iterator)
}

func (t *LLRB) ascendRange(h *Node, inf, sup api.Key, iterator Iterator) bool {
    if h == nil {
        return true
    }
    if !less(h.Key, sup) {
        return t.ascendRange(h.Left, inf, sup, iterator)
    }
    if less(h.Key, inf) {
        return t.ascendRange(h.Right, inf, sup, iterator)
    }

    if !t.ascendRange(h.Left, inf, sup, iterator) {
        return false
    }
    if !iterator(h.Key, h.Value) {
        return false
    }
    return t.ascendRange(h.Right, inf, sup, iterator)
}

// AscendGreaterOrEqual will call iterator once for each element greater or equal to
// pivot in ascending order. It will stop whenever the iterator returns false.
func (t *LLRB) AscendGreaterOrEqual(pivot api.Key, iterator Iterator) {
    t.ascendGreaterOrEqual(t.root, pivot, iterator)
}

func (t *LLRB) ascendGreaterOrEqual(h *Node, pivot api.Key, iterator Iterator) bool {
    if h == nil {
        return true
    }
    if !less(h.Key, pivot) {
        if !t.ascendGreaterOrEqual(h.Left, pivot, iterator) {
            return false
        }
        if !iterator(h.Key, h.Value) {
            return false
        }
    }
    return t.ascendGreaterOrEqual(h.Right, pivot, iterator)
}

func (t *LLRB) AscendLessThan(pivot api.Key, iterator Iterator) {
    t.ascendLessThan(t.root, pivot, iterator)
}

func (t *LLRB) ascendLessThan(h *Node, pivot api.Key, iterator Iterator) bool {
    if h == nil {
        return true
    }
    if !t.ascendLessThan(h.Left, pivot, iterator) {
        return false
    }
    if !iterator(h.Key, h.Value) {
        return false
    }
    if less(h.Key, pivot) {
        return t.ascendLessThan(h.Left, pivot, iterator)
    }
    return true
}

// DescendLessOrEqual will call iterator once for each element less than the
// pivot in descending order. It will stop whenever the iterator returns false.
func (t *LLRB) DescendLessOrEqual(pivot api.Key, iterator Iterator) {
    t.descendLessOrEqual(t.root, pivot, iterator)
}

func (t *LLRB) descendLessOrEqual(h *Node, pivot api.Key, iterator Iterator) bool {
    if h == nil {
        return true
    }
    if less(h.Key, pivot) || !less(pivot, h.Key) {
        if !t.descendLessOrEqual(h.Right, pivot, iterator) {
            return false
        }
        if !iterator(h.Key, h.Value) {
            return false
        }
    }
    return t.descendLessOrEqual(h.Left, pivot, iterator)
}
