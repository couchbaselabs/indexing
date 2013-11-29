package llrb

import (
    "fmt"
    "github.com/couchbaselabs/indexing/api"
)

type IndexLLRB struct {
    name  string
    tree *LLRB
    trait api.TraitInfo
}

var errPurged = fmt.Errorf("Tree is purged")

func NewIndex(name string) api.Finder {
    return &IndexLLRB{name: name, tree: New()}
}

// api.Finder interface
func (index *IndexLLRB) Name() string {
    return index.name
}

func (index *IndexLLRB) Purge() {
    index.tree = nil
}

func (index *IndexLLRB) Trait(operator interface{}) api.TraitInfo {
    switch operator.(type) {
    default:
        return index.trait
    }
}

// api.Counter interface
func (index *IndexLLRB) CountTotal() (uint64, error) {
    if index.tree != nil {
        return uint64(index.tree.Len()), nil
    }
    return 0, errPurged
}

// api.Exister interface
func (index *IndexLLRB) Exists(key api.Key) bool {
    if index.tree != nil {
        return index.tree.Has(key)
    }
    return false
}

// api.Looker interface
func (index *IndexLLRB) Lookup(key api.Key) (chan api.Value, chan error) {
    chval := make(chan api.Value)
    cherr := make(chan error)
    closeChans := func() {
        close(chval)
        close(cherr)
    }
    if index.tree != nil {
        iterfn := func(_ api.Key, value api.Value) bool {
            chval <- value
            return false //TODO: Is it okay to return false ???
        }
        go func() {
            index.tree.Get(key, iterfn)
            closeChans()
        }()
    } else {
        cherr <- errPurged
        closeChans()
    }
    return chval, cherr
}

func (index *IndexLLRB) KeySet() (chan api.Key, chan error) {
    chval := make(chan api.Key)
    cherr := make(chan error)
    closeChans := func() {
        close(chval)
        close(cherr)
    }
    if index.tree != nil {
        iterfn := func(key api.Key, _ api.Value) bool {
            chval <- key
            return false
        }
        go func() {
            index.tree.AscendRange(api.NInf, api.PInf, iterfn)
            closeChans()
        }()
    } else {
        cherr <- errPurged
        closeChans()
    }
    return chval, cherr
}

func (index *IndexLLRB) ValueSet() (chan api.Value, chan error) {
    chval := make(chan api.Value)
    cherr := make(chan error)
    closeChans := func() {
        close(chval)
        close(cherr)
    }
    if index.tree != nil {
        iterfn := func(_ api.Key, value api.Value) bool {
            chval <- value
            return false
        }
        go func() {
            index.tree.AscendRange(api.NInf, api.PInf, iterfn)
            closeChans()
        }()
    } else {
        cherr <- errPurged
        closeChans()
    }
    return chval, cherr
}

func (index *IndexLLRB) KVSet() (chan api.KV, chan error) {
    chval := make(chan api.KV)
    cherr := make(chan error)
    closeChans := func() {
        close(chval)
        close(cherr)
    }
    if index.tree != nil {
        iterfn := func(key api.Key, value api.Value) bool {
            chval <- [2]interface{}{key, value}
            return false
        }
        go func() {
            index.tree.AscendRange(api.NInf, api.PInf, iterfn)
            closeChans()
        }()
    } else {
        cherr <- errPurged
        closeChans()
    }
    return chval, cherr
}


// api.Ranger
func (index *IndexLLRB) KeyRange(low, high api.Key, inclusion api.Inclusion) (
    chan api.Key, chan error, api.SortOrder) {

    chval := make(chan api.Key)
    cherr := make(chan error)
    closeChans := func() {
        close(chval)
        close(cherr)
    }
    if index.tree != nil {
        iterfn := func(key api.Key, _ api.Value) bool {
            chval <- key
            return false
        }
        go func() {
            index.tree.AscendRange(low, high, iterfn)
            closeChans()
        }()
    } else {
        cherr <- errPurged
        closeChans()
    }
    return chval, cherr, api.Asc
}

func (index *IndexLLRB) ValueRange(low, high api.Key, inclusion api.Inclusion) (
    chan api.Value, chan error, api.SortOrder) {

    chval := make(chan api.Value)
    cherr := make(chan error)
    closeChans := func() {
        close(chval)
        close(cherr)
    }
    if index.tree != nil {
        iterfn := func(_ api.Key, value api.Value) bool {
            chval <- value
            return false
        }
        go func() {
            index.tree.AscendRange(low, high, iterfn)
            closeChans()
        }()
    } else {
        cherr <- errPurged
        closeChans()
    }
    return chval, cherr, api.Asc
}

func (index *IndexLLRB) KVRange(low, high api.Key, inclusion api.Inclusion) (
    chan api.KV, chan error, api.SortOrder) {

    chval := make(chan api.KV)
    cherr := make(chan error)
    closeChans := func() {
        close(chval)
        close(cherr)
    }
    if index.tree != nil {
        iterfn := func(key api.Key, value api.Value) bool {
            chval <- [2]interface{}{key, value}
            return false
        }
        go func() {
            index.tree.AscendRange(low, high, iterfn)
            closeChans()
        }()
    } else {
        cherr <- errPurged
        closeChans()
    }
    return chval, cherr, api.Asc
}
