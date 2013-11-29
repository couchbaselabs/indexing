// Defines different types of keys, like negative-infinity, positive-infinity,
// integer, string, bytes, json etc ...
//
// All the key types must implement `Key` interface.

package api

import (
    "bytes"
    "encoding/binary"
)

var (
    NInf = nInf{}
    PInf = pInf{}
    infs = map[int]Key{1: PInf, -1: NInf}
)

// Inf returns a Key that is "bigger than" any other item, if sign is positive.
// Otherwise  it returns a Key that is "smaller than" any other item.
func Inf(sign int) Key {
    return infs[sign]
}


// Negative infinity
type nInf struct{}

func (nInf) Less(Key) bool {
    return true
}

func (nInf) Bytes() []byte {
    return []byte{}
}

// Positive inifinity
type pInf struct{}

func (pInf) Less(Key) bool {
    return false
}

func (pInf) Bytes() []byte {
    return []byte{}
}

// []byte
type Bytes []byte

func (this Bytes) Less(than Key) (rc bool) {
    switch than.(type) {
    case nInf:
        rc = false
    case Bytes:
        rc = bytes.Compare(this, than.(Bytes)) < 0
    case pInf:
        rc = true
    }
    return
}

func (this Bytes) Bytes() []byte {
    return []byte(this)
}

// JSON
type JSON []byte

func (this JSON) Less(than Key) (rc bool) { // TODO: To be completed
    switch than.(type) {
    case nInf:
        rc = false
    case JSON:
        rc = false
    case pInf:
        rc = true
    }
    return
}

func (this JSON) Bytes() []byte {
    return []byte(this)
}

// Int is UInt64
type Int int64

func (x Int) Less(than Key) (rc bool) {
    switch than.(type) {
    case nInf:
        rc = false
    case Int:
        rc = x < than.(Int)
    case pInf:
        rc = true
    }
    return
}

func (x Int) Bytes() []byte {
    data := make([]byte, 8)
    binary.PutVarint(data, int64(x))
    return data
}

// String
type String string

func (x String) Less(than Key) (rc bool) {
    switch than.(type) {
    case nInf:
        rc = false
    case Int:
        rc = x < than.(String)
    case pInf:
        rc = true
    }
    return
}

func (x String) Bytes() []byte {
    return []byte(x)
}
