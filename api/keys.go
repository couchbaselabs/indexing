// Defines different types of keys, like negative-infinity, positive-infinity,
// integer, string, bytes, json etc ...
//
// All the key types must implement `Key` interface.

package api

/*
import (
    "bytes"
    "encoding/binary"
)

type Ord int8

const (
    LT    Ord = -1
    EQUAL Ord = 0
    GT    Ord = 1
)

var (
    NegInf = NInf{}
    PosInf = PInf{}
    infs = map[int]Key{1: PosInf, -1: NegInf}
)

// Inf returns a Key that is "bigger than" any other item, if sign is positive.
// Otherwise  it returns a Key that is "smaller than" any other item.
func Inf(sign int) Key {
    return infs[sign]
}


// Negative infinity
type NInf struct{}

func (NInf) Less(Key) bool {
    return true
}

func (NInf) Compare(Key) Ord {
    return EQUAL
}

func (NInf) Bytes() []byte {
    return []byte{}
}

// Positive inifinity
type PInf struct{}

func (PInf) Less(Key) bool {
    return false
}

func (PInf) Compare(Key) Ord {
    return EQUAL
}

func (PInf) Bytes() []byte {
    return []byte{}
}

// []byte
type Bytes []byte

func (this Bytes) Compare(than Key) (result Ord) {
    switch than.(type) {
    case NInf:
        result = GT
    case Bytes:
        switch bytes.Compare(this, than.(Bytes)) {
        case -1:
            result = LT
        case 0:
            result = EQUAL
        case 1:
            result = GT
        }
    case PInf:
        result = LT
    }
    return
}

func (this Bytes) Less(than Key) (rc bool) {
    return this.Compare(than) == LT
}

func (this Bytes) Bytes() []byte {
    return []byte(this)
}

// JSON
type JSON []byte

func (this JSON) Less(than Key) (rc bool) { // TODO: Incompleted
    return this.Compare(than) == LT
}

func (this JSON) Compare(than Key) Ord { // TODO: Incomplete
    return LT
}

func (this JSON) Bytes() []byte {
    return []byte(this)
}

// Int is UInt64
type Int64 int64

func (x Int64) Compare(than Key) (result Ord) {
    switch than.(type) {
    case NInf:
        result = GT
    case Int64:
        switch {
        case x < than.(Int64):
            result = LT
        case x == than.(Int64):
            result = EQUAL
        case x > than.(Int64):
            result = GT
        }
    case PInf:
        result = LT
    }
    return
}

func (x Int64) Less(than Key) bool {
    return x.Compare(than) == LT
}

func (x Int64) Bytes() []byte {
    data := make([]byte, 8)
    binary.PutVarint(data, int64(x))
    return data
}

// String
type String string

func (x String) Compare(than Key) (result Ord) {
    switch than.(type) {
    case NInf:
        result = GT
    case String:
        switch {
        case x < than.(String):
            result = LT
        case x == than.(String):
            result = EQUAL
        case x > than.(String):
            result = GT
        }
    case PInf:
        result = LT
    }
    return
}

func (x String) Less(than Key) bool {
    return x.Compare(than) == LT
}

func (x String) Bytes() []byte {
    return []byte(x)
}
*/
