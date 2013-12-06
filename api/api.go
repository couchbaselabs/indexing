// Defines indexing API. APIs defined here are applicable to all packages
// under `indexing`.

package api

// TODO: Define the semantics of buffer size of channels that are returned by
// the following method receiver.

// Known index types
type IndexType string

const (
    View    IndexType = "view"
    Llrb              = "llrb"
    LevelDB           = "leveldb"
    RocksDB           = "rocksdb"
    HyperDB           = "hyperdb"
    RBTree            = "rbtree"
    CBTree            = "cbtree"
)

// Inclusion, controls how the boundaries values of a range are treated
type Inclusion string

const (
    Neither Inclusion = "none"
    Left              = "left"
    Right             = "right"
    Both              = "both"
)

// Uniqueness, characterizes if the algorithm demands unique keys
type Uniqueness bool

const (
    Unique    Uniqueness = true
    NonUnique            = false
)

// SortOrder characterizes if the algorithm emits keys in a predictable order
type SortOrder string

const (
    Unsorted SortOrder = "none"
    Asc                = "asc"
    Desc               = "desc"
)

// Expression to be applied on the document to get the secondary key.
type ExprType string

const (
    Simple     string = "simple"
    JavaScript        = "javascript"
    N1QL              = "n1ql"
)

// Every index ever created and maintained by this package will have an
// associated index-info structure.
type IndexInfo struct {
    Name       string    // Name of the index
    Uuid       string    // unique id for every new index created.
    Using      IndexType // indexing algorithm to use / used.
    CreateStmt string    // in case the index was created by N1QL DDL
    Bucket     string    // bucket name, for which the index is created.
    IsPrimary  bool      // true/false based on index
    Exprtype   ExprType  // type of `Expression`
    Expression string    // expression content, check out ExprType
    Index      Finder    // instance of index algorithm.
}

// Accuracy characterizes if the results of the index is subject to probabilistic errors.
// When an algorithm that is not Perfect is used, the caller must verify the results.
type Accuracy float64

const (
    Useless Accuracy = 0.0
    Perfect          = 1.0
)

// Complexity characterizes space and time characteristics of the algorithm
type Complexity int

const (
    O1 Complexity = iota
    Ologm
    Ologn
    Om
    Omlogm
    Omlogn
    On
    Onlogn
    Om2
    On2
    Ounknown
)

// TraitInfo is collection of traits of an algorithm. One can query the traits of an
// entire indexing algorithm, or traits of a specific operation. May change soon.
type TraitInfo struct {
    Unique     Uniqueness
    Order      SortOrder
    Accuracy   Accuracy
    AvgTime    Complexity
    AvgSpace   Complexity
    WorstTime  Complexity
    WorstSpace Complexity
}

// Indexer is the interface into the index engine
type IndexManager interface {
    // Create builds an instance of index
    Create(indexInfo IndexInfo) (string, IndexInfo, error)

    // Drop kills an instance of an index
    Drop(uuid string) (string, error)

    // If `ServerUuid` is not nil, then check to see if the local ServerUUID
    // matches it. A match means client already has latest server
    // information and index data is not sent. A zero value makes server send
    // the latest index data unconditionally.
    //
    // Returned list IndexInfo won't contain the index instance.
    List(ServerUuid string) (string, []IndexInfo, error)

    // Gets a specific instance
    Index(uuid string) (IndexInfo, error)

    // Get Uuid
    GetUuid() string
}

type Key interface {
    Bytes() []byte        // content of key as byte representation
    Less(than Key) bool   // compare whether `this` key is less than `than` key
    Compare(than Key) Ord // compare whether `this` key is less than `than` key
}

type Value interface{
    Bytes() []byte // content of value, typically document-id
}

type KV [2]interface{} // [Key, Value]

// Algorithm is the basic capability of any index algorithm
type Finder interface {
    Name() string
    Purge()
    Trait(operator interface{}) TraitInfo
}

// Counter is a class of algorithms that return total node count efficiently
type Counter interface {
    Finder
    CountTotal() (uint64, error)
}

// Exister is a class of algorithms that allow testing if a key exists in the
// index
type Exister interface {
    Finder
    Exists(key Key) bool // TODO: Should we have the `error` part of return ?
}

// Looker is a class of algorithms that allow looking up a key in an index.
// Usually, being able to look up a key means we can iterate through all keys
// too, and so that is introduced here as well.
//
// TODO: Define the semantics of buffer size of channels that are returned by
// the following method receiver.
type Looker interface {
    Exister
    Lookup(key Key) (chan Value, chan error)
    KeySet() (chan Key, chan error)
    ValueSet() (chan Value, chan error)
    KVSet() (chan KV, chan error)
}

// Ranger is a class of algorithms that can extract a range of keys from the
// index.
type Ranger interface {
    Looker
    KeyRange(low, high Key, inclusion Inclusion) (chan Key, chan error, SortOrder)
    ValueRange(low, high Key, inclusion Inclusion) (chan Value, chan error, SortOrder)
    KVRange(low Key, high Key, inclusion Inclusion) (chan KV, chan error, SortOrder)
}

// RangeCounter is a class of algorithms that can count a range efficiently
type RangeCounter interface {
    Finder
    CountRange(low Key, high Key, inclusion Inclusion) (uint64, error)
}
