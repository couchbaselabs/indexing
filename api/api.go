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

// Inclusion controls how the boundaries values of a range are treated
type Inclusion int

const (
    Neither Inclusion = iota
    Low
    High
    Both
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
    Name       string    `json:"name,omitempty"`       // Name of the index
    Uuid       string    `json:"uuid,omitempty"`       // unique id for every index
    Using      IndexType `json:"using,omitempty"`      // indexing algorithm
    OnExprList []string  `json:"onExprList,omitempty"` // expression list
    Bucket     string    `json:"bucket,omitempty"`     // bucket name
    IsPrimary  bool      `json:"isPrimary,omitempty"`
    Exprtype   ExprType  `json:"exprType,omitempty"`
    Engine     Finder    `json:"engine,omitempty"` // instance of index algorithm.
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

// Key is an array of JSON objects, per encoding/json
type Key struct {
    raw     keydata
    encoded []byte //collatejson byte representation of the key
}

type keydata struct {
    keybytes Keybytes
    docid    string
}

// Value is the primary key of the relavent document
type Value struct {
    raw     valuedata
    encoded []byte
}

type valuedata struct {
    Keybytes Keybytes
    Docid    string
    Vbucket  int
    Seqno    int64
}

type Keybytes [][]byte

type Persister interface {

    //Persist a key/value pair
    InsertMutation(key Key, value Value) error

    //Delete a key/value pair by docId
    DeleteMutation(docid string) error

    //Get an existing key/value pair by key
    GetBackIndexEntry(docid string) (Key, error)

    //Close the db. Should be able to reopen after this operation
    Close() error

    //Destroy/Wipe the DB completely
    Destroy() error
}

// Algorithm is the basic capability of any index algorithm
type Finder interface {
    Name() string
    //  Purge()
    Trait(operator interface{}) TraitInfo
    Persister
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
}

// Ranger is a class of algorithms that can extract a range of keys from the
// index.
type Ranger interface {
    Looker
    KeyRange(low, high Key, inclusion Inclusion) (chan Key, chan error, SortOrder)
    ValueRange(low, high Key, inclusion Inclusion) (chan Value, chan error, SortOrder)
}

// RangeCounter is a class of algorithms that can count a range efficiently
type RangeCounter interface {
    Finder
    CountRange(low Key, high Key, inclusion Inclusion) (uint64, error)
}

// Mutations from projector to indexer.
type Mutation struct {
    Type         string
    Indexid      string
    SecondaryKey [][]byte
    Docid        string
    Vbucket      int
    Seqno        int64
    Value        []byte
}
