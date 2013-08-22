package api

import "github.com/couchbaselabs/tuqtng/ast"

// Indexer is the interface into the index engine
type Indexer interface {

	// Create builds an instance of an index
	Create(statement *ast.CreateIndexStatement) error

	// Drop kills an instance of an index
	Drop(name string) error

	// Instances lists all known index instances
	Indexes() []string

	// Gets a specific instance
	Index(name string) Accesser
}

// Accesser represents an instance of an index.
// Issuing a CREATE INDEX statement will create one new Accessor.
type Accesser interface {

	// The name of this index instance
	Name() string

	// Type index type
	Type() IndexType

	// Definition
	Defn() *ast.CreateIndexStatement
}

// Key is an array of JSON objects, per encoding/json
type Key []interface{}

// Value is the primary key of the relavent document
type Value string

// Known index types
type IndexType int

const (
	View IndexType = iota
	BTree
	RBTree
)

// Inclusion controls how the boundaries values of a range are treated
type Inclusion int

const (
	Neither Inclusion = iota
	Left
	Right
	Both
)

// Algorithm is the basic capability of any index algorithm
type Finder interface {
	Name() string
	Traits(operator interface{}) TraitInfo
}

// Counter is a class of algorithms that return total node count efficiently
type Counter interface {
	Finder
	CountTotal() (uint64, error)
}

// Exister is a class of algorithms that allow testing if a key exists in the index
type Exister interface {
	Finder
	Exists(key Key) bool
}

// Looker is a class of algorithms that allow looking up a key in an index. Usually, being able to
// look up a key means we can iterate through all keys too, and so that is introduced here as well.
type Looker interface {
	Exister
	Lookup(key Key) (chan Value, chan error)
	KeySet() (chan Key, chan error)
	ValueSet() (chan Value, chan error)
}

// Ranger is a class of algorithms that can extract a range of keys from the index.
type Ranger interface {
	Looker
	KeyRange(low Key, high Key, inclusion Inclusion) (chan Key, chan error, SortOrder)
	ValueRange(low Key, high Key, inclusion Inclusion) (chan Value, chan error, SortOrder)
}

// RangeCounter is a class of algorithms that can count a range efficiently
type RangeCounter interface {
	Finder
	CountRange(low Key, high Key, inclusion Inclusion) (uint64, error)
}

// Uniqueness characterizes if the algorithm demands unique keys
type Uniqueness bool

const (
	Unique    Uniqueness = true
	NonUnique            = false
)

// SortOrder characterizes if the algorithm emits keys in a predictable order
type SortOrder int

const (
	Unsorted SortOrder = iota
	Asc
	Desc
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

// Accuracy characterizes if the results of the index is subject to probabilistic errors.
// When an algorithm that is not Perfect is used, the caller must verify the results.
type Accuracy float64

const (
	Useless Accuracy = 0.0
	Perfect          = 1.0
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
