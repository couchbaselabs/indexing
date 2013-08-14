package api

import "github.com/couchbaselabs/tuqtng/ast"

// Engineer is the interface into the index engine
type Engineer interface {

	// Types returns all known index types
	Types() []Algorithm

	// Create builds an instance of an index
	Create(statement ast.Statement) (Instance, error)

	// Drop kills an instance of an index
	Drop(statement ast.Statement) error
	
	// Instances lists all known index instances
	Instances() []Instance
}

// Key is an array of JSON objects, per encoding/json
type Key []interface{}

// Value is the primary key of the relavent document
type Value string

// Inclusion controls how the boundaries values of a range are treated
type Inclusion int
const (
	Neither Inclusion = iota
	Left              = iota
	Right             = iota
	Both              = iota
)

// Algorithm is the basic capability of any index algorithm
type Algorithm interface {
	Name() string
	Traits(operator interface{}) TraitInfo
}

// Exister is a class of algorithms that allow testing if a key exists in the index
type Exister interface {
	Algorithm
	Exists(key Key) bool
}

// Looker is a class of algorithms that allow looking up a key in an index. Usually, being able to
// look up a key means we can iterate through all keys too, and so that is introduced here as well.
type Looker interface {
	Exister
	Lookup(key Key) (chan Value, chan error)
	Keyset() (chan Key, chan error)
	Valueset() (chan Value, chan error)
}

// Ranger is a class of algorithms that can extract a range of keys from the index.
type Ranger interface {
	Looker
	Keyrange(low Key, high Key, inclusion Inclusion) (chan Key, chan error, SortOrder)
	Valuerange(low Key, high Key, inclusion Inclusion) (chan Value, chan error, SortOrder)
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
	Asc                = iota
	Desc               = iota
)

// Complexity characterizes space and time characteristics of the algorithm
type Complexity int
const (
	O1     Complexity = iota
	Ologm             = iota
	Ologn             = iota
	Om                = iota
	Omlogm            = iota
	Omlogn            = iota
	On                = iota
	Onlogn            = iota
	Om2               = iota
	On2               = iota
	Ounknown          = iota
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

// Finder represents an instance of an index. Each CREATE INDEX statement
// creates one finder instance logically.
type Instance struct {
	Name       string
	Definition ast.Statement
	Type       Algorithm
}
