package indexing

// primitives

type Key []interface{}
type Value interface{}

type Inclusion int

const (
	Neither Inclusion = iota
	Left              = iota
	Right             = iota
	Both              = iota
)

// capabilities

type Indexer interface {
	Name() string
	Traits(operator interface{}) TraitInfo
}

type Exister interface {
	Exists(key Key) bool
}

type Looker interface {
	Exister
	Lookup(key Key) chan Value
	Keyset() chan Key
	Valueset() chan Value
}

type Ranger interface {
	Looker
	Keyrange(low Key, high Key, inclusion Inclusion) (chan Key, SortOrder)
	Valuerange(low Key, high Key, inclusion Inclusion) (chan Value, SortOrder)
}

// promises

type Constraint bool

const (
	Unique    Constraint = true
	NonUnique            = false
)

type SortOrder int

const (
	Unsorted SortOrder = iota
	Asc                = iota
	Desc               = iota
)

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

type Accuracy float64

const (
	Useless Accuracy = 0.0
	Perfect          = 1.0
)

type TraitInfo struct {
	Constraint Constraint
	Order      SortOrder
	Accuracy   Accuracy
	AvgTime    Complexity
	AvgSpace   Complexity
	WorstTime  Complexity
	WorstSpace Complexity
}
