package indexing

// primitives

type Key []interface{}
type Value interface{}

type Interval int

const (
	open      Interval = iota
	closed             = iota
	leftopen           = iota
	rightopen          = iota
)

// capabilities

type Indexer interface {
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
	Range(low Key, high Key, interval Interval)
}

// promises

type Constraint bool

const (
	Unique    Constraint = true
	NonUnique            = false
)

type SortOrder int

const (
	None SortOrder = iota
	Asc            = iota
	Desc           = iota
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
)

type Accuracy float64

const (
	Useless Accuracy = 0.0
	Perfect          = 1.0
)

type TraitInfo struct {
	constraint Constraint
	order      SortOrder
	accuracy   Accuracy
	avgTime    Complexity
	avgSpace   Complexity
	worstTime  Complexity
	worstSpace Complexity
}
