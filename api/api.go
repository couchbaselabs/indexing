//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not
//  use this file except in compliance with the License. You may obtain a copy
//  of the License at,
//          http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//  License for the specific language governing permissions and limitations
//  under the License.

package api

// MaxVbuckets fixes the maximum number of vbuckets in the system.
const MAX_VBUCKETS = 1024

// IndexType tells the index backend algorithm to be used for secondary-index.
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

// ExprType tells the type of expression to be applied on the document to
// get the secondary key.
type ExprType string

const (
	Simple     ExprType = "simple"
	JavaScript          = "javascript"
	N1QL                = "n1ql"
)

// IndexInfo is per index data structure that provides system-level meta data
// information for all index managed by the system.
type IndexInfo struct {
	Name       string    `json:"name,omitempty"`       // Name of the index
	Uuid       string    `json:"uuid,omitempty"`       // unique id for every index
	Using      IndexType `json:"using,omitempty"`      // indexing algorithm
	OnExprList []string  `json:"onExprList,omitempty"` // expression list
	Bucket     string    `json:"bucket,omitempty"`     // bucket name
	IsPrimary  bool      `json:"isPrimary,omitempty"`
	Exprtype   ExprType  `json:"exprType,omitempty"`
	//  Engine     Finder    `json:"engine,omitempty"` // instance of index algorithm.
}

type keydata struct {
	keybytes Keybytes
	docid    string
}

// Key is an array of JSON objects, per encoding/json
type Key struct {
	raw     keydata
	encoded []byte // collatejson byte representation of the key
}

type valuedata struct {
	Keybytes Keybytes
	Docid    string
	Vbucket  uint16
	Seqno    uint64
}

// Value is the primary key of the relavent document
type Value struct {
	raw     valuedata
	encoded []byte
}

type Keybytes [][]byte

//list of index UUIDs
type IndexList []string

//sequence number for each of 1024 vbuckets
type SequenceVector []uint64

//map of <Index, SequenceVector>
type IndexSequenceMap map[string]SequenceVector // indexed with index-uuid
