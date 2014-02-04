//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package leveldb

import (
	"github.com/couchbaselabs/indexing/api"
	"github.com/jmhodges/levigo"
	"log"
)

//FIXME try to use single leveldb object, rather than all the elements here
type LevelDBEngine struct {
	name    string
	options *levigo.Options
	ro      *levigo.ReadOptions
	wo      *levigo.WriteOptions
	c       *levigo.DB
	b       *levigo.DB
	trait   api.TraitInfo
}

func NewIndexEngine(name string) (engine api.Finder) {
	var err error
	if engine, err = Create(name); err != nil {
		log.Printf("Error Creating LevelDB Engine %v", err)
	}
	return engine
}

func OpenIndexEngine(name string) (engine api.Finder) {

	var err error
	if engine, err = Open(name); err != nil {
		log.Printf("Error Creating LevelDB Engine %v", err)
	}
	return engine
}
