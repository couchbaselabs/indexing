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
	"log"
)

// api.Finder interface
func (ldb *LevelDBEngine) Name() string {
	return ldb.name
}

func (ldb *LevelDBEngine) Trait(operator interface{}) api.TraitInfo {
	switch operator.(type) {
	default:
		return ldb.trait
	}
}

// api.Counter interface
func (ldb *LevelDBEngine) CountTotal() (uint64, error) {

	var nilKey api.Key
	var err error
	if nilKey, err = api.NewKeyFromEncodedBytes(nil); err != nil {
		return 0, err
	}

	return ldb.CountRange(nilKey, nilKey, api.Both)
}

// api.Exister interface
func (ldb *LevelDBEngine) Exists(key api.Key) bool {

	var totalRows uint64
	var err error
	if totalRows, err = ldb.CountRange(key, key, api.Both); err != nil {
		return false
	}
	if totalRows > 0 {
		return true
	}
	return false
}

// api.Looker interface
//FIXME add limit parameter
func (ldb *LevelDBEngine) Lookup(key api.Key) (chan api.Value, chan error) {
	chval := make(chan api.Value)
	cherr := make(chan error)

	log.Printf("Received Lookup Query for Key %s", key.String())
	go ldb.GetValueSetForKeyRange(key, key, api.Both, chval, cherr)
	return chval, cherr
}

//FIXME add limit parameter
func (ldb *LevelDBEngine) KeySet() (chan api.Key, chan error) {
	chkey := make(chan api.Key)
	cherr := make(chan error)

	nilKey, _ := api.NewKeyFromEncodedBytes(nil)
	go ldb.GetKeySetForKeyRange(nilKey, nilKey, api.Both, chkey, cherr)
	return chkey, cherr
}

//FIXME add limit parameter
func (ldb *LevelDBEngine) ValueSet() (chan api.Value, chan error) {
	chval := make(chan api.Value)
	cherr := make(chan error)

	nilKey, _ := api.NewKeyFromEncodedBytes(nil)
	go ldb.GetValueSetForKeyRange(nilKey, nilKey, api.Both, chval, cherr)
	return chval, cherr
}

// api.Ranger
//FIXME add limit parameter
func (ldb *LevelDBEngine) KeyRange(low, high api.Key, inclusion api.Inclusion) (
	chan api.Key, chan error, api.SortOrder) {

	chkey := make(chan api.Key)
	cherr := make(chan error)

	go ldb.GetKeySetForKeyRange(low, high, inclusion, chkey, cherr)
	return chkey, cherr, api.Asc
}

//FIXME add limit parameter
func (ldb *LevelDBEngine) ValueRange(low, high api.Key, inclusion api.Inclusion) (
	chan api.Value, chan error, api.SortOrder) {

	chval := make(chan api.Value)
	cherr := make(chan error)

	go ldb.GetValueSetForKeyRange(low, high, inclusion, chval, cherr)
	return chval, cherr, api.Asc
}
