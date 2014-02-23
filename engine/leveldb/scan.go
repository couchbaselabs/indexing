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

var perfReadCount int64

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

	if api.DebugLog {
		log.Printf("Received Lookup Query for Key %s", key.String())
	}
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

func (ldb *LevelDBEngine) GetKeySetForKeyRange(low api.Key, high api.Key,
	inclusion api.Inclusion, chkey chan api.Key, cherr chan error) {

	defer close(chkey)
	defer close(cherr)

	snap := ldb.c.NewSnapshot()
	defer ldb.c.ReleaseSnapshot(snap)

	ro := levigo.NewReadOptions()
	ro.SetSnapshot(snap)

	it := ldb.c.NewIterator(ro)
	defer it.Close()

	if api.DebugLog {
		log.Printf("LevelDB Received Key Low - %s High - %s for Scan", low.String(), high.String())
	}

	var lowkey []byte
	var err error

	if lowkey = low.EncodedBytes(); lowkey == nil {
		it.SeekToFirst()
	} else {
		it.Seek(lowkey)
	}

	var key api.Key
	for it = it; it.Valid(); it.Next() {
		if key, err = api.NewKeyFromEncodedBytes(it.Key()); err != nil {
			log.Printf("Error Converting from bytes %v to key %v. Skipping row", it.Key(), err)
			continue
		}

		if api.DebugLog {
			log.Printf("LevelDB Got Key - %s", key.String())
		}

		var highcmp int
		if high.EncodedBytes() == nil {
			highcmp = -1 //if high key is nil, iterate through the fullset
		} else {
			highcmp = key.Compare(high)
		}

		var lowcmp int
		if low.EncodedBytes() == nil {
			lowcmp = 1 //all keys are greater than nil
		} else {
			lowcmp = key.Compare(low)
		}

		if highcmp == 0 && (inclusion == api.Both || inclusion == api.High) {
			if api.DebugLog {
				log.Printf("LevelDB Sending Key Equal to High Key")
			}
			chkey <- key
		} else if lowcmp == 0 && (inclusion == api.Both || inclusion == api.Low) {
			if api.DebugLog {
				log.Printf("LevelDB Sending Key Equal to Low Key")
			}
			chkey <- key
		} else if (highcmp == -1) && (lowcmp == 1) { //key is between high and low
			if highcmp == -1 {
				if api.DebugLog {
					log.Printf("LevelDB Sending Key Lesser Than High Key")
				}
			} else if lowcmp == 1 {
				if api.DebugLog {
					log.Printf("LevelDB Sending Key Greater Than Low Key")
				}
			}
			chkey <- key
		} else {
			if api.DebugLog {
				log.Printf("LevelDB not Sending Key")
			}
			//if we have reached past the high key, no need to scan further
			if highcmp == 1 {
				break
			}
		}
	}

	//FIXME
	/*
	   if err := it.GetError() {
	       log.Printf("Error %v", err)
	       return err
	   }
	*/

}

func (ldb *LevelDBEngine) GetValueSetForKeyRange(low api.Key, high api.Key,
	inclusion api.Inclusion, chval chan api.Value, cherr chan error) {

	defer close(chval)
	defer close(cherr)

	snap := ldb.c.NewSnapshot()
	defer ldb.c.ReleaseSnapshot(snap)

	ro := levigo.NewReadOptions()
	ro.SetSnapshot(snap)

	it := ldb.c.NewIterator(ro)
	defer it.Close()

	if api.DebugLog {
		log.Printf("LevelDB Received Key Low - %s High - %s Inclusion - %v for Scan", low.String(), high.String(), inclusion)
	}

	var lowkey []byte
	var err error

	if lowkey = low.EncodedBytes(); lowkey == nil {
		it.SeekToFirst()
	} else {
		it.Seek(lowkey)
	}

	var key api.Key
	var val api.Value
	for it = it; it.Valid(); it.Next() {
		if key, err = api.NewKeyFromEncodedBytes(it.Key()); err != nil {
			log.Printf("Error Converting from bytes %v to key %v. Skipping row", it.Key(), err)
			continue
		}

		if val, err = api.NewValueFromEncodedBytes(it.Value()); err != nil {
			log.Printf("Error Converting from bytes %v to value %v, Skipping row", it.Value(), err)
			continue
		}

		if api.DebugLog {
			log.Printf("LevelDB Got Value - %s", val.String())
		}

		var highcmp int
		if high.EncodedBytes() == nil {
			highcmp = -1 //if high key is nil, iterate through the fullset
		} else {
			highcmp = key.Compare(high)
		}

		var lowcmp int
		if low.EncodedBytes() == nil {
			lowcmp = 1 //all keys are greater than nil
		} else {
			lowcmp = key.Compare(low)
		}

		if highcmp == 0 && (inclusion == api.Both || inclusion == api.High) {
			if api.DebugLog {
				log.Printf("LevelDB Sending Value Equal to High Key")
			}
			chval <- val
		} else if lowcmp == 0 && (inclusion == api.Both || inclusion == api.Low) {
			if api.DebugLog {
				log.Printf("LevelDB Sending Value Equal to Low Key")
			}
			chval <- val
		} else if (highcmp == -1) && (lowcmp == 1) { //key is between high and low
			if highcmp == -1 {
				if api.DebugLog {
					log.Printf("LevelDB Sending Value Lesser Than High Key")
				}
			} else if lowcmp == 1 {
				if api.DebugLog {
					log.Printf("LevelDB Sending Value Greater Than Low Key")
				}
			}
			chval <- val
		} else {
			if api.DebugLog {
				log.Printf("LevelDB not Sending Value")
			}
			//if we have reached past the high key, no need to scan further
			if highcmp == 1 {
				break
			}
		}
		perfReadCount += 1
	}
	log.Printf("Index Values Read %v", perfReadCount)

	//FIXME
	/*
	   if err := it.GetError() {
	       log.Printf("Error %v", err)
	       return err
	   }
	*/

}

func (ldb *LevelDBEngine) CountRange(low api.Key, high api.Key, inclusion api.Inclusion) (
	uint64, error) {

	var count uint64

	snap := ldb.c.NewSnapshot()
	defer ldb.c.ReleaseSnapshot(snap)

	ro := levigo.NewReadOptions()
	ro.SetSnapshot(snap)

	it := ldb.c.NewIterator(ro)
	defer it.Close()

	if api.DebugLog {
		log.Printf("LevelDB Received Key Low - %s High - %s for Scan", low.String(), high.String())
	}

	var lowkey []byte
	var err error

	if lowkey = low.EncodedBytes(); lowkey == nil {
		it.SeekToFirst()
	} else {
		it.Seek(lowkey)
	}

	var key api.Key
	for it = it; it.Valid(); it.Next() {
		if key, err = api.NewKeyFromEncodedBytes(it.Key()); err != nil {
			log.Printf("Error Converting from bytes %v to key %v. Skipping row", it.Key(), err)
			continue
		}

		if api.DebugLog {
			log.Printf("LevelDB Got Key - %s", key.String())
		}

		var highcmp int
		if high.EncodedBytes() == nil {
			highcmp = -1 //if high key is nil, iterate through the fullset
		} else {
			highcmp = key.Compare(high)
		}

		var lowcmp int
		if low.EncodedBytes() == nil {
			lowcmp = 1 //all keys are greater than nil
		} else {
			lowcmp = key.Compare(low)
		}

		if highcmp == 0 && (inclusion == api.Both || inclusion == api.High) {
			if api.DebugLog {
				log.Printf("LevelDB Sending Value Equal to High Key")
			}
			count++
		} else if lowcmp == 0 && (inclusion == api.Both || inclusion == api.Low) {
			if api.DebugLog {
				log.Printf("LevelDB Sending Value Equal to Low Key")
			}
			count++
		} else if (highcmp == -1) && (lowcmp == 1) { //key is between high and low
			if highcmp == -1 {
				if api.DebugLog {
					log.Printf("LevelDB Sending Value Lesser Than High Key")
				}
			} else if lowcmp == 1 {
				if api.DebugLog {
					log.Printf("LevelDB Sending Value Greater Than Low Key")
				}
			}
			count++
		} else {
			if api.DebugLog {
				log.Printf("LevelDB not Sending Value")
			}
			//if we have reached past the high key, no need to scan further
			if highcmp == 1 {
				break
			}
		}
	}

	//FIXME
	/*
	   if err := it.GetError() {
	       log.Printf("Error %v", err)
	       return err
	   }
	*/

	return count, nil
}
