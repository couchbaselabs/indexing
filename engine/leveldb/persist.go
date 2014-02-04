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

func Create(name string) (*LevelDBEngine, error) {

	var ldb LevelDBEngine
	//FIXME move this to a dir
	ldb.name = name

	ldb.options = levigo.NewOptions()
	ldb.options.SetCreateIfMissing(true)
	ldb.options.SetErrorIfExists(true)

	//set filter policy
	filterPolicy := levigo.NewBloomFilter(10)
	ldb.options.SetFilterPolicy(filterPolicy)

	ldb.options.SetCompression(levigo.SnappyCompression)

	ldb.wo = levigo.NewWriteOptions()
	ldb.ro = levigo.NewReadOptions()

	var err error
	if ldb.c, err = levigo.Open(ldb.name, ldb.options); err != nil {
		return nil, err
	}

	//create a separate back-index
	if ldb.b, err = levigo.Open(ldb.name+"_back", ldb.options); err != nil {
		return nil, err
	}

	return &ldb, nil
}

func Open(name string) (*LevelDBEngine, error) {

	var ldb LevelDBEngine
	//FIXME move this to a dir
	ldb.name = name

	ldb.options = levigo.NewOptions()
	ldb.options.SetCreateIfMissing(false)

	//set filter policy
	filterPolicy := levigo.NewBloomFilter(10)
	ldb.options.SetFilterPolicy(filterPolicy)

	ldb.options.SetCompression(levigo.SnappyCompression)

	ldb.wo = levigo.NewWriteOptions()
	ldb.ro = levigo.NewReadOptions()

	var err error
	if ldb.c, err = levigo.Open(ldb.name, ldb.options); err != nil {
		return nil, err
	}

	//create a separate back-index
	if ldb.b, err = levigo.Open(ldb.name+"_back", ldb.options); err != nil {
		return nil, err
	}

	return &ldb, nil

}

func (ldb *LevelDBEngine) InsertMutation(k api.Key, v api.Value) error {

	var err error
	var backkey api.Key

	log.Printf("LevelDB Set Key - %s Value - %s", k.String(), v.String())

	//check if the docid exists in the back index
	if backkey, err = ldb.GetBackIndexEntry(v.Docid()); err != nil {
		log.Printf("Error locating backindex entry %v", err)
		return err
	} else if backkey.EncodedBytes() != nil {
		//there is already an entry in main index for this docid
		//delete from main index
		if err = ldb.c.Delete(ldb.wo, backkey.EncodedBytes()); err != nil {
			log.Printf("Error deleting entry from main index %v", err)
			return err
		}
	}
	//FIXME : Handle the case if old-value from backindex matches with the new-value(false mutation). Skip It.

	//set in main index
	if err = ldb.c.Put(ldb.wo, k.EncodedBytes(), v.EncodedBytes()); err != nil {
		return err
	}

	//set the back index entry <docid, encodedkey>
	if err = ldb.b.Put(ldb.wo, []byte(v.Docid()), k.EncodedBytes()); err != nil {
		return err
	}

	return err
}

func (ldb *LevelDBEngine) InsertMeta(metaid string, metavalue string) error {

	log.Printf("LevelDB Set Meta Key - %s, Value - %s", metaid, metavalue)
	var err error

	//meta values go to the back index
	if err = ldb.b.Put(ldb.wo, []byte(metaid), []byte(metavalue)); err != nil {
		return err
	}

	return err
}

func (ldb *LevelDBEngine) GetMeta(metaid string) (string, error) {

	var metavalue []byte
	var err error
	if metavalue, err = ldb.b.Get(ldb.ro, []byte(metaid)); err == nil {
		log.Printf("LevelDB Get Meta Key - %s, Value - %s", metaid, string(metavalue))
		return string(metavalue), nil
	}

	return "", err
}

func (ldb *LevelDBEngine) GetBackIndexEntry(docid string) (api.Key, error) {

	var k api.Key
	var kbyte []byte
	var err error

	log.Printf("LevelDB Get BackIndex Key - %s", docid)

	if kbyte, err = ldb.b.Get(ldb.ro, []byte(docid)); err != nil {
		return k, err
	}

	k, err = api.NewKeyFromEncodedBytes(kbyte)

	return k, err
}

func (ldb *LevelDBEngine) DeleteMutation(docid string) error {

	log.Printf("LevelDB Delete Key - %s", docid)
	var backkey api.Key
	var err error

	if backkey, err = ldb.GetBackIndexEntry(docid); err != nil {
		log.Printf("Error locating backindex entry %v", err)
		return err
	}

	//delete from main index
	if err = ldb.c.Delete(ldb.wo, backkey.EncodedBytes()); err != nil {
		log.Printf("Error deleting entry from main index %v", err)
		return err
	}

	//delete from the back index
	if err = ldb.b.Delete(ldb.wo, []byte(docid)); err != nil {
		log.Printf("Error deleting entry from back index %v", err)
		return err
	}

	return nil

}

func (ldb *LevelDBEngine) Close() error {
	//close the main index
	if ldb.c != nil {
		ldb.c.Close()
	}
	//close the back index
	if ldb.b != nil {
		ldb.b.Close()
	}
	return nil
}

func (ldb *LevelDBEngine) Destroy() error {
	var err error
	if err = ldb.Close(); err != nil {
		return err
	}
	//Destroy the main index
	if err = levigo.DestroyDatabase(ldb.name, ldb.options); err != nil {
		return err
	}
	//Destroy the back index
	if err = levigo.DestroyDatabase(ldb.name+"_back", ldb.options); err != nil {
		return err
	}
	return err
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

	log.Printf("LevelDB Received Key Low - %s High - %s for Scan", low.String(), high.String())

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

		log.Printf("LevelDB Got Key - %s", key.String())

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
			log.Printf("LevelDB Sending Key Equal to High Key")
			chkey <- key
		} else if lowcmp == 0 && (inclusion == api.Both || inclusion == api.Low) {
			log.Printf("LevelDB Sending Key Equal to Low Key")
			chkey <- key
		} else if (highcmp == -1) && (lowcmp == 1) { //key is between high and low
			if highcmp == -1 {
				log.Printf("LevelDB Sending Key Lesser Than High Key")
			} else if lowcmp == 1 {
				log.Printf("LevelDB Sending Key Greater Than Low Key")
			}
			chkey <- key
		} else {
			log.Printf("LevelDB not Sending Key")
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

	log.Printf("LevelDB Received Key Low - %s High - %s Inclusion - %v for Scan", low.String(), high.String(), inclusion)

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

		log.Printf("LevelDB Got Value - %s", val.String())

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
			log.Printf("LevelDB Sending Value Equal to High Key")
			chval <- val
		} else if lowcmp == 0 && (inclusion == api.Both || inclusion == api.Low) {
			log.Printf("LevelDB Sending Value Equal to Low Key")
			chval <- val
		} else if (highcmp == -1) && (lowcmp == 1) { //key is between high and low
			if highcmp == -1 {
				log.Printf("LevelDB Sending Value Lesser Than High Key")
			} else if lowcmp == 1 {
				log.Printf("LevelDB Sending Value Greater Than Low Key")
			}
			chval <- val
		} else {
			log.Printf("LevelDB not Sending Value")
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

func (ldb *LevelDBEngine) CountRange(low api.Key, high api.Key, inclusion api.Inclusion) (
	uint64, error) {

	var count uint64

	snap := ldb.c.NewSnapshot()
	defer ldb.c.ReleaseSnapshot(snap)

	ro := levigo.NewReadOptions()
	ro.SetSnapshot(snap)

	it := ldb.c.NewIterator(ro)
	defer it.Close()

	log.Printf("LevelDB Received Key Low - %s High - %s for Scan", low.String(), high.String())

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

		log.Printf("LevelDB Got Key - %s", key.String())

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
			log.Printf("LevelDB Sending Value Equal to High Key")
			count++
		} else if lowcmp == 0 && (inclusion == api.Both || inclusion == api.Low) {
			log.Printf("LevelDB Sending Value Equal to Low Key")
			count++
		} else if (highcmp == -1) && (lowcmp == 1) { //key is between high and low
			if highcmp == -1 {
				log.Printf("LevelDB Sending Value Lesser Than High Key")
			} else if lowcmp == 1 {
				log.Printf("LevelDB Sending Value Greater Than Low Key")
			}
			count++
		} else {
			log.Printf("LevelDB not Sending Value")
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
