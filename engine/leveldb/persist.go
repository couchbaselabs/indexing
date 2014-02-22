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
	ldb.options.SetMaxOpenFiles(500)

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
	ldb.options.SetMaxOpenFiles(500)

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

	if api.DebugLog {
		log.Printf("LevelDB Set Key - %s Value - %s", k.String(), v.String())
	}

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

	//if secondary-key is nil, no further processing is required. If this was a KV insert, nothing needs to be done.
	//if this was a KV update, only delete old back/main index entry
	if v.KeyBytes() == nil {
		if api.DebugLog {
			log.Printf("Received NIL secondary key. Skipping Index Insert.")
		}
		return nil
	}
	//FIXME : Handle the case if old-value from backindex matches with the new-value(false mutation). Skip It.

	//set the back index entry <docid, encodedkey>
	if err = ldb.b.Put(ldb.wo, []byte(v.Docid()), k.EncodedBytes()); err != nil {
		return err
	}

	//set in main index
	if err = ldb.c.Put(ldb.wo, k.EncodedBytes(), v.EncodedBytes()); err != nil {
		return err
	}

	return err
}

func (ldb *LevelDBEngine) InsertMeta(metaid string, metavalue string) error {

	if api.DebugLog {
		log.Printf("LevelDB Set Meta Key - %s, Value - %s", metaid, metavalue)
	}

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
		if api.DebugLog {
			log.Printf("LevelDB Get Meta Key - %s, Value - %s", metaid, string(metavalue))
		}
		return string(metavalue), nil
	}

	return "", err
}

func (ldb *LevelDBEngine) GetBackIndexEntry(docid string) (api.Key, error) {

	var k api.Key
	var kbyte []byte
	var err error

	if api.DebugLog {
		log.Printf("LevelDB Get BackIndex Key - %s", docid)
	}

	if kbyte, err = ldb.b.Get(ldb.ro, []byte(docid)); err != nil {
		return k, err
	}

	k, err = api.NewKeyFromEncodedBytes(kbyte)

	return k, err
}

func (ldb *LevelDBEngine) DeleteMutation(docid string) error {

	if api.DebugLog {
		log.Printf("LevelDB Delete Key - %s", docid)
	}
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
