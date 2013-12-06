// Disk based catalog manager implementing `Indexer` interface. On disk
// catalog file has the following format, all fields encoded using
// "encoding/gob" library.
//
//   offset 0   count   int32
//   offset 4   uuid    string
//              index1  IndexInfo
//              index2  IndexInfo
//              ...
//              <upto count index>

// The disk based catalog indexer supports all the APIs defined by `Indexer`
// interface.

package server

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/indexing/llrb"
	"github.com/nu7hatch/gouuid" // TODO: Remove this dependancy ??
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

const (
	CATALOGFILE   string = "index_catalog.dat" // contains gob data
	DEFAULT_LIMIT int    = 100
)

type IndexCatalog struct {
	sync.RWMutex
	DefaultLimit int
	Uuid         string
	Datadir      string
	File         string
	Indexes      map[string]*api.IndexInfo
}

func NewIndexCatalog(datadir string) (indexer *IndexCatalog, err error) {
	indexer = &IndexCatalog{
		DefaultLimit: DEFAULT_LIMIT,
		Datadir:      datadir,
		File:         filepath.Join(datadir, CATALOGFILE),
	}
	if err = indexer.tryCreate(); err != nil {
		return nil, err
	}
	if err = indexer.loadCatalog(); err != nil {
		return nil, err
	}
	return indexer, nil
}

// Clean up the disk catalog
func (indexer *IndexCatalog) Purge() (err error) {
	indexer.Lock() // Write lock !!
	defer indexer.Unlock()

	if err = os.Remove(indexer.File); err != nil {
		return err
	}
	indexer.Datadir = ""
	indexer.Indexes = nil
	return nil
}

// set the default limit for SCAN command.
func (indexer *IndexCatalog) SetLimit(limit int) {
	indexer.Lock() // Write lock !!
	defer indexer.Unlock()

	indexer.DefaultLimit = limit
}

// Get the catalog's unique id, which gets updated when ever a new index is
// created or dropped.
func (indexer *IndexCatalog) GetUuid() string {
	indexer.RLock()
	defer indexer.RUnlock()
	return indexer.Uuid
}

func (indexer *IndexCatalog) Create(indexinfo api.IndexInfo) (
	string, api.IndexInfo, error) {

	var err error

	// Generate UUID for the index.
	if uvalue, err := uuid.NewV4(); err == nil {
		indexinfo.Uuid = fmt.Sprintf("%v", uvalue)
		// Add to indexer catalog
		indexer.Lock() // Write lock !!
		defer indexer.Unlock()
		indexer.Indexes[indexinfo.Uuid] = &indexinfo

		// Save to disk
		if err = indexer.saveCatalog(); err == nil {
			err = getIndexEngine(&indexinfo)
		}
	}
	return indexer.Uuid, indexinfo, err
}

func (indexer *IndexCatalog) Drop(uuid string) (string, error) {
	var err error

	if indexinfo, ok := indexer.Indexes[uuid]; ok {
		indexinfo.Index.Purge()

		// Remove from indexer catalog
		indexer.Lock() // Write lock !!
		defer indexer.Unlock()

		delete(indexer.Indexes, uuid)

		indexinfo.Index = nil
		err = indexer.saveCatalog()
	} else {
		err = errors.New("uuid not found in index catalog")
	}
	return indexer.Uuid, err
}

func (indexer *IndexCatalog) List(serverUuid string) (string, []api.IndexInfo, error) {
	var indexinfos []api.IndexInfo
	indexer.RLock()
	defer indexer.RUnlock()

	if indexer.Uuid != serverUuid {
		indexinfos = make([]api.IndexInfo, 0, len(indexer.Indexes))
		for _, index := range indexer.Indexes {
			indexClone := *index // Copy
			indexClone.Index = nil
			indexinfos = append(indexinfos, indexClone)
		}
	}
	return indexer.Uuid, indexinfos, nil
}

func (indexer *IndexCatalog) Index(uuid string) (api.IndexInfo, error) {
	indexer.RLock()
	defer indexer.RUnlock()

	for _, index := range indexer.Indexes {
		if index.Uuid == uuid {
			return *index, nil
		}
	}
	return api.IndexInfo{}, errors.New("Invalid uuid")
}

// save catalog information on disk, the format of the catalog file is
// described at the top.
func (indexer *IndexCatalog) saveCatalog() (err error) {
	var fd *os.File

	// Open the catalog file
	if fd, err = os.OpenFile(indexer.File, os.O_WRONLY, 0660); err != nil {
		return err
	}
	defer fd.Close()

	// gob-encoder
	buf := new(bytes.Buffer)
	genc := gob.NewEncoder(buf)

	// Write the count
	genc.Encode(int32(len(indexer.Indexes)))

	// Write the uuid
	if uvalue, err := uuid.NewV4(); err == nil {
		indexer.Uuid = fmt.Sprintf("%v", uvalue)
		genc.Encode(indexer.Uuid)
	}

	// Write IndexInfo
	for _, index := range indexer.Indexes {
		indexClone := *index // Copy
		indexClone.Index = nil
		genc.Encode(indexClone)
	}
	if _, err = fd.Write(buf.Bytes()); err != nil {
		return err
	}

	return nil
}

// load catalog information from disk, the format of the catalog file is
// described at the top.
func (indexer *IndexCatalog) loadCatalog() (err error) {
	var fd *os.File
	var bytebuf []byte

	// open the catalog file.
	if fd, err = os.OpenFile(indexer.File, os.O_RDONLY, 0660); err != nil {
		return err
	}
	defer fd.Close()

	// gob-decoder
	if bytebuf, err = ioutil.ReadAll(fd); err != nil {
		return err
	}
	gdec := gob.NewDecoder(bytes.NewBuffer(bytebuf))

	// read count of indexes saved in the catalog file.
	var count int32
	gdec.Decode(&count)

	// read catalog uuid
	gdec.Decode(&indexer.Uuid)

	// read the IndexInfo from the catalog file.
	indexes := make(map[string]*api.IndexInfo)
	for i := int32(0); i < count; i++ {
		indexinfo := api.IndexInfo{}
		gdec.Decode(&indexinfo)
		indexes[indexinfo.Uuid] = &indexinfo
		if err = getIndexEngine(&indexinfo); err != nil {
			return err
		}
	}
	indexer.Indexes = indexes
	return nil
}

// If catalog file is not present, create and close.
func (indexer *IndexCatalog) tryCreate() (err error) {
	var fd *os.File
	if _, err = os.Stat(indexer.File); err != nil {
		if fd, err = os.Create(indexer.File); err == nil {
			fd.Close()
		}
	}
	return
}

// Instantiate index engine
func getIndexEngine(index *api.IndexInfo) (err error) {
	index.Index = nil
	switch index.Using {
	case api.Llrb:
		index.Index = llrb.NewIndex(index.Name)
	default:
		err = fmt.Errorf("Invalid index-type, `%v`", index.Using)
	}
	return
}
