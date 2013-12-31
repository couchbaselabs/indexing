// Disk based catalog manager implementing `IndexManager` interface. On disk
// catalog file has the following format, all fields encoded using
// "encoding/gob" library.
//
//   offset 0   count   int32
//   offset 4   uuid    string
//              index1  IndexInfo
//              index2  IndexInfo
//              ...
//              <upto count index>

// The disk based catalog index manager supports all the APIs defined by `IndexManager`
// interface.

package catalog

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/couchbaselabs/indexing/api"
	"github.com/nu7hatch/gouuid" // TODO: Remove this dependancy ??
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

// IndexCatalog is the interface for disk-based catalog for Index Manager

type IndexCatalog interface {
	// Create builds an instance of index
	Create(indexInfo api.IndexInfo) (string, error)

	// Drop kills an instance of an index
	Drop(uuid string) (string, error)

	// If `ServerUuid` is not nil, then check to see if the local    ServerUUID
	// matches it. A match means client already has latest server
	// information and index data is not sent. A zero value makes    server send
	// the latest index data unconditionally.
	//
	// Returned list IndexInfo won't contain the index instance.
	List(ServerUuid string) (string, []api.IndexInfo, error)

	// Gets a specific instance
	Index(uuid string) (api.IndexInfo, error)

	// Get Uuid
	GetUuid() string

	//Check if index already exists for a given bucket
	Exists(name string, bucket string) error

	//Purge the catalog
	Purge() error
}

const (
	CATALOGFILE string = "index_catalog.dat" // contains gob data
)

type catalog struct {
	sync.RWMutex
	uuid    string
	datadir string
	file    string
	indexes map[string]*api.IndexInfo
}

func NewIndexCatalog(datadir string) (c *catalog, err error) {
	c = &catalog{
		datadir: datadir,
		file:    filepath.Join(datadir, CATALOGFILE),
	}
	if err = c.tryCreate(); err != nil {
		return nil, err
	}
	if err = c.loadCatalog(); err != nil {
		return nil, err
	}
	return c, nil
}

// Clean up the disk catalog
func (c *catalog) Purge() (err error) {
	c.Lock() // Write lock !!
	defer c.Unlock()

	if err = os.Remove(c.file); err != nil {
		return err
	}
	c.datadir = ""
	c.indexes = nil
	return nil
}

// Get the catalog's unique id, which gets updated when ever a new index is
// created or dropped.
func (c *catalog) GetUuid() string {
	c.RLock()
	defer c.RUnlock()
	return c.uuid
}

func (c *catalog) Create(indexinfo api.IndexInfo) (string, error) {

	var err error

	//Write Lock
	c.Lock()
	defer c.Unlock()

	c.indexes[indexinfo.Uuid] = &indexinfo

	// Save to disk
	if err = c.saveCatalog(); err != nil {
		err = errors.New("Error saving index catalog to disk")
	}

	return c.uuid, err

}

func (c *catalog) Drop(uuid string) (string, error) {
	var err error

	//Write Lock
	c.Lock()
	defer c.Unlock()

	if _, ok := c.indexes[uuid]; ok {

		delete(c.indexes, uuid)
		err = c.saveCatalog()
	} else {
		err = errors.New("uuid not found in index catalog")
	}
	return c.uuid, err
}

func (c *catalog) List(serverUuid string) (string, []api.IndexInfo, error) {
	var indexinfos []api.IndexInfo
	c.RLock()
	defer c.RUnlock()

	if c.uuid != serverUuid {
		indexinfos = make([]api.IndexInfo, 0, len(c.indexes))
		for _, index := range c.indexes {
			indexClone := *index // Copy
			indexinfos = append(indexinfos, indexClone)
		}
	}
	return c.uuid, indexinfos, nil
}

func (c *catalog) Index(uuid string) (api.IndexInfo, error) {
	c.RLock()
	defer c.RUnlock()

	for _, index := range c.indexes {
		if index.Uuid == uuid {
			return *index, nil
		}
	}
	return api.IndexInfo{}, errors.New("Invalid uuid")
}

func (c *catalog) Exists(name string, bucket string) error {

	for _, indexinfo := range c.indexes {

		if name == indexinfo.Name && bucket == indexinfo.Bucket {
			return errors.New(fmt.Sprintf("Index %s already exists with UUID %s", name, indexinfo.Uuid))
		}
	}

	return nil

}

// save catalog information on disk, the format of the catalog file is
// described at the top.
func (c *catalog) saveCatalog() (err error) {
	var fd *os.File

	// Open the catalog file
	if fd, err = os.OpenFile(c.file, os.O_WRONLY, 0660); err != nil {
		return err
	}
	defer fd.Close()

	// gob-encoder
	buf := new(bytes.Buffer)
	genc := gob.NewEncoder(buf)

	// Write the count
	genc.Encode(int32(len(c.indexes)))

	// Write the uuid
	if uvalue, err := uuid.NewV4(); err == nil {
		c.uuid = fmt.Sprintf("%v", uvalue)
		genc.Encode(c.uuid)
	}

	// Write IndexInfo
	for _, index := range c.indexes {
		indexClone := *index // Copy
		genc.Encode(indexClone)
	}
	if _, err = fd.Write(buf.Bytes()); err != nil {
		return err
	}

	return nil
}

// load catalog information from disk, the format of the catalog file is
// described at the top.
func (c *catalog) loadCatalog() (err error) {
	var fd *os.File
	var bytebuf []byte

	// open the catalog file.
	if fd, err = os.OpenFile(c.file, os.O_RDONLY, 0660); err != nil {
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
	gdec.Decode(&c.uuid)

	// read the IndexInfo from the catalog file.
	indexes := make(map[string]*api.IndexInfo)
	for i := int32(0); i < count; i++ {
		indexinfo := api.IndexInfo{}
		gdec.Decode(&indexinfo)
		indexes[indexinfo.Uuid] = &indexinfo
	}
	c.indexes = indexes
	return nil
}

// If catalog file is not present, create and close.
func (c *catalog) tryCreate() (err error) {
	var fd *os.File
	if _, err = os.Stat(c.file); err != nil {
		if fd, err = os.Create(c.file); err == nil {
			fd.Close()
		}
	}
	return
}
