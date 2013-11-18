// Disk based catalog manager implementing `Indexer` interface
package server

import (
    "os"
    "fmt"
    "errors"
    "bytes"
    "io/ioutil"
    "encoding/gob"
    "path/filepath"
    "sync"
    "github.com/couchbaselabs/indexing/api"
    "github.com/nu7hatch/gouuid"  // TODO: Remove this dependancy ??
)

const (
    CATALOGFILE   string = "index_catalog.dat" // contains gob data
    DEFAULT_LIMIT int    = 100
)

type IndexCatalog struct {
    sync.RWMutex
    DefaultLimit int
    Datadir      string
    Indexes      map[string]*api.IndexInfo
}

func NewIndexCatalog(datadir string) (api.Indexer, error) {
    var fd      *os.File
    var bytebuf []byte
    var err     error

    file := filepath.Join(datadir, CATALOGFILE)
    // If catalog file is not present, create and close.
    if _, err = os.Stat(file); err != nil {
        if fd, err = os.Create(file); err != nil {
            return nil, err
        }
        fd.Close()
    }

    // And, open the catalog file.
    if fd, err = os.OpenFile(file, os.O_RDONLY, 0660); err != nil {
        return nil, err
    }
    defer fd.Close()

    // Read IndexInfo list, encoded in GoB format, from CATALOGFILE
    var count int32
    if bytebuf, err = ioutil.ReadAll(fd); err != nil {
        return nil, err
    }
    gdec := gob.NewDecoder(bytes.NewBuffer(bytebuf))
    gdec.Decode(&count)
    i, indexes := int32(0), make(map[string]*api.IndexInfo)
    for i < count {
        info := api.IndexInfo{}
        gdec.Decode(&info)
        indexes[info.Uuid] = &info
    }
    indexer := &IndexCatalog{
        DefaultLimit: DEFAULT_LIMIT, Datadir: datadir, Indexes: indexes,
    }
    return indexer, nil
}

// For SCAN query, set the default limit.
func (indexer *IndexCatalog) SetLimit(limit int) {
    indexer.DefaultLimit = limit
}

func (indexer *IndexCatalog) Create(index *api.IndexInfo) error {
    var err error

    uuid, err := uuid.NewV4()
    if err != nil {
        return err
    }
    index.Uuid = fmt.Sprintf("%v", uuid)

    if err = indexer.persist(); err != nil {
        return err
    }

    // Add to indexer catalog
    indexer.Lock()
    indexer.Indexes[index.Uuid] = index
    indexer.Unlock()

    index.Index = nil
    // TODO: This has to be one of the create calls to index algorithm,
    // based on `Using`
    // TODO: Push the expression for the new index to projector process.
    // Also update the router process on the same. Return only after that.

    return nil
}

func (indexer *IndexCatalog) Drop(uuid string) error {
    var err error

    if _, ok := indexer.Indexes[uuid]; ok {
        // Remove from indexer catalog
        indexer.Lock()
        delete(indexer.Indexes, uuid)
        indexer.Unlock()

        // TODO: Drop the index from router and projector and then drop it
        // from the indexer node.
        // TODO: Invoke the index engine api to delete the index.
        if err = indexer.persist(); err != nil {
            return err
        }
        return nil
    } else {
        return errors.New("uuid not found in index catalog")
    }
}

func (indexer *IndexCatalog) List() ([]api.IndexInfo, error) {
    indexer.RLock()
    defer indexer.RUnlock()

    indexinfo := make([]api.IndexInfo, 0, len(indexer.Indexes))
    for _, index := range indexer.Indexes {
        indexClone := *index // Copy
        indexClone.Index = nil
        indexinfo = append(indexinfo, indexClone)
    }
    return indexinfo, nil
}

func (indexer *IndexCatalog) Index(uuid string) *api.IndexInfo {
    indexer.RLock()
    defer indexer.RUnlock()

    for _, index := range indexer.Indexes {
        if index.Uuid == uuid {
            return index
        }
    }
    return nil
}

func (indexer *IndexCatalog) persist() error {
    var fd *os.File
    var err error

    indexer.RLock()
    defer indexer.RUnlock()

    file := filepath.Join(indexer.Datadir, CATALOGFILE)
    // Persist the index catalog.
    if fd, err = os.OpenFile(file, os.O_WRONLY, 0660); err != nil {
        return err
    }
    defer fd.Close()

    count := int32(len(indexer.Indexes))
    buf := new(bytes.Buffer)
    genc := gob.NewEncoder(buf)
    genc.Encode(count)
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
