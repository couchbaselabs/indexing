// A rest client to be used with server.
package server

import (
    "github.com/couchbaselabs/indexing/api"
    "sync"
    "errors"
    "bytes"
    "io/ioutil"
    "encoding/json"
    "net/http"
)

// A notion of catalog on the client side. For most operations we access the
// server - transparently.
type IndexerClient struct {
    addr    string
    indexes map[string]*api.IndexInfo
    sync.RWMutex
}

// Create a notion of catalog on the client side.
func NewRestClient(addr string) api.Indexer {
    return &IndexerClient{addr: addr, indexes: make(map[string]*api.IndexInfo)}
}

// Create a new index, IndexerClient.indexes will be updated with created
// index's `uuid`.
func (client *IndexerClient) Create(index *api.IndexInfo) error {
    var err  error
    var body []byte

    // Construct request body.
    indexreq := api.IndexRequest{Type: api.CREATE, Indexinfo: *index}
    if body, err = json.Marshal(indexreq); err != nil {
        return err
    }

    // Post HTTP request.
    bodybuf := bytes.NewBuffer(body)
    resp, err := http.Post(client.addr+"/create", "encoding/json", bodybuf)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    // Gather indexinfo
    indexres := api.IndexMetaResponse{}
    body, err = ioutil.ReadAll(resp.Body)
    if err != nil {
        return err
    }
    if err = json.Unmarshal(body, &indexres); err != nil {
        return err
    }
    if indexres.Status == api.ERROR {
        return errors.New(indexres.Errors[0].Msg)
    }
    client.Lock()
    defer client.Unlock()
    index = &indexres.Indexes[0]
    client.indexes[index.Uuid] = index
    return nil
}

// Drop index. IndexInfo from IndexerClient.indexes will be removed as well.
func (client *IndexerClient) Drop(uuid string) error {
    var err  error
    var body []byte

    // Construct request body.
    index := api.IndexInfo{Uuid: uuid}
    indexreq := api.IndexRequest{Type: api.DROP, Indexinfo: index}
    if body, err = json.Marshal(indexreq); err != nil {
        return err
    }

    // Post HTTP request.
    bodybuf := bytes.NewBuffer(body)
    resp, err := http.Post(client.addr+"/drop", "encoding/json", bodybuf)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    // Gather indexinfo.
    indexres := api.IndexMetaResponse{}
    body, err = ioutil.ReadAll(resp.Body)
    if err != nil {
        return err
    }
    if err = json.Unmarshal(body, &indexres); err != nil {
        return err
    }
    if indexres.Status == api.ERROR {
        return errors.New(indexres.Errors[0].Msg)
    }

    // Delete the indexinfo from the client copy.
    client.Lock()
    defer client.Unlock()
    delete(client.indexes, index.Uuid)
    return nil
}

// List of all indexes from the server.
func (client *IndexerClient) List() ([]api.IndexInfo, error) {
    var err  error
    var body []byte

    // Construct request body.
    indexreq := api.IndexRequest{Type: api.LIST}
    if body, err = json.Marshal(indexreq); err != nil {
        return nil, err
    }

    // Post HTTP request.
    bodybuf := bytes.NewBuffer(body)
    resp, err := http.Post(client.addr+"/list", "encoding/json", bodybuf)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    // Gather indexinfo
    indexres := api.IndexMetaResponse{}
    body, err = ioutil.ReadAll(resp.Body)
    if err != nil {
        return nil, err
    }
    if err = json.Unmarshal(body, &indexres); err != nil {
        return nil, err
    }
    if indexres.Status == api.ERROR {
        return nil, errors.New(indexres.Errors[0].Msg)
    }
    indexes := make(map[string]*api.IndexInfo)
    indexinfos := make([]api.IndexInfo, 0, len(indexres.Indexes))
    for _, info := range indexres.Indexes {
        indexes[info.Uuid] = &info
        indexinfos = append(indexinfos, info)
    }

    // Save the list in the client.
    client.Lock()
    defer client.Unlock()
    client.indexes = indexes
    return indexinfos, nil
}

// Get Finder interface for the index.
func (client *IndexerClient) Index(uuid string) *api.IndexInfo {
    client.RLock()
    defer client.RUnlock()
    for _, index := range client.indexes {
        if index.Uuid == uuid {
            return index
        }
    }
    return nil
}

func (client *IndexerClient) Trait(index *api.IndexInfo, op interface{}) (
    api.TraitInfo) {

    panic("Yet to be implemented")
}

// Scan for index entries.
func (client *IndexerClient) Scan(index *api.IndexInfo, q api.QueryParams) (
    []api.IndexRow, error) {

    var err  error
    var body []byte

    // Construct request body.
    indexreq := api.IndexRequest{Type: api.SCAN, Indexinfo: *index, Params: q}
    if body, err = json.Marshal(indexreq); err != nil {
        return nil, err
    }

    // Post HTTP request.
    bodybuf := bytes.NewBuffer(body)
    resp, err := http.Post(client.addr+"/scan", "encoding/json", bodybuf)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    // Gather indexinfo
    indexres := api.IndexScanResponse{}
    body, err = ioutil.ReadAll(resp.Body)
    if err != nil {
        return nil, err
    }
    if err = json.Unmarshal(body, &indexres); err != nil {
        return nil, err
    }
    if indexres.Status == api.ERROR {
        return nil, errors.New(indexres.Errors[0].Msg)
    }
    return indexres.Rows, nil

}
