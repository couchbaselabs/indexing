// Server application serves index engine via rest API.
// TODO : Create a goroutine that will pull mutations from UPR and update the
// index.

package main

import (
    "net/http"
    "encoding/json"
    "strconv"
    "errors"
    "bytes"
    "github.com/couchbaselabs/indexing/api"
    "github.com/couchbaselabs/indexing/server"
)

var indexer api.Indexer

func main() {
    var err error

    // Create indexer catalog
    indexer, err = server.NewIndexCatalog("./")
    if  err != nil {
        panic(err)
    }

    // Subscribe to HTTP server handlers
    http.HandleFunc("/create", handleCreate)
    http.HandleFunc("/drop", handleDrop)
    http.HandleFunc("/list", handleList)
    http.HandleFunc("/scan", handleScan)
    http.HandleFunc("/stats", handleStats)
    http.ListenAndServe(":8094", nil)
}

// /create
func handleCreate(w http.ResponseWriter, r *http.Request) {
    var err      error
    var buf      []byte

    // Get IndexInfo, without the `uuid`.
    index := handleRequest(r)
    // Normalize IndexInfo
    if index.Exprtype == "" {
        index.Exprtype = api.N1QL
        index.Expression = index.CreateStmt
    }

    // Create.
    err = indexer.Create(index);

    // Send the reponse.
    res := indexMetaResponse(index, err)
    header := w.Header()
    header["Content-Type"] = []string{"encoding/json"}
    buf, _ = json.Marshal(&res)
    w.Write(buf)
}

// /drop
func handleDrop(w http.ResponseWriter, r *http.Request) {
    var err error

    // Gather `uuid` to remove
    index := handleRequest(r)

    // Drop.
    if err == nil {
        err = indexer.Drop(index.Uuid)
    }

    // Send back the response
    res := indexMetaResponse(index, err)
    header := w.Header()
    header["Content-Type"] = []string{"encoding/json"}
    buf, _ := json.Marshal(&res)
    w.Write(buf)
}

// /list.
func handleList(w http.ResponseWriter, r *http.Request) {
    indexes, _ := indexer.List()
    res := api.IndexMetaResponse{
        Status:  api.SUCCESS, Indexes: indexes, Errors:  nil,
    }
    header := w.Header()
    header["Content-Type"] = []string{"encoding/json"}
    buf, _ := json.Marshal(&res)
    w.Write(buf)
}

// /scan.
func handleScan(w http.ResponseWriter, r *http.Request) {
    var err      error

    // Gather request
    index := handleRequest(r)

    // Gather and normalizequery parameters
    r.ParseForm()
    q := api.QueryParams{
        Low:       api.Key(r.Form["Low"][0]),
        High:      api.Key(r.Form["High"][0]),
        Inclusion: api.Inclusion(r.Form["Inclusion"][0]),
    }
    offset := r.Form["Offset"][0]
    if offset == "" {
        q.Offset = 0
    } else {
        q.Offset, _ = strconv.Atoi(offset)
    }
    limit := r.Form["Limit"][0]
    if limit == "" {
        q.Limit = server.DEFAULT_LIMIT
    } else {
        q.Limit, _ = strconv.Atoi(limit)
    }

    // Scan
    index = indexer.Index(index.Uuid)
    rows := make([]api.IndexRow, 0)
    if err == nil {
        if q.Low == nil && q.High == nil {            // Scan query
            rows, err = scanQuery(index, q.Offset, q.Limit)
        } else if bytes.Compare(q.Low, q.High) == 0 { // Lookup query
            rows, err =
                rangeQuery(index, q.Low, q.High, q.Inclusion, q.Offset, q.Limit)
        } else if q.Low != nil && q.High != nil {  // Range query
            rows, err = lookupQuery(index, q.Low, q.Offset, q.Limit)
        }
    }
    // send back the response
    res := indexScanResponse(rows, err)
    header := w.Header()
    header["Content-Type"] = []string{"encoding/json"}
    buf, _ := json.Marshal(&res)
    w.Write(buf)
}

// /stats.
func handleStats(w http.ResponseWriter, r *http.Request) {
    panic("Not yet impleted")
}

//---- helper functions
func scanQuery(index *api.IndexInfo, offset, limit int) (
    []api.IndexRow, error) {

    if looker, ok := index.Index.(api.Looker); ok {
        ch, cherr := looker.KVSet()
        return receiveKV(ch, cherr, offset, limit)
    }
    err := errors.New("index does not support Looker interface")
    return nil, err
}

func rangeQuery(
    index *api.IndexInfo, low, high api.Key, incl api.Inclusion, offset, limit int) (
        []api.IndexRow, error) {

    if ranger, ok := index.Index.(api.Ranger); ok {
        ch, cherr, _ := ranger.KVRange(low, high, incl)
        return receiveKV(ch, cherr, offset, limit)
    }
    err := errors.New("index does not support ranger interface")
    return nil, err
}

func lookupQuery(index *api.IndexInfo, key api.Key, offset, limit int) (
    []api.IndexRow, error) {

    if looker, ok := index.Index.(api.Looker); ok {
        ch, cherr := looker.Lookup(key)
        return receiveValue(key, ch, cherr, offset, limit)
    }
    err := errors.New("index does not support looker interface")
    return nil, err
}

func handleRequest(r *http.Request) *api.IndexInfo {
    // Get IndexInfo, without the `uuid`.
    indexreq := api.IndexRequest{}
    buf := make([]byte, 0, r.ContentLength)
    r.Body.Read(buf)
    json.Unmarshal(buf, &indexreq)
    return &indexreq.Indexinfo
}

func indexMetaResponse(index *api.IndexInfo, err error) (
    res api.IndexMetaResponse) {

    if err == nil {
        res = api.IndexMetaResponse{
            Status:  api.SUCCESS, Indexes: []api.IndexInfo{*index}, Errors:  nil,
        }
    } else {
        indexerr := api.IndexError{Code: string(api.ERROR), Msg: err.Error()}
        res = api.IndexMetaResponse{
            Status:  api.ERROR,
            Indexes: nil,
            Errors:  []api.IndexError{indexerr},
        }
    }
    return res
}

func indexScanResponse(rows []api.IndexRow, err error) (
    res api.IndexScanResponse) {

    if err == nil {
        res = api.IndexScanResponse{
            Status: api.SUCCESS, TotalRows: int64(len(rows)),
            Rows:   rows,        Errors:    nil,
        }
    } else {
        indexerr := api.IndexError{Code: string(api.ERROR), Msg: err.Error()}
        res = api.IndexScanResponse{
            Status: api.SUCCESS, TotalRows: int64(0),
            Rows:   nil,
            Errors:  []api.IndexError{indexerr},
        }
    }
    return res
}

func receiveKV(ch chan api.KV, cherr chan error, offset, limit int) (
    []api.IndexRow, error) {

    for offset > 0 {
        select {
        case <-ch:           offset--
        case err := <-cherr: return nil, err
        }
    }

    rows := make([]api.IndexRow, 0, limit)
    for limit > 0 {
        select {
        case kv := <-ch:
            row := api.IndexRow{Key: string(kv[0]), Value: string(kv[1])}
            rows = append(rows, row)
            limit--
        case err := <-cherr:
            return nil, err
        }
    }
    return rows, nil
}

func receiveValue(key api.Key, ch chan api.Value, cherr chan error, offset, limit int) (
    []api.IndexRow, error) {

    for offset > 0 {
        select {
        case <-ch:           offset--
        case err := <-cherr: return nil, err
        }
    }

    rows := make([]api.IndexRow, 0, limit)
    for limit > 0 {
        select {
        case value := <-ch:
            row := api.IndexRow{Key: string(key), Value: string(value)}
            rows = append(rows, row)
            limit--
        case err := <-cherr:
            return nil, err
        }
    }
    return rows, nil
}
