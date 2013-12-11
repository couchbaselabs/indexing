// Server application serves index engine via rest API.
// TODO :
//  - Create a goroutine that will pull mutations from UPR and update the
//    index.

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/indexing/catalog"
	"github.com/couchbaselabs/indexing/engine/llrb"
	"log"
	"net/http"
	"strconv"
	"sync"
)

var c api.IndexCatalog
var ddlLock sync.Mutex

const (
	DEFAULT_LIMIT int = 100
)

func main() {
	var err error

	// Create index catalog
	if c, err = catalog.NewIndexCatalog("./"); err != nil {
		panic(err)
	}

	addr := ":8095"
	// Subscribe to HTTP server handlers
	http.HandleFunc("/create", handleCreate)
	http.HandleFunc("/drop", handleDrop)
	http.HandleFunc("/scan", handleScan)
	http.HandleFunc("/stats", handleStats)
	log.Println("Indexer Listening on", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Println("Fatal:", err)
	}
}

// /create
func handleCreate(w http.ResponseWriter, r *http.Request) {
	var res api.IndexMetaResponse
	var err error

	indexinfo := indexRequest(r).Index // Get IndexInfo

	ddlLock.Lock()
	defer ddlLock.Unlock()

	if _, err = c.Create(indexinfo); err == nil {
		if err = getIndexEngine(&indexinfo); err == nil {
			res = api.IndexMetaResponse{
				Status: api.SUCCESS,
			}
			log.Printf("Created index(%v) %v", indexinfo.Uuid, indexinfo.Name)
		}
	}
	if err != nil {
		res = createMetaResponseFromError(err)
		log.Println("ERROR: Failed to create index", err)
	}
	sendResponse(w, res)
}

// /drop
func handleDrop(w http.ResponseWriter, r *http.Request) {
	var res api.IndexMetaResponse

	indexinfo := indexRequest(r).Index

	ddlLock.Lock()
	defer ddlLock.Unlock()

	if _, err := c.Drop(indexinfo.Uuid); err == nil {
		res = api.IndexMetaResponse{
			Status: api.SUCCESS,
		}
		log.Printf("Dropped index(%v) %v", indexinfo.Uuid, indexinfo.Name)
	} else {
		res = createMetaResponseFromError(err)
		log.Println("ERROR: Failed to drop index", err)
	}
	sendResponse(w, res)
}

// /scan
func handleScan(w http.ResponseWriter, r *http.Request) {
	var err error

	indexinfo := indexRequest(r).Index // Gather request

	// Gather and normalizequery parameters
	r.ParseForm()
	q := api.QueryParams{
		Low:       api.Key(api.String(r.Form["Low"][0])),
		High:      api.Key(api.String(r.Form["High"][0])),
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
		q.Limit = DEFAULT_LIMIT
	} else {
		q.Limit, _ = strconv.Atoi(limit)
	}

	// Scan
	rows := make([]api.IndexRow, 0)
	if indexinfo, err = c.Index(indexinfo.Uuid); err == nil {
		if q.Low == nil && q.High == nil {
			rows, err = scanQuery(
				&indexinfo, q.Offset, q.Limit)
		} else if bytes.Compare(q.Low.Bytes(), q.High.Bytes()) == 0 {
			rows, err = rangeQuery(
				&indexinfo, q.Low, q.High, q.Inclusion, q.Offset, q.Limit)
		} else if q.Low != nil && q.High != nil {
			rows, err = lookupQuery(
				&indexinfo, q.Low, q.Offset, q.Limit)
		}
	}
	// send back the response
	sendScanResponse(w, rows, err)
}

// /stats.
func handleStats(w http.ResponseWriter, r *http.Request) {
	panic("Not yet impleted")
}

//---- helper functions
func scanQuery(indexinfo *api.IndexInfo, offset, limit int) (
	[]api.IndexRow, error) {

	if looker, ok := indexinfo.Engine.(api.Looker); ok {
		chkv, cherr := looker.KVSet()
		return receiveKV(chkv, cherr, offset, limit)
	}
	err := errors.New("index does not support Looker interface")
	return nil, err
}

func rangeQuery(
	indexinfo *api.IndexInfo, low, high api.Key, incl api.Inclusion, offset,
	limit int) ([]api.IndexRow, error) {

	if ranger, ok := indexinfo.Engine.(api.Ranger); ok {
		chkv, cherr, _ := ranger.KVRange(low, high, incl)
		return receiveKV(chkv, cherr, offset, limit)
	}
	err := errors.New("index does not support ranger interface")
	return nil, err
}

func lookupQuery(indexinfo *api.IndexInfo, key api.Key, offset, limit int) (
	[]api.IndexRow, error) {

	if looker, ok := indexinfo.Engine.(api.Looker); ok {
		ch, cherr := looker.Lookup(key)
		return receiveValue(key, ch, cherr, offset, limit)
	}
	err := errors.New("index does not support looker interface")
	return nil, err
}

func sendResponse(w http.ResponseWriter, res interface{}) {
	var buf []byte
	var err error
	header := w.Header()
	header["Content-Type"] = []string{"application/json"}

	if buf, err = json.Marshal(&res); err != nil {
		log.Println("Unable to marshal response", res)
	}
	w.Write(buf)
}

func sendScanResponse(w http.ResponseWriter, rows []api.IndexRow, err error) {
	var res api.IndexScanResponse

	if err == nil {
		res = api.IndexScanResponse{
			Status:    api.SUCCESS,
			TotalRows: int64(len(rows)),
			Rows:      rows,
			Errors:    nil,
		}
	} else {
		indexerr := api.IndexError{Code: string(api.ERROR), Msg: err.Error()}
		res = api.IndexScanResponse{
			Status:    api.SUCCESS,
			TotalRows: int64(0),
			Rows:      nil,
			Errors:    []api.IndexError{indexerr},
		}
	}
	sendResponse(w, res)
}

func receiveKV(ch chan api.KV, cherr chan error, offset, limit int) (
	[]api.IndexRow, error) {

	for offset > 0 {
		select {
		case <-ch:
			offset--
		case err := <-cherr:
			return nil, err
		}
	}

	rows := make([]api.IndexRow, 0, limit)
	for limit > 0 {
		select {
		case kv := <-ch:
			key, value := kv[0].(api.Key), kv[1].(api.Value)
			row := api.IndexRow{
				Key:   string(key.Bytes()),
				Value: string(value.Bytes()),
			}
			rows = append(rows, row)
			limit--
		case err := <-cherr:
			return rows, err
		}
	}
	return rows, nil
}

func receiveValue(key api.Key, ch chan api.Value, cherr chan error, offset, limit int) (
	[]api.IndexRow, error) {

	for offset > 0 {
		select {
		case <-ch:
			offset--
		case err := <-cherr:
			return nil, err
		}
	}

	rows := make([]api.IndexRow, 0, limit)
	for limit > 0 {
		select {
		case value := <-ch:
			row := api.IndexRow{
				Key:   string(key.Bytes()),
				Value: string(value.Bytes()),
			}
			rows = append(rows, row)
			limit--
		case err := <-cherr:
			return rows, err
		}
	}
	return rows, nil
}

// Parse HTTP Request to get IndexInfo.
func indexRequest(r *http.Request) *api.IndexRequest {
	indexreq := api.IndexRequest{}
	buf := make([]byte, r.ContentLength, r.ContentLength)
	r.Body.Read(buf)
	json.Unmarshal(buf, &indexreq)
	return &indexreq
}

func createMetaResponseFromError(err error) api.IndexMetaResponse {

	indexerr := api.IndexError{Code: string(api.ERROR), Msg: err.Error()}
	res := api.IndexMetaResponse{
		Status: api.ERROR,
		Errors: []api.IndexError{indexerr},
	}
	return res
}

// Instantiate index engine
func getIndexEngine(index *api.IndexInfo) error {
	var err error
	index.Engine = nil
	switch index.Using {
	case api.Llrb:
		index.Engine = llrb.NewIndexEngine(index.Name)
	default:
		err = errors.New(fmt.Sprintf("Invalid index-type, `%v`", index.Using))
	}
	return err
}
