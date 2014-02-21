//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// Server application serves index engine via rest API.
// TODO :
//  - Create a goroutine that will pull mutations from UPR and update the
//    index.

package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/indexing/catalog"
	"github.com/couchbaselabs/indexing/engine/leveldb"
	"log"
	"net/http"
	"sync"
)

var c catalog.IndexCatalog
var ddlLock sync.Mutex
var chnotify chan ddlNotification
var engineMap map[string]api.Finder

var options struct {
	debugLog bool
}

func main() {
	var err error

	argParse()

	// Create index catalog
	if c, err = catalog.NewIndexCatalog("./", "icatalog.dat"); err != nil {
		log.Fatalf("Fatal error opening catalog: %v", err)
	}

	engineMap = make(map[string]api.Finder)
	//open engine for existing indexes and assign to engineMap
	openIndexEngine()

	//FIXME add error handing to this
	if chnotify, err = StartMutationManager(engineMap); err != nil {
		log.Printf("Error Starting Mutation Manager %v", err)
		return
	}

	addr := ":8095"
	// Subscribe to HTTP server handlers
	http.HandleFunc("/create", handleCreate)
	http.HandleFunc("/drop", handleDrop)
	http.HandleFunc("/scan", handleScan)
	http.HandleFunc("/stats", handleStats)

	//FIXME This doesn't work on Ctrl-C
	defer freeResourcesOnExit()
	log.Println("Indexer Listening on", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("Fatal: %v", err)
	}

}

// /create
func handleCreate(w http.ResponseWriter, r *http.Request) {
	var res api.IndexMetaResponse
	var err error

	indexinfo := indexRequest(r).Index // Get IndexInfo

	ddlLock.Lock()
	defer ddlLock.Unlock()

	if err = assignIndexEngine(&indexinfo); err == nil {
		if _, err = c.Create(indexinfo); err == nil {
			//notify mutation manager about the new index
			notification := ddlNotification{
				ddltype:   api.CREATE,
				engine:    engineMap[indexinfo.Uuid],
				indexinfo: indexinfo,
			}
			chnotify <- notification

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
	var err error

	indexinfo := indexRequest(r).Index

	ddlLock.Lock()
	defer ddlLock.Unlock()

	if indexinfo, err = c.Index(indexinfo.Uuid); err == nil {
		//Notify mutation manager before destroying the engine
		notification := ddlNotification{
			ddltype:   api.DROP,
			engine:    engineMap[indexinfo.Uuid],
			indexinfo: indexinfo,
		}
		chnotify <- notification

		if err = engineMap[indexinfo.Uuid].Destroy(); err == nil {
			if _, err = c.Drop(indexinfo.Uuid); err == nil {
				res = api.IndexMetaResponse{
					Status: api.SUCCESS,
				}
				log.Printf("Dropped index(%v) %v", indexinfo.Uuid, indexinfo.Name)
			}
		}

		if err != nil {
			//FIXME need to renotify the mutation manager??
		}
	}

	if err != nil {
		res = createMetaResponseFromError(err)
		log.Println("ERROR: Failed to drop index", err)
	}
	sendResponse(w, res)
}

// /scan
func handleScan(w http.ResponseWriter, r *http.Request) {
	var err error

	indexreq := indexRequest(r) // Gather request
	uuid := indexreq.Index.Uuid
	q := indexreq.Params

	if options.debugLog {
		log.Printf("Received Scan Index %v Params %v %v", uuid, q.Low, q.High)
	}

	// Scan
	rows := make([]api.IndexRow, 0)
	var totalRows uint64
	var lowkey, highkey api.Key

	if lowkey, err = api.NewKey(q.Low, ""); err != nil {
		sendScanResponse(w, nil, 0, err)
		return
	}

	if highkey, err = api.NewKey(q.High, ""); err != nil {
		sendScanResponse(w, nil, 0, err)
		return
	}

	var indexinfo api.IndexInfo
	if indexinfo, err = c.Index(uuid); err == nil {
		switch q.ScanType {

		case api.COUNT:
			totalRows, err = countQuery(&indexinfo, q.Limit)

		case api.EXISTS:
			var exists bool
			exists, err = existsQuery(&indexinfo, lowkey)
			if exists {
				totalRows = 1
			}

		case api.LOOKUP:

			rows, err = lookupQuery(&indexinfo, lowkey, q.Limit)
			totalRows = uint64(len(rows))

		case api.RANGESCAN:

			rows, err = rangeQuery(&indexinfo, lowkey, highkey, q.Inclusion, q.Limit)
			totalRows = uint64(len(rows))

		case api.FULLSCAN:
			rows, err = scanQuery(&indexinfo, q.Limit)
			totalRows = uint64(len(rows))

		case api.RANGECOUNT:
			totalRows, err = rangeCountQuery(&indexinfo, lowkey, highkey, q.Inclusion, q.Limit)
		}
	}
	// send back the response
	sendScanResponse(w, rows, totalRows, err)
}

// /stats.
func handleStats(w http.ResponseWriter, r *http.Request) {
	panic("Not yet implemented")
}

//---- helper functions

func countQuery(indexinfo *api.IndexInfo, limit int64) (
	uint64, error) {

	if counter, ok := engineMap[indexinfo.Uuid].(api.Counter); ok {
		count, err := counter.CountTotal()
		return count, err
	}
	err := errors.New("Index does not support Looker interface")
	return uint64(0), err
}

func existsQuery(indexinfo *api.IndexInfo, key api.Key) (bool, error) {

	if exister, ok := engineMap[indexinfo.Uuid].(api.Exister); ok {
		exists := exister.Exists(key)
		return exists, nil
	}
	err := errors.New("Index does not support Exister interface")
	return false, err
}

func scanQuery(indexinfo *api.IndexInfo, limit int64) (
	[]api.IndexRow, error) {

	if looker, ok := engineMap[indexinfo.Uuid].(api.Looker); ok {
		ch, cherr := looker.ValueSet()
		return receiveValue(ch, cherr, limit)
	}
	err := errors.New("Index does not support Looker interface")
	return nil, err
}

func rangeQuery(
	indexinfo *api.IndexInfo, low, high api.Key, incl api.Inclusion,
	limit int64) ([]api.IndexRow, error) {

	if ranger, ok := engineMap[indexinfo.Uuid].(api.Ranger); ok {
		ch, cherr, _ := ranger.ValueRange(low, high, incl)
		return receiveValue(ch, cherr, limit)
	}
	err := errors.New("Index does not support ranger interface")
	return nil, err
}

func lookupQuery(indexinfo *api.IndexInfo, key api.Key, limit int64) (
	[]api.IndexRow, error) {

	if looker, ok := engineMap[indexinfo.Uuid].(api.Looker); ok {
		if options.debugLog {
			log.Printf("Looking up key %s", key.String())
		}
		ch, cherr := looker.Lookup(key)
		return receiveValue(ch, cherr, limit)
	}
	err := errors.New("Index does not support looker interface")
	return nil, err
}

func rangeCountQuery(
	indexinfo *api.IndexInfo, low, high api.Key, incl api.Inclusion,
	limit int64) (uint64, error) {

	if rangeCounter, ok := engineMap[indexinfo.Uuid].(api.RangeCounter); ok {
		totalRows, err := rangeCounter.CountRange(low, high, incl)
		return totalRows, err
	}
	err := errors.New("Index does not support RangeCounter interface")
	return 0, err
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

func sendScanResponse(w http.ResponseWriter, rows []api.IndexRow, totalRows uint64, err error) {
	var res api.IndexScanResponse

	if err == nil {
		res = api.IndexScanResponse{
			Status:    api.SUCCESS,
			TotalRows: totalRows,
			Rows:      rows,
			Errors:    nil,
		}
	} else {
		indexerr := api.IndexError{Code: string(api.ERROR), Msg: err.Error()}
		res = api.IndexScanResponse{
			Status:    api.ERROR,
			TotalRows: uint64(0),
			Rows:      nil,
			Errors:    []api.IndexError{indexerr},
		}
	}
	sendResponse(w, res)
}

func receiveValue(ch chan api.Value, cherr chan error, limit int64) (
	[]api.IndexRow, error) {

	//FIXME limit should be sent to the engine and only limit response be sent on the
	//channel
	rows := make([]api.IndexRow, 0)
	var nolimit = false
	if limit == 0 {
		nolimit = true
	}
	ok := true
	var value api.Value
	var err error
	for ok && (limit > 0 || nolimit) {
		select {
		case value, ok = <-ch:
			if ok {
				if options.debugLog {
					log.Printf("Indexer Received Value %s", value.String())
				}
				row := api.IndexRow{
					Key:   value.KeyBytes(),
					Value: value.Docid(),
				}
				rows = append(rows, row)
				limit--
			}
		case err, ok = <-cherr:
			if err != nil {
				return rows, err
			}
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
func assignIndexEngine(indexinfo *api.IndexInfo) error {
	var err error
	switch indexinfo.Using {
	case api.LevelDB:
		engineMap[indexinfo.Uuid] = leveldb.NewIndexEngine(indexinfo.Uuid)
	default:
		err = errors.New(fmt.Sprintf("Invalid index-type, `%v`", indexinfo.Using))
	}
	return err
}

func openIndexEngine() error {

	var err error
	var indexinfos []api.IndexInfo
	//For the existing indexes, open the existing engine
	if _, indexinfos, err = c.List(""); err != nil {
		log.Printf("Error while retrieving index list %v", err)
		return err
	}

	for _, indexinfo := range indexinfos {
		log.Printf("Try Finding Existing Engine for Index %v", indexinfo)
		switch indexinfo.Using {
		case api.LevelDB:
			engineMap[indexinfo.Uuid] = leveldb.OpenIndexEngine(indexinfo.Uuid)
			log.Printf("Got Existing Engine for Index %v", indexinfo.Uuid)
		default:
			err = errors.New(fmt.Sprintf("Unknown Index Type. Skipping Opening Engine"))
		}
	}

	return err

}

func freeResourcesOnExit() {

	//purge the catalog
	if err := c.Purge(); err != nil {
		log.Printf("Error Purging Catalog %v", err)
	}

	//close the index engines
	if err := closeIndexEngines(); err != nil {
		log.Printf("Error Closing Index Engine %v", err)
	}

	//FIXME close the mutation manager?

}

func closeIndexEngines() error {

	if _, indexinfos, err := c.List(""); err == nil {

		for _, indexinfo := range indexinfos {
			if err := engineMap[indexinfo.Uuid].Close(); err != nil {
				return err
			}
		}
	} else {
		return err
	}

	return nil
}

func argParse() {
	flag.BoolVar(&options.debugLog, "debugLog", false, "Debug Logging Enabled")
	flag.Parse()
	api.DebugLog = options.debugLog
}
