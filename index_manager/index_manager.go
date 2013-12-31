// Server application serves index engine via rest API.
// TODO :
//  - Create a goroutine that will pull mutations from UPR and update the
//    index.

package main

import (
	"encoding/json"
	"fmt"
	"github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/indexing/catalog"
	"github.com/nu7hatch/gouuid" // TODO: Remove this dependancy ??
	"log"
	"net/http"
	"sync"
)

var c catalog.IndexCatalog
var longPolls = make([]chan string, 0)
var mutex sync.Mutex

//FIXME Get Node info from command line
var node = api.NodeInfo{IndexerURL: "http://localhost:8095"}
var ddlLock sync.Mutex

func main() {
	var err error

	// Create index catalog
	if c, err = catalog.NewIndexCatalog("./"); err != nil {
		panic(err)
	}

	addr := ":8094"
	// Subscribe to HTTP server handlers
	http.HandleFunc("/create", handleCreate)
	http.HandleFunc("/drop", handleDrop)
	http.HandleFunc("/list", handleList)
	http.HandleFunc("/nodes", handleNodes)
	http.HandleFunc("/notify", handleNotify)
	log.Println("Index Manager Listening on", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Println("Fatal:", err)
	}
}

// /create
func handleCreate(w http.ResponseWriter, r *http.Request) {
	var res api.IndexMetaResponse
	var servUuid string
	var err error

	//only one DDL (Create/Drop) can run concurrently
	ddlLock.Lock()
	defer ddlLock.Unlock()

	indexinfo := indexRequest(r).Index // Get IndexInfo, without the `uuid`

	// Normalize IndexInfo
	if indexinfo.Exprtype == "" {
		indexinfo.Exprtype = api.N1QL
	}

	if uvalue, err := uuid.NewV4(); err == nil {
		indexinfo.Uuid = fmt.Sprintf("%v", uvalue)
	} else {
		log.Fatalln("Unable to generate UUID for index")
	}

	if err = c.Exists(indexinfo.Name, indexinfo.Bucket); err == nil {
		if err = sendCreateToIndexer(indexinfo); err == nil {
			if servUuid, err = c.Create(indexinfo); err == nil {
				res = api.IndexMetaResponse{
					Status:     api.SUCCESS,
					Indexes:    []api.IndexInfo{indexinfo},
					ServerUuid: servUuid,
				}
				notifyLongPolls(servUuid)
				log.Printf("Created index(%v) %v", indexinfo.Uuid, indexinfo.OnExprList)
			}
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

	//only one DDL (Create/Drop) can run concurrently
	ddlLock.Lock()
	defer ddlLock.Unlock()

	if err = sendDropToIndexer(indexinfo.Uuid); err == nil {
		if servUuid, err := c.Drop(indexinfo.Uuid); err == nil {
			res = api.IndexMetaResponse{
				Status:     api.SUCCESS,
				ServerUuid: servUuid,
			}
			notifyLongPolls(servUuid)
			log.Printf("Dropped index(%v) %v", indexinfo.Uuid, indexinfo.Name)
		}
	}

	if err != nil {
		res = createMetaResponseFromError(err)
		log.Println("ERROR: Failed to drop index", err)
	}
	sendResponse(w, res)
}

// /list
func handleList(w http.ResponseWriter, r *http.Request) {
	var res api.IndexMetaResponse

	serverUuid := indexRequest(r).ServerUuid
	if servUuid, indexes, err := c.List(serverUuid); err == nil {
		res = api.IndexMetaResponse{
			Status:     api.SUCCESS,
			Indexes:    indexes,
			ServerUuid: servUuid,
		}
		log.Printf("List server %v", c.GetUuid())
	} else {
		res = createMetaResponseFromError(err)
		log.Println("ERROR: Listing server", err)
	}
	sendResponse(w, res)
}

// /nodes
func handleNodes(w http.ResponseWriter, r *http.Request) {
	res := api.IndexMetaResponse{Status: api.SUCCESS, Nodes: []api.NodeInfo{node}, Errors: nil}
	sendResponse(w, res)
	log.Printf("Nodes list returned %v", node)
}

// /notify
func handleNotify(w http.ResponseWriter, r *http.Request) {
	var res api.IndexMetaResponse
	var newServerUuid string

	//FIXME Use indexer.GetUuid instead of List?
	if servUuid, _, err := c.List(""); err == nil {
		req := indexRequest(r)

		log.Printf("Received Notify Request with ServerUuid %s", req.ServerUuid)
		if req.ServerUuid == servUuid {
			ch := make(chan string, 1)
			mutex.Lock()
			longPolls = append(longPolls, ch)
			mutex.Unlock()
			select {
			case newServerUuid = <-ch:
				log.Printf("Sending Notification to Client")
			case <-w.(http.CloseNotifier).CloseNotify():
				log.Printf("Connection Closed by Client. Notify thread closing.")
				return
			}
		}
		res = api.IndexMetaResponse{
			Status:     api.INVALID_CACHE,
			ServerUuid: newServerUuid,
		}
	} else {
		res = createMetaResponseFromError(err)
	}
	log.Printf("Exited Notify Request")
	sendResponse(w, res)

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

// Parse HTTP Request to get IndexInfo.
func indexRequest(r *http.Request) *api.IndexRequest {
	indexreq := api.IndexRequest{}
	buf := make([]byte, r.ContentLength, r.ContentLength)
	r.Body.Read(buf)
	json.Unmarshal(buf, &indexreq)
	return &indexreq
}

func notifyLongPolls(serverUuid string) {
	mutex.Lock()
	defer mutex.Unlock()
	for _, ch := range longPolls {
		ch <- serverUuid
	}
	longPolls = make([]chan string, 0)
}

func createMetaResponseFromError(err error) api.IndexMetaResponse {

	indexerr := api.IndexError{Code: string(api.ERROR), Msg: err.Error()}
	res := api.IndexMetaResponse{
		Status: api.ERROR,
		Errors: []api.IndexError{indexerr},
	}
	return res
}
