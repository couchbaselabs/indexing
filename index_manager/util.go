package main

import (
	"net/http"
	. "github.com/couchbaselabs/indexing/api"
	"log"
	"bytes"
	"encoding/json"
)

var httpc = http.DefaultClient

func sendCreateToIndexer(indexinfo IndexInfo) error {

	var body []byte
	var resp *http.Response
	var err error

	// Construct request body.
	indexreq := IndexRequest{Type: CREATE, Index: indexinfo}
	if body, err = json.Marshal(indexreq); err == nil {
		// Post HTTP request.
		bodybuf := bytes.NewBuffer(body)
		url := node.IndexerURL + "/create"
		log.Printf("Posting %v to URL %v", bodybuf, url)
		if resp, err = httpc.Post(url, "application/json", bodybuf); err == nil {
			defer resp.Body.Close()
			_, err = metaResponse(resp)
		}
	}
	return err

} 

func sendDropToIndexer(uuid string) error {

	var body []byte
	var err error

	// Construct request body.
	index := IndexInfo{Uuid: uuid}
	indexreq := IndexRequest{Type: DROP, Index: index}
	if body, err = json.Marshal(indexreq); err == nil {
		// Post HTTP request.
		bodybuf := bytes.NewBuffer(body)
		url := node.IndexerURL + "/drop"
		log.Printf("Posting %v to URL %v", bodybuf, url)
		if resp, err := httpc.Post(url, "application/json", bodybuf); err == nil {
			defer resp.Body.Close()
			_, err = metaResponse(resp)
		}
	}
	return err
}




