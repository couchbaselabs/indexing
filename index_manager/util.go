//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

import (
	"bytes"
	"encoding/json"
	. "github.com/couchbaselabs/indexing/api"
	"github.com/couchbaselabs/indexing/index_manager/client"
	"log"
	"net/http"
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
			_, err = client.MetaResponse(resp)
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
			_, err = client.MetaResponse(resp)
		}
	}
	return err
}
