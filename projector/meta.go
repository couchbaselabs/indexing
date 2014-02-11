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
	"github.com/couchbaselabs/indexing/api"
	ast "github.com/couchbaselabs/tuqtng/ast"
	"log"
	"net"
	"net/rpc/jsonrpc"
	"time"
)

// exponential backoff while connection retry.
const initialRetryInterval = 1 * time.Second
const maximumRetryInterval = 30 * time.Second

func (p *projectorInfo) getMetaData() (err error) {
	var rpcconn net.Conn
	var returnMap api.IndexSequenceMap
	var indexinfos []api.IndexInfo
	var ex ast.Expression

	bmap := make(bucketMap)
	tryConnection(func() bool {
		// Create a map of indexinfos structured around buckets.
		if p.serverUuid, indexinfos, err = p.imanager.List(""); err != nil {
			log.Println("Error getting list:", err)
			return false
		}
		for i, ii := range indexinfos {
			bmeta := bmap[ii.Bucket]
			if bmeta == nil {
				bmeta = &bucketMeta{
					indexMap:   make(map[string]*api.IndexInfo),
					indexExprs: make(map[string][]ast.Expression),
				}
			}
			bmeta.indexMap[ii.Uuid] = &indexinfos[i]
			// Get sequence vector for each index
			url := options.inhost // TODO [1]
			// url := ii.RouterNode
			if rpcconn, err = net.Dial("tcp", url); err != nil {
				log.Printf("error connecting with indexer %v: %v\n", url, err)
				return false
			}
			c := jsonrpc.NewClient(rpcconn)
			indexList := api.IndexList{ii.Uuid}
			if err = c.Call(GETSEQUENCE_VECTOR, &indexList, &returnMap); err != nil {
				log.Printf("getting sequence vector %v: %v\n", url, err)
				return false
			}
			// Build sequence vector per bucket based on the collection of indexes
			// defined on the bucket.
			for _, vector := range returnMap {
				for vb, seqno := range vector {
					if bmeta.vector[vb] == 0 || bmeta.vector[vb] > seqno {
						bmeta.vector[vb] = seqno
					}
				}
			}
			// AST expression
			astexprs := make([]ast.Expression, 0)
			for _, expr := range ii.OnExprList {
				ex, err = ast.UnmarshalExpression([]byte(expr))
				if err != nil {
					log.Printf("unmarshal error: %v", err)
					return false
				}
				astexprs = append(astexprs, ex)
			}
			bmeta.indexExprs[ii.Uuid] = astexprs
			bmap[ii.Bucket] = bmeta
		}
		log.Printf("Got %v indexes in %v buckets\n", len(indexinfos), len(bmap))
		return true
	})
	p.buckets = bmap
	return
}

func (p *projectorInfo) waitNotify(notifych chan string) {
	var err error
	if _, p.serverUuid, err = p.imanager.Notify(p.serverUuid); err != nil {
		log.Println("Index manager closed connection:", err)
	}
	notifych <- p.serverUuid
	close(notifych)
	log.Println("Projector exiting wait-notify")
}

func (p *projectorInfo) close() {
	// TODO: index_manager/client/client.go should implement Close() api.
	// p.imanager.Close()
}

func tryConnection(fn func() bool) {
	retryInterval := initialRetryInterval
	for {
		ok := fn()
		if ok {
			return
		}
		log.Printf("Retrying after %v seconds ...\n", retryInterval)
		<-time.After(retryInterval)
		if retryInterval *= 2; retryInterval > maximumRetryInterval {
			retryInterval = maximumRetryInterval
		}
	}
}
