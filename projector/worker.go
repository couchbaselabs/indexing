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
	"fmt"
	"github.com/couchbaselabs/dparval"
	"github.com/couchbaselabs/indexing/api"
	ast "github.com/couchbaselabs/tuqtng/ast"
	"github.com/prataprc/go-couchbase"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
)

type BucketWorkerCmd struct {
	client     couchbase.Client
	pool       *couchbase.Pool
	bucketname string
	bmeta      *bucketMeta
	nconn      int
	rpcurl     string
}

type BucketWorker struct {
	client     couchbase.Client            // couchbase client
	pool       *couchbase.Pool             // pool where the bucket lives
	bucketname string                      // bucket for which to open the feed
	bmeta      *bucketMeta                 // bucket meta-data
	mclients   map[string][]*indexerClient // rpc clients per index
}

type indexerClient struct {
	client *rpc.Client
	mch    chan *api.Mutation
}

func NewBucketWorker(bwc BucketWorkerCmd) *BucketWorker {
	var rpcconn net.Conn
	var err error

	mclients := make(map[string][]*indexerClient)
	for uuid, _ := range bwc.bmeta.indexMap {
		iclients := make([]*indexerClient, 0, bwc.nconn)
		for i := 0; i < bwc.nconn; i++ {
			if rpcconn, err = net.Dial("tcp", bwc.rpcurl); err != nil {
				log.Printf(
					"error connecting with mutation server %v: %v\n", bwc.rpcurl, err)
				return nil
			}
			s := &indexerClient{
				client: jsonrpc.NewClient(rpcconn),
				mch:    make(chan *api.Mutation, 1000),
			}
			iclients = append(iclients, s)
		}
		mclients[uuid] = iclients
	}
	return &BucketWorker{
		client:     bwc.client,
		pool:       bwc.pool,
		bucketname: bwc.bucketname,
		bmeta:      bwc.bmeta,
		mclients:   mclients,
	}
}

func push2Indexer(s *indexerClient, quit chan interface{}, kill chan bool) {
	var r bool
	var err error

loop:
	for {
		select {
		case m, ok := <-s.mch:
			if ok {
				err = s.client.Call(PROCESS_1MUTATION, *m, &r)
			}
		case <-kill:
			break loop
		}
		if err != nil {
			quit <- ExitRoutine{kill, err}
			break
		}
	}
}

func (bw *BucketWorker) run(quit chan interface{}, kill chan bool) {
	var err error
	var pool couchbase.Pool
	var bucket *couchbase.Bucket

	finish := func() {
		if bucket != nil {
			bucket.Close()
		}
	}

	tryConnection(func() bool {
		// Refresh the pool to get any new buckets created on the server.
		if pool, err = bw.client.GetPool("default"); err != nil {
			fmt.Println("Error getting pool", err)
			finish()
			return false
		}
		bw.pool = &pool
		// Get bucket instance
		if bucket, err = bw.pool.GetBucket(bw.bucketname); err != nil {
			log.Printf("Unable to get bucket %v\n", bw.bucketname)
			finish()
			return false
		}
		return true
	})

	// Open feed
	bfeed := NewUprStreams(bucket)
	if err := bfeed.openFeed(bw.bmeta.vector); err != nil {
		log.Printf("Unable to open feed for bucket %v: %v\n", bw.bucketname, err)
		finish()
		return
	}

loop:
	for {
		select {
		case e, ok := <-bfeed.feed.C:
			if ok == false {
				quit <- ExitRoutine{kill, nil}
				break loop
			}
			for uuid, astexprs := range bw.bmeta.indexExprs {
				ii := bw.bmeta.indexMap[uuid]
				m := api.Mutation{
					Type:    api.UprEventName(e.Opstr),
					Indexid: uuid,
					Docid:   string(e.Key),
					Vbucket: e.Vbucket,
					Seqno:   e.Seqno,
				}
				if ii.IsPrimary && m.Type == api.INSERT {
					m.SecondaryKey = [][]byte{e.Key}
				} else if m.Type == api.INSERT {
					m.SecondaryKey = evaluate(e.Value, astexprs)
				}
				log.Println(e.Opstr, e.Seqno, uuid[:8], bw.bucketname, m.Docid, fmtSKey(m.SecondaryKey))
				x := int(e.Vbucket) % len(bw.mclients[uuid])
				bw.mclients[uuid][x].mch <- &m
			}
		case <-kill:
			break loop
		}
	}
	bfeed.closeFeed()
	// Close all connections with indexer.
	for _, sl := range bw.mclients {
		for _, s := range sl {
			close(s.mch)
		}
	}
}

func evaluate(value []byte, astexprs []ast.Expression) [][]byte {
	secKey := make([][]byte, 0)
	for _, expr := range astexprs {
		key, err := expr.Evaluate(dparval.NewValueFromBytes([]byte(value)))
		if err != nil {
			log.Println(err)
			secKey = append(secKey, []byte{})
		} else {
			secKey = append(secKey, key.Bytes())
		}
	}
	return secKey
}

func fmtSKey(keys [][]byte) []string {
	ss := make([]string, 0)
	for _, bs := range keys {
		ss = append(ss, string(bs))
	}
	return ss
}
