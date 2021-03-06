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
	"flag"
	"github.com/couchbaselabs/indexing/api"
	imclient "github.com/couchbaselabs/indexing/index_manager/client"
	ast "github.com/couchbaselabs/tuqtng/ast"
	"github.com/prataprc/go-couchbase"
	"log"
)

// TODO:
// [1] the node in which router runs will have to be mentioned in indexinfo
//     structure. once that is available change the projector accordingly
//
// 1. Automatic reconnect when indexer fails and restarts.
// 2. Randomly kill projector and restart must work from where the vuuid,seqno
//    was left off.
// 3. Try CREATE and DROP index multiple times.

var options struct {
	kvhost string
	imhost string
	inhost string // TODO: [1]
	nconn  int
	proto  string
}

const (
	GETSEQUENCE_VECTOR string = "MutationManager.GetSequenceVectors"
	PROCESS_1MUTATION  string = "MutationManager.ProcessSingleMutation"
	DEFAULT_NCONN      int    = 8
)

type bucketMeta struct {
	vector     api.SequenceVector
	indexMap   map[string]*api.IndexInfo
	indexExprs map[string][]ast.Expression
}
type bucketMap map[string]*bucketMeta

// Supervisor messages
type ImNotify struct {
	serverUuid string
}
type ExitRoutine struct {
	kill chan bool
	err  error
}

type projectorInfo struct {
	imanager   *imclient.RestClient
	serverUuid string
	buckets    bucketMap
}

type streamer interface {
	openFeed(api.SequenceVector) error
	closeFeed()
}

func main() {
	argParse()
	// Couchbase client, pool and default bucket
	couch, err := couchbase.Connect("http://" + options.kvhost)
	if err != nil {
		log.Fatalf("Error connecting:  %v", err)
	}
	pool, err := couch.GetPool("default")
	if err != nil {
		log.Fatalf("Error getting pool:  %v", err)
	}

	p := &projectorInfo{
		imanager: imclient.NewRestClient("http://" + options.imhost),
	}
	superch := make(chan interface{})
	for {
		workers := make([]chan bool, 0)
		if p.serverUuid, err = p.getMetaData(); err != nil {
			continue
		}
		for bucket, bmeta := range p.buckets {
			bw := NewBucketWorker(BucketWorkerCmd{
				client:     couch,
				pool:       &pool,
				bucketname: bucket,
				bmeta:      bmeta,
				nconn:      options.nconn,
				rpcurl:     options.inhost,
			})
			// Workers to push mutations to indexer through parallel connection
			for _, iclients := range bw.mclients {
				for _, s := range iclients {
					killch := make(chan bool)
					go push2Indexer(s, superch, killch)
					workers = append(workers, killch)
				}
			}
			// Workers to gather UprEvents from upr-feed.
			killch := make(chan bool)
			go bw.run(superch, killch)
			workers = append(workers, killch)
		}
		go p.waitNotify(p.serverUuid, superch)
		select {
		case msg, _ := <-superch:
			if notify, ok := msg.(ImNotify); ok {
				p.serverUuid = notify.serverUuid
			} else if exit, ok := msg.(ExitRoutine); ok {
				log.Println(exit.err)
			} else {
				panic("Unknown supervisor message")
			}
		}
		// Kill all workers
		for _, ch := range workers {
			close(ch)
		}
	}
	// TODO: index_manager/client/client.go should implement Close() api.
	// p.imanager.Close()
	p.close()
}

func argParse() {
	flag.StringVar(&options.kvhost, "kvhost", "localhost:11211",
		"Port to connect to kv-cluster")
	flag.StringVar(&options.inhost, "inhost", "localhost:8096",
		"Port to connect to indexer node") // TODO [1]
	flag.StringVar(&options.imhost, "imhost", "localhost:8094",
		"Port to connect to index-manager node")
	flag.StringVar(&options.proto, "proto", "upr",
		"Use either `tap` or `upr`")
	flag.IntVar(&options.nconn, "nconn", DEFAULT_NCONN,
		"Number of indexer (rpc) connections ber bucket")
	flag.Parse()
}
