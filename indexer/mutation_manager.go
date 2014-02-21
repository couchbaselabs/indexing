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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbaselabs/indexing/api"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"
)

type MutationManager struct {
	enginemap   map[string]api.Finder
	sequencemap api.IndexSequenceMap
	chmutation  chan *api.Mutation                       //buffered channel to store incoming mutations
	chworkers   [MAX_MUTATION_WORKERS]chan *api.Mutation //buffered channel for each worker
	chseq       chan seqNotification                     //buffered channel to store sequence notifications from workers
	chddl       chan ddlNotification                     //channel for incoming ddl notifications
}

type ddlNotification struct {
	indexinfo api.IndexInfo
	engine    api.Finder
	ddltype   api.RequestType
}

type seqNotification struct {
	engine  api.Finder
	indexid string
	seqno   uint64
	vbucket uint16
}

//error state flag
var indexerErrorState bool
var indexerErrorString string
var wg sync.WaitGroup
var chdrain chan bool

const MAX_INCOMING_QUEUE = 50000
const MAX_MUTATION_WORKERS = 8
const MAX_WORKER_QUEUE = 1000
const MAX_SEQUENCE_QUEUE = 50000
const META_DOC_ID = "."
const SEQ_MAP_PERSIST_INTERVAL = 10 //number of mutations after which sequence map is persisted

var mutationMgr MutationManager

//perf data
var mutationCount int64

//---Exported RPC methods which are available to remote clients

//This function returns a map of <Index, SequenceVector> based on the IndexList received in request
func (m *MutationManager) GetSequenceVectors(indexList api.IndexList, reply *api.IndexSequenceMap) error {

	// if indexer is in error state, let the error handing routines finish
	if indexerErrorState == true {
		wg.Wait()
		//reset the error state at each handshake
		indexerErrorState = false
		indexerErrorString = ""
	}

	//if indexList is nil, return the complete map
	if len(indexList) == 0 {
		*reply = m.sequencemap
		if options.debugLog {
			log.Printf("Mutation Manager returning complete SequenceMap %v", m.sequencemap)
		}
		return nil
	}

	//loop through the list of requested indexes and return the sequenceVector for those indexes
	var replyMap = make(api.IndexSequenceMap)
	for _, idx := range indexList {
		//if the requested index is not found, return an error
		v, ok := m.sequencemap[idx]
		if !ok {
			return errors.New("Requested Index Not Found")
		}

		//add to the reply map
		if options.debugLog {
			log.Printf("Mutation Manager returning sequence vector for index %v %v", idx, v)
		}
		replyMap[idx] = v
	}
	*reply = replyMap
	return nil

}

//This method takes as input an api.Mutation and copies into mutation queue for processing
func (m *MutationManager) ProcessSingleMutation(mutation *api.Mutation, reply *bool) error {
	if options.debugLog {
		log.Printf("Received Mutation Type %s Indexid %v, Docid %v, Vbucket %v, Seqno %v", mutation.Type, mutation.Indexid, mutation.Docid, mutation.Vbucket, mutation.Seqno)
	}

	//if there is any pending error, reply with that. This will force a handshake again.
	if indexerErrorState == true {
		*reply = false
	}

	//copy the mutation data and return
	m.chmutation <- mutation
	*reply = true
	return nil

}

//---End of exported RPC methods

//read incoming mutation and distribute it on worker queues based on vbucketid
func (m *MutationManager) manageMutationQueue() {

	for {
		select {
		case mut, ok := <-m.chmutation:
			if ok {
				m.chworkers[mut.Vbucket%MAX_MUTATION_WORKERS] <- mut
			}
		case <-chdrain:
			wg.Add(1)
			m.drainMutationChannel(m.chmutation)
		}
	}

}

//start a mutation worker which handles mutation on the specified workerId channel
func (m *MutationManager) startMutationWorker(workerId int) {

	for {
		select {
		case mutation := <-m.chworkers[workerId]:
			m.handleMutation(mutation)
		case <-chdrain:
			wg.Add(1)
			m.drainMutationChannel(m.chworkers[workerId])
		}
	}
}

func (m *MutationManager) handleMutation(mutation *api.Mutation) {

	if mutation.Type == api.INSERT {

		var key api.Key
		var value api.Value
		var err error

		if key, err = api.NewKey(mutation.SecondaryKey, mutation.Docid); err != nil {
			log.Printf("Error Generating Key From Mutation %v. Skipped.", err)
			return
		}

		if value, err = api.NewValue(mutation.SecondaryKey, mutation.Docid, mutation.Vbucket, mutation.Seqno); err != nil {
			log.Printf("Error Generating Value From Mutation %v. Skipped.", err)
			return
		}

		if engine, ok := m.enginemap[mutation.Indexid]; ok {
			if err := engine.InsertMutation(key, value); err != nil {
				log.Printf("Error from Engine during InsertMutation. Key %v. Index %v. Error %v", key, mutation.Docid, err)
			}
			//send notification for this seqno to be recorded in SeqVector
			seqnotify := seqNotification{engine: engine,
				indexid: mutation.Indexid,
				seqno:   mutation.Seqno,
				vbucket: mutation.Vbucket,
			}
			m.chseq <- seqnotify
		} else {
			err := fmt.Sprintf("Unknown Index %v or Engine not found", mutation.Indexid)
			m.initErrorState(err)
		}

	} else if mutation.Type == api.DELETE {

		if engine, ok := m.enginemap[mutation.Indexid]; ok {
			if err := engine.DeleteMutation(mutation.Docid); err != nil {
				log.Printf("Error from Engine during Delete Mutation. Key %v. Error %v", mutation.Docid, err)
				return
			}
			//send notification for this seqno to be recorded in SeqVector
			seqnotify := seqNotification{engine: engine,
				indexid: mutation.Indexid,
				seqno:   mutation.Seqno,
				vbucket: mutation.Vbucket,
			}
			m.chseq <- seqnotify
		} else {
			err := fmt.Sprintf("Unknown Index %v or Engine not found", mutation.Indexid)
			m.initErrorState(err)
		}
	}
}

func StartMutationManager(engineMap map[string]api.Finder) (chan ddlNotification, error) {

	var err error

	//init the mutation manager maps
	mutationMgr.sequencemap = make(api.IndexSequenceMap)
	//copy the inital map from the indexer
	mutationMgr.enginemap = engineMap
	mutationMgr.initSequenceMapFromPersistence()

	//create channel to receive notification for new sequence numbers
	//and start a goroutine to manage it
	mutationMgr.chseq = make(chan seqNotification, MAX_SEQUENCE_QUEUE)
	go mutationMgr.manageSeqNotification()

	//create a channel to receive notification from indexer
	//and start a goroutine to listen to it
	mutationMgr.chddl = make(chan ddlNotification)
	go mutationMgr.manageIndexerNotification()

	//init the channel for incoming mutations
	mutationMgr.chmutation = make(chan *api.Mutation, MAX_INCOMING_QUEUE)
	go mutationMgr.manageMutationQueue()

	//init the workers for processing mutations
	for w := 0; w < MAX_MUTATION_WORKERS; w++ {
		mutationMgr.chworkers[w] = make(chan *api.Mutation, MAX_WORKER_QUEUE)
		go mutationMgr.startMutationWorker(w)
	}

	//init error state
	indexerErrorState = false
	indexerErrorString = ""
	chdrain = make(chan bool)

	//start the rpc server
	if err = startRPCServer(); err != nil {
		return nil, err
	}

	return mutationMgr.chddl, nil
}

func startRPCServer() error {

	log.Println("Starting Mutation Manager")
	server := rpc.NewServer()
	server.Register(&mutationMgr)

	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

	l, err := net.Listen("tcp", ":8096")
	if err != nil {
		return err
	}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				log.Printf("Error in Accept %v. Shutting down")
				//FIXME Add a cleanup function
				return
			}
			go server.ServeCodec(jsonrpc.NewServerCodec(conn))
		}
	}()
	return nil

}

func (m *MutationManager) manageIndexerNotification() {

	ok := true
	var ddl ddlNotification
	for ok {
		ddl, ok = <-m.chddl
		if ok {
			switch ddl.ddltype {
			case api.CREATE:
				m.enginemap[ddl.indexinfo.Uuid] = ddl.engine
				//init sequence map of new index
				seqVec := make(api.SequenceVector, api.MAX_VBUCKETS)
				m.sequencemap[ddl.indexinfo.Uuid] = seqVec
			case api.DROP:
				delete(m.enginemap, ddl.indexinfo.Uuid)
				//FIXME : Delete index entry from sequence map
			default:
				log.Printf("Mutation Manager Received Unsupported Notification %v", ddl.ddltype)
			}
		}
	}
}

func (m *MutationManager) manageSeqNotification() {

	var seq seqNotification
	openSeqCount := 0
	ok := true

	for ok {
		select {
		case seq, ok = <-m.chseq:
			if ok {
				seqVector, exists := m.sequencemap[seq.indexid]
				if !exists {
					log.Printf("IndexId %v not found in Sequence Vector. INCONSISTENT INDEXER STATE!!!", seq.indexid)
					break
				}
				seqVector[seq.vbucket] = seq.seqno
				m.sequencemap[seq.indexid] = seqVector
				openSeqCount += 1
				//persist only after SEQ_MAP_PERSIST_INTERVAL
				if openSeqCount == SEQ_MAP_PERSIST_INTERVAL {
					m.persistSequenceMap()
					openSeqCount = 0
				}
			}
		case <-chdrain:
			wg.Add(1)
			m.drainSeqChannel(m.chseq)
		}
	}
}

func (m *MutationManager) initSequenceMapFromPersistence() {

	var sequenceVector api.SequenceVector
	for idx, engine := range m.enginemap {
		metaval, err := engine.GetMeta(META_DOC_ID)
		if err != nil {
			log.Printf("Error retreiving Meta from Engine %v", err)
		}
		err = json.Unmarshal([]byte(metaval), &sequenceVector)
		if err != nil {
			log.Printf("Error unmarshalling SequenceVector %v", err)
		}
		m.sequencemap[idx] = sequenceVector
	}
}

func (m *MutationManager) persistSequenceMap() {

	for idx, seqm := range m.sequencemap {
		jsonval, err := json.Marshal(seqm)
		if err != nil {
			log.Printf("Error Marshalling SequenceMap %v", err)
		} else {
			//FIXME - Handle Error here
			m.enginemap[idx].InsertMeta(META_DOC_ID, string(jsonval))
		}
	}
}

func (m *MutationManager) initErrorState(err string) {

	indexerErrorState = true
	indexerErrorString = err

	//send drain signal to mutation queue
	chdrain <- true

	//send drain signal to sequence queue
	chdrain <- true

	//send drain signal to worker queues
	for w := 0; w < MAX_MUTATION_WORKERS; w++ {
		chdrain <- true
	}

}

func (m *MutationManager) drainMutationChannel(ch chan *api.Mutation) {

loop:
	for {
		select {
		case <-ch:
			continue
		default:
			break loop
		}
	}
	wg.Done()

}

func (m *MutationManager) drainSeqChannel(ch chan seqNotification) {

loop:
	for {
		select {
		case <-ch:
			continue
		default:
			break loop
		}
	}
	wg.Done()

}
