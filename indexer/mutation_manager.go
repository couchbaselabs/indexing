package main

import (
	"encoding/json"
	"errors"
	"github.com/couchbaselabs/indexing/api"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
)

type MutationManager struct {
	enginemap   map[string]api.Finder
	sequencemap api.IndexSequenceMap
}

var META_DOC_ID = "."
var mutationMgr MutationManager

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

//channel for sequence notification
var chseq chan seqNotification

//This function returns a map of <Index, SequenceVector> based on the IndexList received in request
func (m *MutationManager) GetSequenceVectors(indexList api.IndexList, reply *api.IndexSequenceMap) error {

	//reset the error state at each handshake
	indexerErrorState = false
	indexerErrorString = ""

	//if indexList is nil, return the complete map
	if len(indexList) == 0 {
		*reply = m.sequencemap
		log.Printf("Mutation Manager returning complete SequenceMap")
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
		log.Printf("Mutation Manager returning sequence vector for index %v", idx)
		replyMap[idx] = v
	}
	*reply = replyMap
	return nil

}

func (m *MutationManager) ProcessSingleMutation(mutation *api.Mutation, reply *bool) error {
	log.Printf("Received Mutation Type %s Indexid %v, Docid %v, Vbucket %v, Seqno %v", mutation.Type, mutation.Indexid, mutation.Docid, mutation.Vbucket, mutation.Seqno)

	//if there is any pending error, reply with that. This will force a handshake again.
	if indexerErrorState == true {
		*reply = false
	}

	//copy the mutation data and return
	mutationCopy := *mutation
	go m.handleMutation(mutationCopy)
	*reply = true
	return nil

}

//process the mutation, store it. In case of any error, set indexerInErrorState to TRUE
func (m *MutationManager) handleMutation(mutation api.Mutation) {

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
			chseq <- seqnotify
		} else {
			log.Printf("Unknown Index %v or Engine not found", mutation.Indexid)
			indexerErrorState = true
			indexerErrorString = "Unknown Index or Engine not found"
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
			chseq <- seqnotify
		} else {
			log.Printf("Unknown Index %v or Engine not found", mutation.Indexid)
			indexerErrorState = true
			indexerErrorString = "Unknown Index or Engine not found"
		}
	}
}

func StartMutationManager(engineMap map[string]api.Finder) (chan ddlNotification, error) {

	var err error

	//init the mutation manager maps
	//mutationMgr.enginemap= make(map[string]api.Finder)
	mutationMgr.sequencemap = make(api.IndexSequenceMap)
	//copy the inital map from the indexer
	mutationMgr.enginemap = engineMap
	mutationMgr.initSequenceMapFromPersistence()

	//create channel to receive notification for new sequence numbers
	//and start a goroutine to manage it
	chseq = make(chan seqNotification, api.MAX_VBUCKETS)
	go mutationMgr.manageSeqNotification(chseq)

	//create a channel to receive notification from indexer
	//and start a goroutine to listen to it
	chnotify = make(chan ddlNotification)
	go acceptIndexerNotification(chnotify)

	//start the rpc server
	if err = startRPCServer(); err != nil {
		return nil, err
	}

	return chnotify, nil
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

func acceptIndexerNotification(chnotify chan ddlNotification) {

	ok := true
	var ddl ddlNotification
	for ok {
		ddl, ok = <-chnotify
		if ok {
			switch ddl.ddltype {
			case api.CREATE:
				mutationMgr.enginemap[ddl.indexinfo.Uuid] = ddl.engine
				//init sequence map of new index
				var seqVec api.SequenceVector
				mutationMgr.sequencemap[ddl.indexinfo.Uuid] = seqVec
			case api.DROP:
				delete(mutationMgr.enginemap, ddl.indexinfo.Uuid)
				//FIXME : Delete index entry from sequence map
			default:
				log.Printf("Mutation Manager Received Unsupported Notification %v", ddl.ddltype)
			}
		}
	}
}

func (m *MutationManager) manageSeqNotification(chseq chan seqNotification) {

	ok := true
	var seq seqNotification
	for ok {
		seq, ok = <-chseq
		if ok {
			seqVector := m.sequencemap[seq.indexid]
			seqVector[seq.vbucket] = seq.seqno
			m.sequencemap[seq.indexid] = seqVector
			jsonval, err := json.Marshal(m.sequencemap[seq.indexid])
			if err != nil {
				log.Printf("Error Marshalling SequenceMap %v", err)
			} else {
				m.enginemap[seq.indexid].InsertMeta(META_DOC_ID, string(jsonval))
			}
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
