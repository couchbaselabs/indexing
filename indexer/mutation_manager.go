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
	vbucket uint
}

var chseq chan seqNotification

//This function returns a map of <Index, SequenceVector> based on the IndexList received in request
func (m *MutationManager) GetSequenceVector(indexList api.IndexList, reply *api.IndexSequenceMap) error {

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

	//FIXME change this to channel based
	*reply = false

	if mutation.Type == "INSERT" {

		var key api.Key
		var value api.Value
		var err error

		if key, err = api.NewKey(mutation.SecondaryKey, mutation.Docid); err != nil {
			log.Printf("Error Generating Key From Mutation %v", err)
			*reply = false
			return err
		}

		if value, err = api.NewValue(mutation.SecondaryKey, mutation.Docid, mutation.Vbucket, mutation.Seqno); err != nil {
			log.Printf("Error Generating Value From Mutation %v", err)
			*reply = false
			return err
		}

		if engine, ok := m.enginemap[mutation.Indexid]; ok {
			if err := engine.InsertMutation(key, value); err != nil {
				log.Printf("Error from Engine during InsertMutation %v", err)
				*reply = false
				return err
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
			*reply = false
			return errors.New("Unknown Index or Engine not found")
		}

		*reply = true

	} else if mutation.Type == "DELETE" {

		if engine, ok := m.enginemap[mutation.Indexid]; ok {
			if err := engine.DeleteMutation(mutation.Docid); err != nil {
				log.Printf("Error from Engine during Delete Mutation %v", err)
				*reply = false
				return err
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
			*reply = false
			return errors.New("Unknown Index or Engine not found")
		}
		*reply = true

	}
	return nil
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
	chseq = make(chan seqNotification, 1024)
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
			case api.DROP:
				delete(mutationMgr.enginemap, ddl.indexinfo.Uuid)
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
